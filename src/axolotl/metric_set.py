from typing import Dict, Set, Optional, NamedTuple, Tuple, List, Generator
# from typing import override
from enum import Enum
import itertools
from statistics import stdev
from collections import defaultdict

import humanize

from .trackers import MetricTracker, MetricAlert, MetricKey, AlertSeverity, AlertMethod
from .connectors.state_dao import Metric, Run
from .connectors.identifiers import FqTable
from . import derived_metrics
from . import trackers as ts
from .connectors.snowflake_connection import SimpleDataType


class MetricSet:
    def __init__(self, runs: List[Run], metrics: List[Metric]):
        self.latest_run_id = max(r.run_id for r in runs)
        self.runs = runs
        self.run_times_by_id = {
            r.run_id: r.started_at
            for r in self.runs
        }
        self.metrics = metrics
        # do the group by once so we don't have to filter the whole list each time
        self.grouped_metrics: Dict[Tuple[FqTable, str | None, str], List[Metric]] = defaultdict(list)
        for m in metrics:
            self.grouped_metrics[(m.target_table, m.target_column, m.metric_name)].append(m)

        # alertable metrics must have non-null values in the last 2 runs
        self.alertable_metric_keys = {
            MetricKey(m.target_table, m.target_column, m.metric_name)
            for m in metrics
            if m.run_id in sorted(r.run_id for r in runs)[-2:]
        }

    def get_create_destory_alerts(self) -> List[MetricAlert]:
        current_keys = {
            (m.target_table, m.target_column)
            for m in self.metrics
            if m.run_id == self.latest_run_id
        }

        prev_run_id = max((r.run_id for r in self.runs if r.run_id != self.latest_run_id), default=None)
        prev_keys = {
            (m.target_table, m.target_column)
            for m in self.metrics
            if m.run_id == prev_run_id
        }

        alerts = []

        for table, column in current_keys - prev_keys:
            alerts.append(MetricAlert(
                key=MetricKey(table, column, 'column_added' if column else 'table_added'),
                pretty_name='Column Added' if column else 'Table Added',
                alert_severity=AlertSeverity.Major,
                alert_method=AlertMethod.Changed,
                current_value_formatted='tracked',
                prev_value_formatted='untracked',
                change_formatted='+',
            ))

        for table, column in prev_keys - current_keys:
            alerts.append(MetricAlert(
                key=MetricKey(table, column, 'column_removed' if column else 'table_removed'),
                pretty_name='Column Removed' if column else 'Table Removed',
                alert_severity=AlertSeverity.Major,
                alert_method=AlertMethod.Changed,
                current_value_formatted='untracked',
                prev_value_formatted='tracked',
                change_formatted='-',
            ))

        new_or_removed_tables = {a.key.target_table for a in alerts if a.key.target_column is None}
        # filter out column level alerts for created or destroyed tables
        alerts = [
            a for a in alerts
            if
                a.key.target_column is None
                or a.key.target_table not in new_or_removed_tables
        ]

        return alerts

    def get_tracked_tables(self) -> Set[FqTable]:
        return {
            k.target_table
            for k in self.alertable_metric_keys
        }

    def get_tracked_columns(self, table: Optional[FqTable] = None) -> Set[Tuple[FqTable, str]]:
        return {
            (k.target_table, k.target_column)
            for k in self.alertable_metric_keys
            if
                k.target_column is not None
                and (table is None or table == k.target_table)
        }

    def get_metric_trackers_for_table(self, table: FqTable) -> Generator[MetricTracker]:
        # We might list a table because there are column metrics within it, but
        # not track any table metrics. In that case, skip its metrics.
        if not any(
            t == table and c is None
            for t, c, m in self.alertable_metric_keys
        ):
            return

        yield ts.TableSizeTracker(
            self._get_metric_with_nulls(MetricKey(table, None, 'bytes')),
        )
        yield ts.TableRowCountTracker(
            self._get_metric_with_nulls(MetricKey(table, None, 'row_count')),
        )
        yield ts.TableCreateTimeTracker(
            self._get_metric_with_nulls(MetricKey(table, None, 'created_at')),
        )
        yield ts.TableAlterTimeTracker(
            self._get_metric_with_nulls(MetricKey(table, None, 'altered_at')),
        )
        yield ts.TableUpdateTimeTracker(
            self._get_metric_with_nulls(MetricKey(table, None, 'updated_at')),
        )
        yield ts.TableStalenessTracker(
            derived_metrics.staleness_hours(
                self._get_metric_with_nulls(MetricKey(table, None, 'updated_at')),
                self.run_times_by_id,
            ),
        )

    def get_metric_trackers_for_column(self, table: FqTable, column: str) -> Generator[MetricTracker]:
        col_data_types = self._get_metric_with_nulls(MetricKey(table, column, 'data_type'))
        col_data_type_simples = derived_metrics.data_type_simple(col_data_types)

        data_type_simple = col_data_type_simples[-1].metric_value

        yield ts.ColumnTypeTracker(
            col_data_types,
        )
        yield ts.ColumnTypeSimpleTracker(
            col_data_type_simples,
        )

        if data_type_simple != SimpleDataType.BOOLEAN:
            yield ts.DistinctCount(
                self._get_metric_with_nulls(MetricKey(table, column, 'distinct_count'))
            )
            yield ts.DistinctRate(
                self._get_metric_with_nulls(MetricKey(table, column, 'distinct_rate'))
            )
        yield ts.NullCount(
            self._get_metric_with_nulls(MetricKey(table, column, 'null_count'))
        )
        yield ts.NullRate(
            self._get_metric_with_nulls(MetricKey(table, column, 'null_pct'))
        )

        if data_type_simple == SimpleDataType.BOOLEAN:
            yield ts.TrueCount(
                self._get_metric_with_nulls(MetricKey(table, column, 'true_count'))
            )
            yield ts.FalseCount(
                self._get_metric_with_nulls(MetricKey(table, column, 'false_count'))
            )
            yield ts.BooleanRate(
                derived_metrics.boolean_rate(
                    self._get_metric_with_nulls(MetricKey(table, column, 'true_count')),
                    self._get_metric_with_nulls(MetricKey(table, column, 'false_count')),
                )
            )

        if data_type_simple == SimpleDataType.NUMERIC:
            yield ts.Min(
                self._get_metric_with_nulls(MetricKey(table, column, 'numeric_min'))
            )
            yield ts.Max(
                self._get_metric_with_nulls(MetricKey(table, column, 'numeric_max'))
            )
            yield ts.Mean(
                self._get_metric_with_nulls(MetricKey(table, column, 'numeric_mean'))
            )
            yield ts.Stddev(
                self._get_metric_with_nulls(MetricKey(table, column, 'numeric_stddev'))
            )
            yield ts.NumericPercentiles(
                self._get_metric_with_nulls(MetricKey(table, column, 'numeric_percentiles'))
            )
            yield ts.NumericHistogram(
                self._get_metric_with_nulls(MetricKey(table, column, 'numeric_histogram'))
            )

        if data_type_simple == SimpleDataType.STRING:
            yield ts.AvgStringLength(
                self._get_metric_with_nulls(MetricKey(table, column, 'string_avg_length'))
            )

        if data_type_simple == SimpleDataType.DATETIME:
            yield ts.MinTS(
                self._get_metric_with_nulls(MetricKey(table, column, 'datetime_max'))
            )
            yield ts.MaxTS(
                self._get_metric_with_nulls(MetricKey(table, column, 'datetime_min'))
            )
            yield ts.DatetimeHistogram(
                self._get_metric_with_nulls(MetricKey(table, column, 'datetime_histogram'))
            )

        if data_type_simple == SimpleDataType.STRUCTURED:
            pass # TODO
        if data_type_simple == SimpleDataType.UNSTRUCTURED:
            pass # TODO
        if data_type_simple == SimpleDataType.VECTOR:
            pass # TODO
        if data_type_simple == SimpleDataType.OTHER:
            pass # TODO

    def get_all_alerts(self) -> List[MetricAlert]:
        create_destroy_alerts = self.get_create_destory_alerts()
        create_destroy_keys = {
            (a.key.target_table, a.key.target_column)
            for a in create_destroy_alerts
        }

        trackers: List[MetricTracker] = []

        for t in self.get_tracked_tables():
            if (t, None) not in create_destroy_keys:
                trackers += self.get_metric_trackers_for_table(t)

        for t, c in self.get_tracked_columns():
            if (t, c) not in create_destroy_keys and (t, None) not in create_destroy_keys:
                trackers += self.get_metric_trackers_for_column(t, c)

        return create_destroy_alerts + [
            a for a in
            (t.get_alert() for t in trackers)
            if a is not None
        ]

    def _get_metric_with_nulls(self, key: MetricKey, type_constrained: bool = False) -> List[Metric]:
        """
        returns a dense list of metrics ordered by run_id
        if type_constrained is True, only include metrics where the column's
        `data_type_simple` matches the latest `data_type_simple`
        """
        matching_metrics = self.grouped_metrics[(
            key.target_table,
            key.target_column,
            key.metric_name,
        )]

        if type_constrained:
            if key.target_column is None:
                raise ValueError('type_constrained can only be used on column metrics')
            simple_type_history = self._get_metric_with_nulls(key._replace(metric_name='data_type_simple'))
            if simple_type_history:
                latest_simple_type = simple_type_history[-1].metric_value
                same_type_run_ids = {
                    m.run_id
                    for m in simple_type_history
                    if m.metric_value == latest_simple_type
                }
                matching_metrics = [
                    m for m in matching_metrics
                    if m.run_id in same_type_run_ids
                ]

        missing_run_ids = [
            r.run_id for r in self.runs
            if r.run_id not in {m.run_id for m in matching_metrics}
        ]
        null_metrics = [
            Metric(
                run_id=run_id,
                target_table=key.target_table,
                target_column=key.target_column,
                metric_name=key.metric_name,
                metric_value=None,
                measured_at=self.run_times_by_id[run_id],
            )
            for run_id in missing_run_ids
        ]
        return sorted(matching_metrics + null_metrics, key=lambda m: m.run_id)
