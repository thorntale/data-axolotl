from typing import NamedTuple
from typing import List
# from typing import override
from enum import Enum
import itertools
from statistics import stdev

import humanize

from .state_dao import Metric
from . import derived_metrics
from .trackers import MetricKey
from .trackers import AlertSeverity
from .trackers import AlertMethod
from .trackers import MetricTracker
from .trackers import NumericMetricTracker
from .trackers import TableSizeTracker
from .trackers import TableRowCountTracker
from .trackers import TableCreateTimeTracker
from .trackers import TableUpdateTimeTracker
from .trackers import TableStalenessTracker


class MetricSet:
    def __init__(self, runs, metrics):
        self.latest_run_id = max(r.run_id for r in runs)
        self.runs = runs
        self.run_times_by_id = {
            r.run_id: r.started_at
            for r in self.runs
        }
        self.metrics = metrics
        # self.metric_keys = {
        #     MetricKey(m.target_table, m.target_column, m.metric_name)
        #     for m in metrics
        # }
        # alertable metrics must have non-null values in the last 2 runs
        self.alertable_metric_keys = {
            MetricKey(m.target_table, m.target_column, m.metric_name)
            for m in metrics
            if m.run_id in sorted(r.run_id for r in runs)[-2:]
        }

    def get_tracked_tables(self) -> List[str]:
        return {
            k.target_table
            for k in self.alertable_metric_keys
        }

    def get_metric_trackers_for_table(self, table: str) -> List[MetricTracker]:
        return [
            TableSizeTracker(
                MetricKey(table, None, 'table_size'),
                self._get_metric_with_nulls(MetricKey(table, None, 'table_size')),
            ),
            TableRowCountTracker(
                MetricKey(table, None, 'row_count'),
                self._get_metric_with_nulls(MetricKey(table, None, 'row_count')),
            ),
            TableCreateTimeTracker(
                MetricKey(table, None, 'created_at'),
                self._get_metric_with_nulls(MetricKey(table, None, 'created_at')),
            ),
            TableUpdateTimeTracker(
                MetricKey(table, None, 'updated_at'),
                self._get_metric_with_nulls(MetricKey(table, None, 'updated_at')),
            ),
            TableStalenessTracker(
                MetricKey(table, None, 'staleness_hours'),
                derived_metrics.staleness_hours(
                    self._get_metric_with_nulls(MetricKey(table, None, 'updated_at')),
                    self.run_times_by_id,
                ),
            ),
        ]

    def _get_metric_with_nulls(self, key: MetricKey, type_constrained: bool = False) -> List[Metric]:
        """
        returns a dense list of metrics ordered by run_id
        if type_constrained is True, only include metrics where the column's
        `data_type_simple` matches the latest `data_type_simple`
        """
        matching_metrics = [
            m for m in self.metrics
            if m.target_table == key.target_table
            and m.target_column == key.target_column
            and m.metric_name == key.metric_name
        ]

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
        return sorted(matching_metrics + null_metrics, key=lambda m: r.run_id)
