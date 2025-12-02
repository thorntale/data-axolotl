from typing import NamedTuple
from typing import List
from typing import Optional
from typing import Any
from enum import Enum
from abc import ABC, abstractmethod
from datetime import datetime
from datetime import timedelta
import itertools
import math
from statistics import stdev

import humanize

from .database_types import SimpleDataType
from .display_utils import maybe_float
from .connectors.state_dao import Metric
from .connectors.identifiers import FqTable
from .line_chart import arr_to_dots


""" Number of data points required to use delta trend estimation """
MIN_DELTA_ESTIMATION_COUNT = 5
""" Maximum number of data points to use for delta trend estimation """
MAX_DELTA_ESTIMATION_COUNT = 30


class MetricKey(NamedTuple):
    target_table: FqTable
    target_column: Optional[str]
    metric_name: str

class AlertSeverity(Enum):
    # Major is for changes that are likely to be systemic
    Major = 'Major'
    # Minor is for changes that could be systemic
    Minor = 'Minor'
    Changed = 'Changed'
    Unchanged = 'Unchanged'

class AlertMethod(Enum):
    """ The method used for deciding on alert severity """
    # z score was outside bounds
    ZScore = 'ZScore'
    # delta / prev was outside bounds
    Pct = 'Pct'
    # prev != current
    Changed = 'Changed'
    # (prev is None) != (current is None)
    ToFromNull = 'ToFromNull'

class ChartMode(Enum):
    Standard = 'Standard'
    HasChanged = 'HasChanged'
    NumericPercentiles = 'NumericPercentiles'
    Histogram = 'Histogram'

# The display value describing the alert change, ex `+10%` or `5.6std`
type AlertingDelta = str

class MetricAlert(NamedTuple):
    key: MetricKey
    pretty_name: str
    alert_severity: AlertSeverity
    alert_method: AlertMethod
    current_value_formatted: str
    prev_value_formatted: str
    change_formatted: str

class MetricTracker(ABC):
    pretty_name: str = "<pretty_name>"
    description: str = "<description>"
    chart_mode = ChartMode.Standard

    key: MetricKey
    values: List[Metric]

    def __init__(self, values: List[Metric]):
        if not values:
            raise ValueError('No values provided to MetricTracker')
        self.key = MetricKey(
            values[0].target_table,
            values[0].target_column,
            values[0].metric_name,
        )
        self.values = values

    @abstractmethod
    def get_change_severity(self) -> tuple[AlertSeverity, AlertMethod, AlertingDelta]:
        pass

    def get_alert(self) -> Optional[MetricAlert]:
        severity, method, change_formatted = self.get_change_severity()
        return MetricAlert(
            key=self.key,
            pretty_name=self.pretty_name,
            alert_severity=severity,
            alert_method=method,
            current_value_formatted=self.value_formatter(self.get_current_value()),
            prev_value_formatted=self.value_formatter(self.get_prev_value()),
            change_formatted=change_formatted,
        )

    def value_formatter(self, value: Any) -> str:
        if value is True:
            return 'true'
        if value is False:
            return 'false'
        if value is None:
            return 'null'
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S %Z")
        if isinstance(value, (int, float)) and value.is_integer():
            return f"{value:,}"
        if isinstance(value, float):
            return f"{value:.6g}"
        return str(value)

    def get_current_value(self) -> Any:
        if len(self.values) < 1:
            return None
        else:
            return self.values[-1].metric_value

    def get_prev_value(self) -> Any:
        if len(self.values) < 2:
            return None
        else:
            return self.values[-2].metric_value

    def get_latest_delta(self) -> Any:
        deltas = self.get_all_detlas()
        if not deltas:
            return None
        return deltas[-1]

    def get_all_detlas(self) -> List[float|None]:
        """ Returns a list of all delta, in order, for numeric types """
        return [
            self.get_single_delta(a, b)
            for (a, b)
            in itertools.pairwise(v.metric_value for v in self.values)
        ]

    def get_single_delta(self, a, b) -> float | None:
        try:
            diff = b - a
            if isinstance(diff, timedelta):
                return diff.total_seconds()
            else:
                return diff
        except TypeError:
            return None

    def estimate_delta_z_score(self) -> Optional[float]:
        """ Computes the mean and stddev of latest 30 deltas, then compares
        latest delta to those, returning abs(d - mean) / stddev.
        If there are too few data points for a reliable estimation, return None
        If any data types are non-numeric, returns None
        If the latest delta is None, returns None
        """
        deltas = self.get_all_detlas()
        if not deltas:
            return None
        latest = deltas[-1]
        if latest is None:
            return None
        latest_30 = [
            d for
            d in deltas[-MAX_DELTA_ESTIMATION_COUNT - 1 : -1]
            if d is not None
        ]
        if len(latest_30) < MIN_DELTA_ESTIMATION_COUNT:
            return None

        def avg(v):
            return sum(v) / len(v)

        sd = stdev(latest_30)
        # Don't use delta-z when stddev is 0
        if abs(sd) < 1e-10:
            return None
        diff = latest - avg(latest_30)

        if sd == 0 and diff == 0:
            return 0
        if sd == 0:
            return float('inf')
        return abs(diff / sd)

    def get_delta_z_alert(self) -> Optional[tuple[AlertSeverity, AlertMethod, AlertingDelta]]:
        z_score = self.estimate_delta_z_score()
        if z_score is None:
            return None
        # large deltas are major alerts, this is definitely a systemic change
        if z_score > 4:
            return (AlertSeverity.Major, AlertMethod.ZScore, f"Δ={z_score:.2f}z")
        # anything from 3-4z is a minor alert
        if z_score > 3:
            return (AlertSeverity.Minor, AlertMethod.ZScore, f"Δ={z_score:.2f}z")
        # if z is 0 AND the metric didn't change, give unchanged
        if z_score == 0 and self.get_prev_value() == self.get_current_value():
            return (AlertSeverity.Unchanged, AlertMethod.ZScore, f"==,Δ=0z")
        # if z is 0 but the metric did change, put the alert in other
        # ex: a metric increases by +1 every run
        return (AlertSeverity.Changed, AlertMethod.ZScore, f"Δ={z_score:.2f}z")

    def estimate_delta_pct(self) -> Optional[float]:
        """
        Computes (current - prev) / prev
        Handles some special cases:
        - if prev or current is None, returns None
        - if prev == current == 0, returns 0
        - if prev == 0, returns ±Infinity
        - if prev or current is non-numeric, returns None
        """
        cur = self.get_current_value()
        prev = self.get_prev_value()
        if cur is None or prev is None:
            return None
        if prev == 0 and cur == 0:
            return 0.0
        if prev == 0:
            return cur * float("inf")
        try:
            return (cur - prev) / prev
        except TypeError:
            return None

    def is_null_status_change(self) -> bool:
        """ Return true if the metric changed to or from None """
        prev_is_null = self.get_prev_value() is None
        cur_is_null = self.get_current_value() is None
        return prev_is_null != cur_is_null


class NumericMetricTracker(MetricTracker):
    """ General purpose metric tracker for numeric metrics.
    Comes with a get_change_severity implementation """
    def get_change_severity(self) -> tuple[AlertSeverity, AlertMethod, AlertingDelta]:
        if self.get_current_value() is None and self.get_prev_value() is None:
            return (AlertSeverity.Unchanged, AlertMethod.ToFromNull, '==')
        if self.is_null_status_change():
            return (AlertSeverity.Major, AlertMethod.ToFromNull, 'null')
        if z_alert := self.get_delta_z_alert():
            return z_alert
        _dpct = self.estimate_delta_pct()
        if _dpct is not None:
            dpct = 100 * _dpct
            if abs(dpct) > 20:
                return (AlertSeverity.Major, AlertMethod.Pct, f"{dpct:+.0f}%")
            if abs(dpct) > 5:
                return (AlertSeverity.Minor, AlertMethod.Pct, f"{dpct:+.0f}%")
            if abs(dpct) > 0:
                return (AlertSeverity.Changed, AlertMethod.Pct, f"{dpct:+.0f}%")
            return (AlertSeverity.Unchanged, AlertMethod.Pct, f"==")
        # probably unreachable
        return (AlertSeverity.Major, AlertMethod.Changed, '!=')

class DatetimeMetricTracker(MetricTracker):
    def get_change_severity(self) -> tuple[AlertSeverity, AlertMethod, AlertingDelta]:
        if self.get_current_value() is None and self.get_prev_value() is None:
            return (AlertSeverity.Unchanged, AlertMethod.ToFromNull, '==')
        if self.is_null_status_change():
            return (AlertSeverity.Major, AlertMethod.ToFromNull, 'null')
        if z_alert := self.get_delta_z_alert():
            return z_alert
        # don't even bother trying to do % changes on datetimes
        if self.get_current_value() != self.get_prev_value():
            return (AlertSeverity.Minor, AlertMethod.Changed, '!=')
        return (AlertSeverity.Unchanged, AlertMethod.Changed, '==')

class PercentMetricTracker(NumericMetricTracker):
    """ Numeric metric tracker for relative values.
    Computes delta changes non-relatively. """
    def estimate_delta_pct(self) -> Optional[float]:
        """
        Computes current - prev
        - if prev or current is None, returns None
        """
        cur = self.get_current_value()
        prev = self.get_prev_value()
        if cur is None or prev is None:
            return None
        return cur - prev

    def value_formatter(self, value: Any) -> str:
        if isinstance(value, (int, float)):
            return f"{value:,.2f}%"
        return super().value_formatter(value)


class EqualityMetricTracker(MetricTracker):
    """ Alerts if the metric changes at all """
    def get_change_severity(self) -> tuple[AlertSeverity, AlertMethod, AlertingDelta]:
        if self.get_current_value() is None and self.get_prev_value() is None:
            return (AlertSeverity.Unchanged, AlertMethod.ToFromNull, '==')
        if self.is_null_status_change():
            return (AlertSeverity.Major, AlertMethod.ToFromNull, 'null')
        if self.get_current_value() != self.get_prev_value():
            return (AlertSeverity.Minor, AlertMethod.Changed, '!=')
        return (AlertSeverity.Unchanged, AlertMethod.Changed, '==')


class TableSizeTracker(NumericMetricTracker):
    pretty_name = "Size"
    description = "Size of table"

    # @override
    def value_formatter(self, value: Optional[int]) -> str:
        if value is None:
            return super().value_formatter(value)
        return humanize.naturalsize(value)


class TableRowCountTracker(NumericMetricTracker):
    pretty_name = "Row Count"
    description = "Number of rows in the table"
    pass


class TableCreateTimeTracker(EqualityMetricTracker):
    pretty_name = "Creation Time"
    description = "Time the table was last created."
    chart_mode = ChartMode.HasChanged

class TableAlterTimeTracker(EqualityMetricTracker):
    pretty_name = "Last Altered Time"
    description = "Time the table structure was last altered."
    chart_mode = ChartMode.HasChanged

class TableUpdateTimeTracker(MetricTracker):
    pretty_name = "Update Time"
    description = "Time the table was last updated"
    chart_mode = ChartMode.HasChanged
    def get_change_severity(self) -> tuple[AlertSeverity, AlertMethod, AlertingDelta]:
        if self.get_current_value() != self.get_prev_value():
            return (AlertSeverity.Changed, AlertMethod.Changed, '!=')
        return (AlertSeverity.Unchanged, AlertMethod.Changed, '==')

class TableStalenessTracker(NumericMetricTracker):
    pretty_name = "Staleness"
    description = "How long ago the table was last updated"

    def get_delta_z_alert(self) -> None:
        """ alerting on delta-z doesn't make a ton of sense for
        stalness, since it's either always increasing or stays
        below the same threshold. """
        return None

    def value_formatter(self, value: int) -> str:
        if value is None:
            return super().value_formatter(value)
        if value < 1:
            return f"{value * 60:.2f} minutes"
        if value < 1/60:
            return f"{value * 3600:.0f} seconds"
        if value > 48:
            return f"{value /24:.2f} days"
        return f'{value:.2f} hours'

class ColumnTypeTracker(EqualityMetricTracker):
    pretty_name = 'Column Type'
    description = 'The type of the column'

class ColumnTypeSimpleTracker(EqualityMetricTracker):
    pretty_name = 'Simple Type'
    description = 'The type category of the column'

    def value_formatter(self, value: SimpleDataType | None) -> str:
        if value is None:
            return 'null'
        return str(value.value)

    # Doesn't alert
    def get_alert(self) -> None:
        return None

class DistinctCount(NumericMetricTracker):
    pretty_name = 'Distinct Count'
    description = 'Count of distinct non-null values. May be approximate for large tables.'

class DistinctRate(PercentMetricTracker):
    pretty_name = 'Distinct Rate'
    description = 'Distinct count / non-null row count.'

class NullCount(NumericMetricTracker):
    pretty_name = 'Null Count'
    description = 'Count of non-null values.'

class NullRate(PercentMetricTracker):
    pretty_name = 'Null Rate'
    description = 'What percentage of rows are null.'

class Min(NumericMetricTracker):
    pretty_name = 'Minimum'
    description = 'Minimum non-null value'

class Max(NumericMetricTracker):
    pretty_name = 'Maximum'
    description = 'Maximum non-null value'

class MinTS(DatetimeMetricTracker):
    pretty_name = 'Minimum'
    description = 'Minimum non-null value'

class MaxTS(DatetimeMetricTracker):
    pretty_name = 'Maximum'
    description = 'Maximum non-null value'

class Mean(NumericMetricTracker):
    pretty_name = 'Mean'
    description = 'Average non-null value'

class AvgStringLength(NumericMetricTracker):
    pretty_name = 'Average Length'
    description = 'Average length of string'

class Stddev(NumericMetricTracker):
    pretty_name = 'Standard Deviation'
    description = 'Standard deviation amoung non-null values'

class NumericPercentiles(NumericMetricTracker):
    pretty_name = 'Percentiles'
    description = 'Approximate percentile measurements'
    chart_mode = ChartMode.NumericPercentiles

    def value_formatter(self, value: Any) -> str:
        if value is None:
            return super().value_formatter(value)
        L = 10
        chars = ' -=░▓░=- '
        part_widths = [
            value['1p'] - value['0p'],
            value['5p'] - value['1p'],
            value['10p'] - value['5p'],
            value['30p'] - value['10p'],
            value['70p'] - value['30p'],
            value['90p'] - value['70p'],
            value['95p'] - value['90p'],
            value['99p'] - value['95p'],
            value['100p'] - value['99p'],
        ]
        if sum(part_widths) == 0:
            return '[' + '░' * L + ']'
        return (
            '['
            + ''.join(
                char * round(w / sum(part_widths) * L * 9)
                for char, w in zip(chars, part_widths)
            )[4::9]  # we render at 9x resolution then downscale to mitigate rounding errors
            + ']'
        )


    def get_single_delta(self, a, b) -> float | None:
        """ Returns average change in percentile across all measured percentiles """
        if a is None or b is None:
            return None

        diffs = [
            b[k] - a[k]
            for k in set(a.keys()) & set(b.keys())
        ]
        if not diffs:
            return None
        return sum(diffs) / len(diffs)

    def estimate_delta_pct(self) -> Optional[float]:
        """
        Computes (current - prev) / prev
        Handles some special cases:
        - if prev or current is None, returns None
        - if prev == current == 0, returns 0
        - if prev == 0, returns ±Infinity
        - if prev or current is non-numeric, returns None
        """
        cur = self.get_current_value()
        prev = self.get_prev_value()
        delta = self.get_single_delta(prev, cur)
        if delta is None:
            return None
        if delta == 0:
            return 0
        span = prev['100p'] - prev['0p']
        if not span:
            return None
        return delta / span

class NumericHistogram(NumericMetricTracker):
    pretty_name = 'Histogram'
    description = 'Histogram of data distribution'
    chart_mode = ChartMode.Histogram

    def value_formatter(self, value: Any) -> str:
        # return str(len(value or []))
        if value is None:
            return super().value_formatter(value)
        max_val = max(value.values())
        sorted_keys = sorted(value.keys(), key=maybe_float)
        ys = [
            math.floor(value[k] / (max_val or 1) * 5.0)
            for k in sorted_keys
        ]
        return '⎹' + arr_to_dots(ys) + '⎸'

    def get_single_delta(self, a, b) -> float | None:
        """ Returns average absolute change in bucket quanitiy
        across all measured buckets """
        if a is None or b is None:
            return None

        def ordered_values(v):
            return [
                v[k] for k in sorted(v.keys(), key=maybe_float)
            ]

        return sum(
            abs(va - vb)
            for va, vb in itertools.zip_longest(
                ordered_values(a),
                ordered_values(b),
                fillvalue=0,
            )
        )

    def estimate_delta_pct(self) -> Optional[float]:
        """
        Computes (current - prev) / prev
        Handles some special cases:
        - if prev or current is None, returns None
        - if prev == current == 0, returns 0
        - if prev == 0, returns ±Infinity
        - if prev or current is non-numeric, returns None
        """
        cur = self.get_current_value()
        prev = self.get_prev_value()
        delta = self.get_single_delta(prev, cur)
        if delta is None:
            return None
        if delta == 0:
            return 0
        total = sum(prev.values())
        if not total:
            return None
        return delta / total

class DatetimeHistogram(NumericHistogram):
    """ Like numeric hist, but align buckets by key """
    def get_single_delta(self, a, b) -> float | None:
        """ Returns average absolute change in bucket quanitiy
        across all measured buckets """
        if a is None or b is None:
            return None

        return sum(
            abs(a.get(k, 0) - b.get(k, 0))
            for k in set(a.keys()) | set(b.keys())
        )


class TrueCount(NumericMetricTracker):
    pretty_name = 'True Count'
    description = 'Number of rows that are true'

class FalseCount(NumericMetricTracker):
    pretty_name = 'False Count'
    description = 'Number of rows that are false'

class BooleanRate(PercentMetricTracker):
    pretty_name = 'True Rate'
    description = 'True count / (true count + false count). Ignores nulls.'
