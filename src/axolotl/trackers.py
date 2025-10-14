from typing import NamedTuple
from typing import List
from typing import Optional
from typing import Any
from enum import Enum
from abc import ABC, abstractmethod
from datetime import datetime
import humanize

from .state_dao import Metric


""" Number of data points required to use delta trend estimation """
MIN_DELTA_ESTIMATION_COUNT = 5
""" Maximum number of data points to use for delta trend estimation """
MAX_DELTA_ESTIMATION_COUNT = 30


class MetricKey(NamedTuple):
    target_table: str
    target_column: Optional[str]
    metric_name: str

class AlertSeverity(Enum):
    # Major is for changes that are likely to be systemic
    Major = 'Major'
    # Minor is for changes that could be systemic
    Minor = 'Minor'
    Other = 'Other'
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


class MetricTracker(ABC):
    pretty_name: str = "<pretty_name>"
    description: str = "<description>"

    key: MetricKey
    values: List[Metric]

    def __init__(self, key: MetricKey, values: List[Metric]):
        self.key = key
        self.values = values

    @abstractmethod
    def get_change_severity(self) -> tuple[AlertSeverity, AlertMethod]:
        pass

    def value_formatter(self, value: Any) -> str:
        if value == True:
            return 'true'
        if value == False:
            return 'false'
        if value is None:
            return 'null'
        if isinstance(value, datetime):
            return value.strftime("%d-%m-%Y %H:%M:%S %Z")
        if isinstance(value, (int, float)):
            return f"{value:,}"
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

    def get_all_detlas(self) -> List[float]:
        """ Returns a list of all delta, in order, for numeric types """
        def diff(a, b):
            try:
                return b - a
            except TypeError:
                return None
        return [
            diff(a, b)
            for (a, b)
            in itertools.pairwise(v.metric_value for v in self.values)
        ]

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
        if not latest:
            return None
        latest_30 = [d for d in deltas[-MAX_DELTA_ESTIMATION_COUNT - 1 : -1] if d is not None]
        if len(latest_30) < MIN_DELTA_ESTIMATION_COUNT:
            return None

        return abs(latest - avg(latest_30)) / stdev(latest_30)

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
    def get_change_severity(self) -> tuple[AlertSeverity, AlertMethod]:
        if self.is_null_status_change():
            return (AlertSeverity.Major, AlertMethod.ToFromNull)
        z_score = self.estimate_delta_z_score()
        if z_score is not None:
            if z_score > 4:
                return (AlertSeverity.Major, AlertMethod.ZScore)
            if z_score > 3:
                return (AlertSeverity.Minor, AlertMethod.ZScore)
            if z_score > 0.0:
                return (AlertSeverity.Other, AlertMethod.ZScore)
            return (AlertSeverity.Unchanged, AlertMethod.ZScore)
        dpct = self.estimate_delta_pct()
        if dpct is not None:
            if abs(dpct) > 0.2:
                return (AlertSeverity.Major, AlertMethod.Pct)
            if abs(dpct) > 0.05:
                return (AlertSeverity.Minor, AlertMethod.Pct)
            if abs(dpct) > 0.0:
                return (AlertSeverity.Other, AlertMethod.Pct)
            return (AlertSeverity.Unchanged, AlertMethod.Pct)
        # probably unreachable
        return (AlertSeverity.Major, AlertMethod.Changed)


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


class TableCreateTimeTracker(NumericMetricTracker):
    pretty_name = "Creation Time"
    description = "Time the table was last created"
    def get_change_severity(self) -> tuple[AlertSeverity, AlertMethod]:
        if self.is_null_status_change():
            return (AlertSeverity.Major, AlertMethod.ToFromNull)
        if self.get_current_value() != self.get_prev_value():
            return (AlertSeverity.Minor, AlertMethod.Changed)
        return (AlertSeverity.Unchanged, AlertMethod.Changed)

class TableUpdateTimeTracker(MetricTracker):
    pretty_name = "Update Time"
    description = "Time the table was last updated"
    def get_change_severity(self) -> tuple[AlertSeverity, AlertMethod]:
        if self.get_current_value() != self.get_prev_value():
            return (AlertSeverity.Other, AlertMethod.Changed)
        return (AlertSeverity.Unchanged, AlertMethod.Changed)

class TableStalenessTracker(NumericMetricTracker):
    pretty_name = "Staleness"
    description = "How long ago the table was last updated"
    def value_formatter(self, value: int) -> str:
        if value is None:
            return super().value_formatter(value)
        return super().value_formatter(value) + ' hours'
