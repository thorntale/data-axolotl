from typing import List
from typing import Dict
from datetime import datetime
from datetime import timedelta

from .state_dao import Metric
from .state_connection import get_simple_data_type



def staleness_hours(updated_at_metrics: List[Metric], run_times_by_id: Dict[int, datetime]) -> List[Metric]:
    return [
        v._replace(
            metric_name='staleness_hours',
            metric_value=(
                None
                if v.metric_value is None
                else (run_times_by_id[v.run_id] - v.metric_value).total_seconds() / 3600.0
            ),
        )
        for v in updated_at_metrics
    ]

def data_type_simple(data_type_metrics: List[Metric]) -> List[Metric]:
    return [
        v._replace(
            metric_name='data_type_simple',
            metric_value=(
                None
                if v.metric_value is None
                else get_simple_data_type(v.metric_value)
            ),
        )
        for v in data_type_metrics
    ]
