from typing import List
from typing import Dict
from datetime import datetime
from datetime import timedelta

from .connectors.state_dao import Metric
from .connectors.snowflake_connection import get_simple_data_type



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

def boolean_rate(true_count_metrics: List[Metric], false_count_metrics: List[Metric]) -> List[Metric]:
    return [
        t._replace(
            metric_name='boolean_rate',
            metric_value=(
                # give null if both are null
                None if t.metric_value == f.metric_value == None else
                # give 50 if both are 0
                50.0 if (t.metric_value or 0) == (f.metric_value or 0) == 0 else
                # else give 100 * t / (t + f)
                100.0 * (t.metric_value or 0) / ((t.metric_value or 0) + (f.metric_value or 0))
            )
        )
        for t, f in zip(true_count_metrics, false_count_metrics)
    ]
