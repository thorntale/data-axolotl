from typing import List
from typing import Dict
from datetime import datetime
from datetime import timedelta

from .state_dao import Metric



def staleness_hours(values: List[Metric], run_times_by_id: Dict[int, datetime]) -> List[Metric]:
    return [
        v.replace(
            metric_name='staleness_hours',
            metric_value=(
                None
                if v.metric_value is None
                else (run_times_by_id[v.run_id] - v.metric_value).total_seconds() / 3600.0
            ),
        )
        for v in values
    ]
