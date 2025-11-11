from typing import Callable, Dict, List, NamedTuple, Any
from enum import Enum

from .state_dao import Metric


class QueryStatus(Enum):
    """Status of a running query"""
    QUEUED = "queued"
    STARTED = "started"
    DONE = "done"
    ERROR = "error"

class MetricQuery(NamedTuple):
    # Query execution details
    query: str  # SQL query string to execute
    query_id: str  # Snowflake query ID (set after execution)
    status: QueryStatus  # Current status of the query

    # debug info
    fq_table_name: str
    column_name: str
    query_detail: str

    # query results into Metric objects
    # Takes run_id, fq_table_name, column_name, and dict of query results, returns list of Metrics
    result_extractor: Callable[[int, str, str, Dict[str, Any]], List[Metric]]
