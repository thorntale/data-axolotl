from typing import Callable, Dict, List, NamedTuple, Any
from enum import Enum

from .connectors.state_dao import Metric
from .connectors.identifiers import FqTable


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
    timeout_at: float  # monotonic time at which to timeout this query
    status: QueryStatus  # Current status of the query

    # debug info
    fq_table_name: FqTable
    column_name: str
    metrics: List[str]  # Which metrics this will produce when run
    query_detail: str

    # query results into Metric objects
    # Takes run_id, fq_table_name, column_name, and dict of query results, returns list of Metrics
    result_extractor: Callable[[int, FqTable, str, Dict[str, Any]], List[Metric]]

    def name(self):
        return f"{self.fq_table_name}.{self.column_name} ({self.query_detail})"
