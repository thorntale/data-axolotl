import json
from typing import (
    NamedTuple,
    List,
    Tuple,
    TypedDict,
    NotRequired,
    Dict,
    Any,
    Set,
    Callable,
    Optional,
)
from .connectors.state_dao import Metric
from .connectors.identifiers import FqTable
from .database_types import SimpleDataType, ColumnInfo
from .metric_query import QueryStatus, MetricQuery


SNOWFLAKE_NUMERIC_TYPES = [
    "NUMBER",
    "DECIMAL",
    "NUMERIC",
    "INT",
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "TINYINT",
    "BYTEINT",
    "FLOAT",
    "FLOAT4",
    "FLOAT8",
    "DOUBLE",
    "DOUBLE PRECISION",
    "REAL",
]

SNOWFLAKE_TEXT_TYPES = [
    "VARCHAR",
    "CHAR",
    "CHARACTER",
    "STRING",
    "TEXT",
    "BINARY",
    "VARBINARY",
]

SNOWFLAKE_DATETIME_TYPES = [
    "DATE",
    "DATETIME",
    "TIME",
    "TIMESTAMP",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_NTZ",
    "TIMESTAMP_TZ",
]

SNOWFLAKE_STRUCTURED_TYPES = [
    "VARIANT",
    "OBJECT",
    "ARRAY",
    "MAP",
]

SNOWFLAKE_UNSTRUCTURED_TYPES = [
    "FILE",
]

SNOWFLAKE_VECTOR_TYPES = [
    "VECTOR",
]


def get_simple_data_type(data_type: str) -> SimpleDataType:
    data_type = data_type.upper()
    if data_type == "BOOLEAN":
        return SimpleDataType.BOOLEAN
    if data_type in SNOWFLAKE_NUMERIC_TYPES:
        return SimpleDataType.NUMERIC
    if data_type in SNOWFLAKE_TEXT_TYPES:
        return SimpleDataType.STRING
    if data_type in SNOWFLAKE_DATETIME_TYPES:
        return SimpleDataType.DATETIME
    if data_type in SNOWFLAKE_STRUCTURED_TYPES:
        return SimpleDataType.STRUCTURED
    if data_type in SNOWFLAKE_UNSTRUCTURED_TYPES:
        return SimpleDataType.UNSTRUCTURED
    if data_type in SNOWFLAKE_VECTOR_TYPES:
        return SimpleDataType.VECTOR
    else:
        return SimpleDataType.OTHER


def extract_percentiles(
    run_id: int, fq_table: FqTable, col: str, results: Dict[str, Any]
) -> List[Metric]:
    return [
        Metric(
            run_id=run_id,
            target_table=fq_table,
            target_column=col,
            metric_name="numeric_percentiles",
            metric_value=json.loads(results["NUMERIC_PERCENTILES"]),
            measured_at=results["_measured_at"],
        )
    ]


def extract_simple_metrics(
    run_id: int, fq_table_name: FqTable, column_name: str, results: Dict[str, Any]
) -> List[Metric]:
    """
    Default extractor function that converts query results into Metric objects.

    Args:
        run_id: The run ID for these metrics
        fq_table_name: Fully qualified table name
        column_name: Column name
        results: Dictionary of query results (from DictCursor.fetchone())

    Returns:
        List of Metric objects
    """
    metrics = []
    measured_at = results["_measured_at"]

    for metric_name, metric_value in results.items():
        if metric_name.startswith("_"):
            continue
        metrics.append(
            Metric(
                run_id=run_id,
                target_table=fq_table_name,
                target_column=column_name,
                metric_name=metric_name,
                metric_value=metric_value,
                measured_at=measured_at,
            )
        )
    return metrics


def extract_histogram(
    run_id: int, fq_table: FqTable, col: str, results: Dict[str, Any]
) -> List[Metric]:
    return [
        Metric(
            run_id=run_id,
            target_table=fq_table,
            target_column=col,
            metric_name="numeric_histogram",
            metric_value=(
                results["NUMERIC_HISTOGRAM"]
                if isinstance(results["NUMERIC_HISTOGRAM"], dict)
                else json.loads(results["NUMERIC_HISTOGRAM"])
            ),
            measured_at=results["_MEASURED_AT"],
        )
    ]


def extract_datetime_histogram(
    run_id: int, fq_table: FqTable, col: str, results: Dict[str, Any]
) -> List[Metric]:
    return [
        Metric(
            run_id=run_id,
            target_table=fq_table,
            target_column=col,
            metric_name="datetime_histogram",
            metric_value=json.loads(results["DATETIME_HISTOGRAM"]),
            measured_at=results["_measured_at"],
        )
    ]


def query_priority(mq: MetricQuery) -> int:
    """Return priority order: lower number = higher priority"""
    if "histogram" in mq.query_detail or "percentile" in mq.query_detail:
        return 0  # High priority
    else:
        return 1  # Normal priority (simple metrics)
