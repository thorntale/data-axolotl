import time
import json
import traceback

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
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
)

import snowflake.connector
from snowflake.connector import DictCursor

from .config import AxolotlConfig, Database
from .state_dao import Metric


def to_string_array(items: List[str]) -> str:
    """
    takes ["a", "b", "c"] -> "('a', 'b', 'c')"
    """
    return f"""('{ "', '".join(items)}')"""


class SimpleDataType(Enum):
    """Simple categorization of Snowflake data types"""

    BOOLEAN = "boolean"
    NUMERIC = "numeric"
    STRING = "string"
    DATETIME = "datetime"
    STRUCTURED = "structured"
    UNSTRUCTURED = "unstructured"
    VECTOR = "vector"
    OTHER = "other"


class QueryStatus(Enum):
    """Status of a running query"""

    QUEUED = "queued"
    STARTED = "started"
    DONE = "done"
    ERROR = "error"


class ColumnInfo(NamedTuple):
    fq_table_name: str
    column_name: str
    data_type: str
    data_type_simple: SimpleDataType
    database: Database


def extract_simple_metrics(
    run_id: int, fq_table_name: str, column_name: str, results: Dict[str, Any]
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


class MetricQuery(NamedTuple):
    # Query execution details
    query: str  # SQL query string to execute
    query_id: str  # Snowflake query ID (set after execution)
    timeout_at: float  # monotonic time at which to timeout this query
    status: QueryStatus  # Current status of the query

    # debug info
    fq_table_name: str
    column_name: str
    query_detail: str

    # query results into Metric objects
    # Takes run_id, fq_table_name, column_name, and dict of query results, returns list of Metrics
    result_extractor: Callable[[int, str, str, Dict[str, Any]], List[Metric]] = (
        extract_simple_metrics
    )


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


class SnowflakeConn:
    """
    Wraps a Snowflake conn in order to take a snapshot of a list of databases
    and schemas.
    """

    def __init__(self, config: AxolotlConfig, connection_name: str, run_id: int):
        """
        Initialize a Snowflake connection.

        Args:
            config: AxolotlConfig object containing all configuration
            connection_name: Name of the connection to use from config.connections
            run_id: Unique identifier for this snapshot run
        """
        if connection_name not in config.connections:
            raise ValueError(
                f"Connection '{connection_name}' not found in config. "
                f"Available connections: {list(config.connections.keys())}"
            )

        # Store connection options and metrics config
        self.connection_options = config.connections[connection_name]
        self.metrics_config = config.metrics_config
        self.per_run_timeout_seconds  = config.per_run_timeout_seconds  

        self.databases = self.connection_options.databases
        self.run_id = run_id

        # Prepare connection parameters for Snowflake connector
        # Convert Pydantic model to dict and filter out custom fields
        conn_params = self.connection_options.model_dump(
            exclude_none=True,
            exclude={"metrics", "type", "include_schemas", "exclude_schemas"},
        )

        # Handle private_key_file -> read and convert to private_key
        if "private_key_file" in conn_params:
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization

            private_key_file = conn_params.pop("private_key_file")
            private_key_pwd = conn_params.pop("private_key_file_pwd", None)

            with open(private_key_file, "rb") as key_file:
                private_key_data = key_file.read()

            password_bytes = private_key_pwd.encode() if private_key_pwd else None

            private_key = serialization.load_pem_private_key(
                private_key_data, password=password_bytes, backend=default_backend()
            )

            pkb = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            conn_params["private_key"] = pkb
        try:
            self.conn = snowflake.connector.connect(**conn_params)

        except Exception:
            # TODO Don't print sensitive connection params
            print(f"Failed to create snowflake connection: {connection_name}")
            print(traceback.format_exc())
            raise

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
        # Return None to propagate any exceptions
        return None

    def run_metric_query(self, metric_query: MetricQuery) -> MetricQuery:
        """
        Execute a MetricQuery asynchronously and return an updated MetricQuery with
        the query_id, updated timeout_at, and status set to STARTED.

        Args:
            metric_query: MetricQuery with query string and status=QUEUED

        Returns:
            Updated MetricQuery with query_id, timeout_at, and status=STARTED or ERROR
        """
        try:
            with self.conn.cursor(DictCursor) as cur:
                cur.execute_async(metric_query.query)
                # Return updated MetricQuery with the query_id and STARTED status
                return metric_query._replace(
                    query_id=cur.sfqid,
                    timeout_at=time.monotonic()
                    + self.metrics_config.per_query_timeout_seconds,
                    status=QueryStatus.STARTED,
                )
        except Exception:
            print(f"Error executing query: {metric_query.query}")
            print(traceback.format_exc())
            # Return updated MetricQuery with ERROR status
            return metric_query._replace(status=QueryStatus.ERROR)

    def _snapshot_database(
        self, db_name: str, database: Database, t_run_timeout: float
    ) -> List[Metric]:
        all_column_infos: List[ColumnInfo] = []

        t_db_start = time.monotonic()
        t_db_timeout = t_db_start + database.metrics_config.per_database_timeout_seconds
        metrics: List[Metric] = []

        (table_metrics, table_names) = self.scan_table_level_metrics(database)
        t_tablescan = time.monotonic()  ## Time it took to scan the table metrics
        print(
            f"Found {len(table_names)} tables in {round(t_tablescan-t_db_start, 2)} seconds in {db_name}({database.database}). Enumerating columns..."
        )
        metrics.extend(table_metrics)

        all_column_infos.extend(self.list_columns(database))

        print(f"Generating queries for {len(all_column_infos)} columns...")
        all_metric_queries: List[MetricQuery] = []
        for c_info in all_column_infos:
            all_metric_queries.extend(self._generate_queries(c_info))

        def query_priority(mq: MetricQuery) -> int:
            """Return priority order: lower number = higher priority"""
            if "histogram" in mq.query_detail or "percentile" in mq.query_detail:
                return 0  # High priority
            else:
                return 1  # Normal priority (simple metrics)

        all_metric_queries.sort(key=query_priority)

        print(
            f"Snapshotting {len(all_metric_queries)} queries across {len(self.databases)} databases..."
        )
        num_queries = len(all_metric_queries)
        i = 0  # Index into all_metric_queries
        j = 0  # Count of completed queries

        running_queries: Dict[str, MetricQuery] = (
            {}
        )  # Track queries currently in flight

        while True:
            if len(running_queries) == 0 and i == num_queries:
                return metrics

            # check if any running queries are done,
            removed: List[str] = []
            for qid, rq in running_queries.items():
                if not self.conn.is_still_running(self.conn.get_query_status(qid)):
                    try:
                        with self.conn.cursor(DictCursor) as cur:
                            cur.get_results_from_sfqid(qid)
                            results = cur.fetchone()
                            # Use the result_extractor from the MetricQuery
                            extracted_metrics = rq.result_extractor(
                                self.run_id, rq.fq_table_name, rq.column_name, results
                            )
                            metrics.extend(extracted_metrics)
                            # Update status to DONE
                            running_queries[qid] = rq._replace(status=QueryStatus.DONE)
                            print(
                                f"Done ({qid}): {rq.fq_table_name}.{rq.column_name} [{rq.query_detail}]"
                            )
                            removed.append(qid)
                            j += 1
                    except Exception:
                        print(
                            f"Error processing results for ({qid}): {rq.fq_table_name}.{rq.column_name} [{rq.query_detail}]"
                        )
                        print(traceback.format_exc())
                        # Update status to ERROR
                        running_queries[qid] = rq._replace(status=QueryStatus.ERROR)
                        removed.append(qid)

                elif rq.timeout_at < time.monotonic():
                    # if we're done with this query, yoink it
                    with self.conn.cursor() as cur:
                        cur.execute(f"SELECT SYSTEM$CANCEL_QUERY('{rq.query_id}')")
                    # Update status to ERROR for timeout
                    running_queries[qid] = rq._replace(status=QueryStatus.ERROR)
                    removed.append(qid)
                    print(
                        f"Timed out: {rq.fq_table_name}.{rq.column_name} [{rq.query_detail}]"
                    )

            # Clear our tracking dict; couldn't do that inside the loop
            for r in removed:
                del running_queries[r]

            # Check run-level timeout
            t_check = time.monotonic()
            if t_run_timeout < t_check:
                print(
                    f"Timed out in {self.per_run_timeout_seconds} seconds: returning early having completed {j}/{num_queries} queries"
                )
                return metrics
            if t_db_timeout < t_check:
                print(
                    f"Timed out for {db_name} in {database.metrics_config.per_database_timeout_seconds} seconds: returning early having completed {j}/{num_queries} queries"
                )
                return metrics

            # Fill our headroom with more queries
            while (
                self.metrics_config.max_threads > len(running_queries)
                and i < num_queries
            ):
                queued_query = all_metric_queries[i]
                # Execute the query using run_metric_query
                started_query = self.run_metric_query(queued_query)
                if started_query.status == QueryStatus.STARTED:
                    running_queries[started_query.query_id] = started_query
                    print(
                        f"Started ({started_query.query_id}): {started_query.fq_table_name}.{started_query.column_name} [{started_query.query_detail}]"
                    )
                else:
                    # Query failed to start, skip it
                    print(
                        f"Failed to start: {queued_query.fq_table_name}.{queued_query.column_name} [{queued_query.query_detail}]"
                    )
                i += 1
            time.sleep(1 / 1000)

    def snapshot(self, t_run_start: float) -> Iterator[List[Metric]]:
        """
        Capture metrics for all tables in the configured databases/schemas, both
        table-level and column-level for all tables

        Args:
            t_run_start: Time at which this run started. Used for timing out

        Returns:
            List of Metric objects containing all collected metrics
        """

        t_run_timeout = (
            t_run_start + self.per_run_timeout_seconds
        )

        for db_name, database in self.databases.items():
            yield self._snapshot_database(db_name, database, t_run_timeout)
            if time.monotonic() > t_run_timeout:
                print(
                    f"Timed out in {self.per_run_timeout_seconds} seconds"
                )
                return
        
        print(f"Finished in {round(time.monotonic()-t_run_start, 2)} seconds")

    def _simple_queries(
        self,
        column_info: ColumnInfo,
    ) -> dict[str, str]:
        """
        The expensive queries are distinct_count (and therefore distinct_rate), and string_avg_length
        """

        data_type_simple = column_info.data_type_simple
        col_sql = f'c."{column_info.column_name}"'
        queries = {
            "row_count": "COUNT(*)",
            "null_count": f"COUNT_IF({col_sql} IS NULL)",
            "null_pct": f'100.0 * "null_count" / "row_count"',
        }

        if not column_info.database.metrics_config.exclude_expensive_queries:
            queries.update(
                {
                    "distinct_count": f"COUNT(DISTINCT({col_sql}))",
                    "distinct_rate": f'100.0 * "distinct_count" / ("row_count" - "null_count")',
                }
            )

        # Type-specific queries
        if data_type_simple == SimpleDataType.NUMERIC:
            queries.update(
                {
                    "numeric_min": f"MIN({col_sql})",
                    "numeric_max": f"MAX({col_sql})",
                    "numeric_mean": f"AVG({col_sql})",
                    "numeric_stddev": f"STDDEV({col_sql}::float)",
                }
            )

        elif data_type_simple == SimpleDataType.STRING:
            if not column_info.database.metrics_config.exclude_expensive_queries:
                queries.update(
                    {
                        "string_avg_length": f"AVG(LEN({col_sql}))",
                    }
                )

        elif data_type_simple == SimpleDataType.BOOLEAN:
            queries.update(
                {
                    "true_count": f"COUNT_IF({col_sql} = TRUE)",
                    "false_count": f"COUNT_IF({col_sql} = FALSE)",
                }
            )
        elif data_type_simple == SimpleDataType.DATETIME:
            queries.update(
                {
                    "datetime_min": f"MIN({col_sql})",
                    "datetime_max": f"MAX({col_sql})",
                }
            )

        return queries

    def _generate_queries(self, column_info: ColumnInfo) -> List[MetricQuery]:
        """
        Generate a list of MetricQuery objects for a given column based on its type.
        This function does not execute queries, only creates the query objects in QUEUED state.

        Args:
            column_info: Information about the column to generate queries for

        Returns:
            List of MetricQuery objects in QUEUED state
        """
        (fq_table_name, column_name, _, data_type_simple, database) = column_info
        queries: List[MetricQuery] = []

        # 1. Generate simple metrics query (always included)
        query_columns = self._simple_queries(column_info)
        simple_query = f"""
            SELECT CURRENT_TIMESTAMP() as "_measured_at",
                {",\n".join(
                    f'{metric_query} AS "{metric_name}"'
                    for metric_name, metric_query
                    in query_columns.items()
                )}
            FROM {fq_table_name} as c
        """

        queries.append(
            MetricQuery(
                query=simple_query,
                query_id="",
                timeout_at=0.0,
                status=QueryStatus.QUEUED,
                fq_table_name=fq_table_name,
                column_name=column_name,
                query_detail="simple_metrics",
            )
        )

        # 2. Generate complex queries based on data type (if not excluded)
        if not database.metrics_config.exclude_complex_queries:
            if data_type_simple == SimpleDataType.NUMERIC:
                # Percentiles query
                percentile_query = f"""
                    WITH percentile_state AS (
                        SELECT
                            APPROX_PERCENTILE_ACCUMULATE("{column_name}") AS col_state
                        FROM {fq_table_name}
                    )
                    SELECT CURRENT_TIMESTAMP() as "_measured_at",
                    OBJECT_CONSTRUCT(
                        '0p',  APPROX_PERCENTILE_ESTIMATE(col_state, 0.00),
                        '1p',  APPROX_PERCENTILE_ESTIMATE(col_state, 0.01),
                        '5p',  APPROX_PERCENTILE_ESTIMATE(col_state, 0.05),
                        '10p', APPROX_PERCENTILE_ESTIMATE(col_state, 0.10),
                        '20p', APPROX_PERCENTILE_ESTIMATE(col_state, 0.20),
                        '30p', APPROX_PERCENTILE_ESTIMATE(col_state, 0.30),
                        '40p', APPROX_PERCENTILE_ESTIMATE(col_state, 0.40),
                        '50p', APPROX_PERCENTILE_ESTIMATE(col_state, 0.50),
                        '60p', APPROX_PERCENTILE_ESTIMATE(col_state, 0.60),
                        '70p', APPROX_PERCENTILE_ESTIMATE(col_state, 0.70),
                        '80p', APPROX_PERCENTILE_ESTIMATE(col_state, 0.80),
                        '90p', APPROX_PERCENTILE_ESTIMATE(col_state, 0.90),
                        '95p', APPROX_PERCENTILE_ESTIMATE(col_state, 0.95),
                        '99p', APPROX_PERCENTILE_ESTIMATE(col_state, 0.99),
                        '100p', APPROX_PERCENTILE_ESTIMATE(col_state, 1.00)
                    ) as numeric_percentiles
                    FROM percentile_state;
                """

                # Custom extractor for percentiles
                def extract_percentiles(
                    run_id: int, fq_table: str, col: str, results: Dict[str, Any]
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

                queries.append(
                    MetricQuery(
                        query=percentile_query,
                        query_id="",
                        timeout_at=0.0,
                        status=QueryStatus.QUEUED,
                        fq_table_name=fq_table_name,
                        column_name=column_name,
                        query_detail="numeric_percentiles",
                        result_extractor=extract_percentiles,
                    )
                )

                # Histogram query - handles both min==max and min!=max cases in one query
                num_buckets = 10
                histogram_query = f"""
                    WITH minmax AS (
                        SELECT
                            MIN(t."{column_name}") AS COL_MIN,
                            MAX(t."{column_name}") AS COL_MAX,
                            COUNT(*) AS ROW_COUNT,
                            COUNT_IF(t."{column_name}" IS NULL) AS NULL_COUNT
                        FROM {fq_table_name} as t
                    ),
                    histogram_buckets AS (
                        SELECT
                            LEAST(GREATEST(WIDTH_BUCKET(t."{column_name}",
                                (SELECT COL_MIN FROM minmax),
                                (SELECT COL_MAX FROM minmax),
                                {num_buckets}
                            ), 1), {num_buckets}) as BUCKET_I,
                            COUNT(*) as COUNT
                        FROM {fq_table_name} AS t
                        CROSS JOIN minmax
                        WHERE minmax.COL_MIN != minmax.COL_MAX
                        GROUP BY BUCKET_I
                    ),
                    empty_buckets as (
                        SELECT
                            column1 as BUCKET_I,
                            0 as COUNT
                        FROM VALUES {', '.join(f'({i})' for i in range(1, num_buckets + 1))}
                        CROSS JOIN minmax
                        WHERE minmax.COL_MIN != minmax.COL_MAX
                    ),
                    dense_buckets as (
                        SELECT
                            BUCKET_I * ((SELECT COL_MAX - COL_MIN FROM minmax) / {num_buckets}) + (SELECT COL_MIN FROM minmax) as BUCKET,
                            sum(COUNT) as COUNT
                        FROM (
                            SELECT * FROM histogram_buckets
                            UNION ALL
                            SELECT * FROM empty_buckets
                        )
                        GROUP BY BUCKET_I
                    )
                    SELECT
                        CURRENT_TIMESTAMP() as _MEASURED_AT,
                        CASE
                            WHEN (SELECT COL_MIN FROM minmax) = (SELECT COL_MAX FROM minmax) THEN
                                -- Single bucket case: all values are the same
                                OBJECT_CONSTRUCT((SELECT COL_MAX FROM minmax)::VARCHAR, (SELECT ROW_COUNT - NULL_COUNT FROM minmax))
                            ELSE
                                -- Multi-bucket case: use computed histogram
                                (SELECT OBJECT_AGG(BUCKET::VARCHAR, COUNT) FROM dense_buckets)
                        END as NUMERIC_HISTOGRAM
                    FROM minmax;
                """

                # Custom extractor for histogram
                def extract_histogram(
                    run_id: int, fq_table: str, col: str, results: Dict[str, Any]
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

                queries.append(
                    MetricQuery(
                        query=histogram_query,
                        query_id="",
                        timeout_at=0.0,
                        status=QueryStatus.QUEUED,
                        fq_table_name=fq_table_name,
                        column_name=column_name,
                        query_detail="numeric_histogram",
                        result_extractor=extract_histogram,
                    )
                )

            elif data_type_simple == SimpleDataType.DATETIME:
                # Datetime histogram query
                histogram_query = f"""
                    with bucket_config as (
                        select
                            min(t."{column_name}")
                                as col_min,
                            max(t."{column_name}")
                                as col_max,
                            datediff(hour, col_min, col_max)
                                as date_diff_hours,
                            datediff(day, col_min, col_max)
                                as date_diff_days,
                            datediff(month, col_min, col_max)
                                as date_diff_months,
                            datediff(year, col_min, col_max)
                                as date_diff_years,
                            case
                                when date_diff_days < 1 then 'hour'
                                when date_diff_days <= 31 then 'day'
                                when date_diff_months <= 36 then 'month'
                                else 'year'
                            end
                                as bucket_unit,
                            greatest(1.0, ceil(date_diff_years / 30.0)) as years_per_bin
                        from {fq_table_name} as t
                    ),
                    histogram_buckets AS (
                        SELECT
                            CASE bc.BUCKET_UNIT
                                WHEN 'hour' THEN
                                    DATE_TRUNC('hour', t."{column_name}")
                                WHEN 'day' THEN
                                    DATE_TRUNC('day', t."{column_name}")
                                WHEN 'month' THEN
                                    DATE_TRUNC('month', t."{column_name}")
                                WHEN 'year' THEN
                                    DATEADD('year',
                                        bc.years_per_bin * floor((year(t."{column_name}") - year(bc.col_min)) / bc.years_per_bin),
                                        DATE_TRUNC('year', bc.col_min)
                                    )
                            END AS bucket_timestamp,
                            bc.BUCKET_UNIT,
                            COUNT(*) as count
                        FROM {fq_table_name} as t
                        CROSS JOIN bucket_config as bc
                        GROUP BY bucket_timestamp, bc.BUCKET_UNIT
                    ),
                    empty_buckets as (
                        SELECT
                            v.column1 as bucket_i,
                            case bc.bucket_unit
                                when 'hour' then
                                    dateadd(hour, bucket_i, DATE_TRUNC(hour, col_min))
                                when 'day' then
                                    dateadd(day, bucket_i, DATE_TRUNC(day, col_min))
                                when 'month' then
                                    dateadd(month, bucket_i, DATE_TRUNC(month, col_min))
                                when 'year' then
                                    dateadd(year, bucket_i * bc.years_per_bin, DATE_TRUNC(year, col_min))
                            end as bucket_timestamp,
                            bc.BUCKET_UNIT,
                            0 as count
                        FROM VALUES {', '.join(f'({i})' for i in range(0, 40))} as v
                        CROSS JOIN bucket_config as bc
                        where
                            bucket_timestamp > bc.col_min
                            and bucket_timestamp < bc.col_max
                    ),
                    formatted_buckets AS (
                        SELECT
                            CASE BUCKET_UNIT
                                WHEN 'hour' THEN TO_CHAR(bucket_timestamp, 'YYYY-MM-DD:HH24')
                                WHEN 'day' THEN TO_CHAR(bucket_timestamp, 'YYYY-MM-DD')
                                WHEN 'month' THEN TO_CHAR(bucket_timestamp, 'YYYY-MM')
                                WHEN 'year' THEN TO_CHAR(bucket_timestamp, 'YYYY')
                            END AS bucket,
                            sum(count) as count
                        FROM (
                            select BUCKET_UNIT, bucket_timestamp, count from histogram_buckets
                            UNION ALL
                            select BUCKET_UNIT, bucket_timestamp, count from empty_buckets
                        )
                        group by 1
                    )
                    SELECT
                        CURRENT_TIMESTAMP() as "_measured_at",
                        OBJECT_AGG(bucket, count) as datetime_histogram
                    FROM formatted_buckets;
                """

                # Custom extractor for datetime histogram
                def extract_datetime_histogram(
                    run_id: int, fq_table: str, col: str, results: Dict[str, Any]
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

                queries.append(
                    MetricQuery(
                        query=histogram_query,
                        query_id="",
                        timeout_at=0.0,
                        status=QueryStatus.QUEUED,
                        fq_table_name=fq_table_name,
                        column_name=column_name,
                        query_detail="datetime_histogram",
                        result_extractor=extract_datetime_histogram,
                    )
                )

        return queries

    def list_columns(self, database: Database) -> List[ColumnInfo]:
        """
        List all columns in a database, respecting included and excluded schemas

        Args:
        Returns:
            ColumnInfo: all the information to scan the column
        """
        column_info_arr: List[ColumnInfo] = []

        if len(database.include_schemas) > 0:
            print(database.include_schemas)
            table_schema_clause = (
                f"WHERE TABLE_SCHEMA IN {to_string_array(database.include_schemas)}"
            )
        elif len(database.exclude_schemas) > 0:
            table_schema_clause = (
                f"WHERE TABLE_SCHEMA NOT IN {to_string_array(database.exclude_schemas)}"
            )
        else:
            table_schema_clause = ""

        # fq_table_name = f"{self.database}.{self.table_schema}.{table_name}"
        with self.conn.cursor() as cur:
            query = f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, TABLE_SCHEMA, TABLE_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    {table_schema_clause}
                """
            cur.execute(f"USE DATABASE {database.database};")
            cur.execute(query)

            for column_name, data_type, is_nullable, table_schema, table_name in cur:
                fq_table_name = f"{database.database}.{table_schema}.{table_name}"
                column_info_arr.append(
                    ColumnInfo(
                        fq_table_name=fq_table_name,
                        column_name=column_name,
                        data_type=data_type,
                        data_type_simple=get_simple_data_type(data_type),
                        database=database,
                    )
                )

        return column_info_arr

    def scan_table_level_metrics(
        self, database: Database
    ) -> Tuple[List[Metric], List[str]]:
        """
        Collect table-level metrics for all tables in the configured database/schema.

        Queries INFORMATION_SCHEMA.TABLES to collect metrics like row_count and bytes
        for each table.

        Args:
            run_id: Unique identifier for this snapshot run

        Returns:
            Tuple containing:
                - List of Metric objects with table-level metrics
                - List of table names found in the schema
        """
        metrics = []
        table_names = []
        fq_table_names = []

        if len(database.include_schemas) > 0:
            print(database.include_schemas)
            table_schema_clause = (
                f"WHERE TABLE_SCHEMA IN {to_string_array(database.include_schemas)}"
            )
        elif len(database.exclude_schemas) > 0:
            table_schema_clause = (
                f"WHERE TABLE_SCHEMA NOT IN {to_string_array(database.exclude_schemas)}"
            )
        else:
            table_schema_clause = ""

        q = f"""
                    SELECT
                        TABLE_CATALOG,
                        TABLE_SCHEMA,
                        TABLE_NAME,
                        ROW_COUNT,
                        BYTES,
                        CREATED,
                        LAST_ALTERED,
                        CURRENT_TIMESTAMP() as measured_at
                    FROM INFORMATION_SCHEMA.TABLES
                    {table_schema_clause};
                """
        with self.conn.cursor() as cur:
            try:
                cur.execute(f"USE DATABASE {database.database};")
                cur.execute(q)

            except Exception:
                print("Error running table scan query")
                print(traceback.format_exc())
                raise

            for (
                table_catalog,
                table_schema,
                table_name,
                row_count,
                table_bytes,
                created,
                last_altered,
                measured_at,
            ) in cur:
                g_measured_at = measured_at
                table_names.append(table_name)
                fq_table_name = f"{table_catalog}.{table_schema}.{table_name}"
                fq_table_names.append(fq_table_name)

                metrics += [
                    Metric(
                        run_id=self.run_id,
                        target_table=fq_table_name,
                        target_column=None,
                        metric_name="row_count",
                        metric_value=row_count,
                        measured_at=measured_at,
                    ),
                    Metric(
                        run_id=self.run_id,
                        target_table=fq_table_name,
                        target_column=None,
                        metric_name="bytes",
                        metric_value=table_bytes,
                        measured_at=measured_at,
                    ),
                    Metric(
                        run_id=self.run_id,
                        target_table=fq_table_name,
                        target_column=None,
                        metric_name="created_at",
                        metric_value=created,
                        measured_at=measured_at,
                    ),
                    Metric(
                        run_id=self.run_id,
                        target_table=fq_table_name,
                        target_column=None,
                        metric_name="altered_at",
                        metric_value=last_altered,
                        measured_at=measured_at,
                    ),
                ]

        with self.conn.cursor() as cur:
            try:
                cur.execute(
                    f"""
                    select {','.join(
                        f"TO_TIMESTAMP_TZ(SYSTEM$LAST_CHANGE_COMMIT_TIME('{table_name}') / 1e9)"
                        for table_name in fq_table_names
                    )}
                """
                )
                results = cur.fetchone()
            except Exception:
                print("Error getting table update times")
                print(traceback.format_exc())
                raise

            metrics += [
                Metric(
                    run_id=self.run_id,
                    target_table=table_name,
                    target_column=None,
                    metric_name="updated_at",
                    metric_value=col,
                    measured_at=measured_at,
                )
                for col, table_name in zip(results, fq_table_names)
            ]

        return metrics, table_names
