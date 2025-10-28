import time
import json
import traceback

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from typing import NamedTuple, List, Tuple, TypedDict, NotRequired, Dict, Any

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


class ColumnInfo(NamedTuple):
    fq_table_name: str
    column_name: str
    data_type: str
    data_type_simple: SimpleDataType
    database: str ## the name of the db


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

        options = config.connections[connection_name]

        self.databases = options.databases
        #self.include_schemas = options.include_schemas
        #self.exclude_schemas = options.exclude_schemas

        self.run_id = run_id

        # Get metrics config (use connection-specific or default)
        metrics_config = options.metricsConfig or config.default_metrics_config
        self.max_threads = metrics_config.max_threads
        self.per_query_timeout_seconds = metrics_config.per_query_timeout_seconds
        self.per_column_timeout_seconds = metrics_config.per_column_timeout_seconds
        self.per_run_timeout_seconds = metrics_config.per_run_timeout_seconds

        self.exclude_expensive_queries = metrics_config.exclude_expensive_queries
        self.exclude_complex_queries = metrics_config.exclude_complex_queries

        # Prepare connection parameters for Snowflake connector
        # Convert Pydantic model to dict and filter out custom fields
        conn_params = options.model_dump(
            exclude_none=True,
            exclude={"metricsConfig", "type", "include_schemas", "exclude_schemas"},
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
            # Don't print sensitive connection params
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

    def snapshot(self) -> Iterator[List[Metric]]:
        """
        Capture metrics for all tables in the configured databases/schemas, both
        table-level and column-level for all tables

        Args:
            run_id: Unique identifier for this snapshot run

        Returns:
            List of Metric objects containing all collected metrics
        """

        for db_name, database in self.databases.items():
            t0 = time.monotonic()
            (metrics, table_names) = self.scan_table_level_metrics(database)
            t1 = time.monotonic()  ## Time it took to scan the table metrics

            print(
                f"Found {len(table_names)} tables in {round(t1-t0, 2)} seconds in {db_name}({database.database}). Enumerating columns..."
            )

            yield metrics

            all_column_infos = self.list_columns(database)

            print(f"Snapshotting {len(all_column_infos)} columns...")

            executor = ThreadPoolExecutor(max_workers=self.max_threads)
            try:
                for column_metrics in executor.map(
                    self.scan_column, all_column_infos, chunksize=1
                ):
                    yield column_metrics
            except Exception:
                print("Error getting running scan queries")
                print(traceback.format_exc())
                raise
            finally:
                # wait for all queries to complete before returning
                executor.shutdown(wait=True)
                t2 = time.monotonic()  ## Time at which we finished scanning all the columns

                print(f"Snapshotted columns in {round(t2-t1, 2)} seconds")
        

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

        if not self.exclude_expensive_queries:
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
            if not self.exclude_expensive_queries:
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

    def _package_metrics(
        self, column_info: ColumnInfo, query_results: Dict[str, Any]
    ) -> List[Metric]:
        """
        Takes the output of _query_values and packages them into metrics list
        """
        metrics: List[Metric] = []
        (fq_table_name, column_name, _, _, _) = column_info

        if not query_results["_measured_at"]:
            raise ValueError("missing _measured_at")

        for metric_name, metric_value in query_results.items():
            if metric_name.startswith("_"):
                continue

            metrics.append(
                Metric(
                    run_id=self.run_id,
                    target_table=fq_table_name,
                    target_column=column_name,
                    metric_name=metric_name,
                    metric_value=metric_value,
                    measured_at=query_results["_measured_at"],
                )
            )
        return metrics

    def _query_values_benchmark(
        self, query_columns: dict[str, str], column_info: ColumnInfo
    ) -> Dict[str, float]:
        """
        INTERNAL ONLY. Runs each query_column query separately, and instead of
        the result, returns a dict { metric_name : query duration}
        """

        (fq_table_name, _, _, _) = column_info
        res: Dict[str, float] = {}

        for metric_name, metric_query in query_columns.items():
            query = f"""
                SELECT {metric_query} AS "{metric_name}"
                FROM {fq_table_name} as c
            """
            with self.conn.cursor(DictCursor) as cur:
                try:
                    t0 = time.monotonic()
                    cur.execute(query)
                    results = cur.fetchone()
                    t1 = time.monotonic()
                    if not results:
                        raise ValueError("No results found")

                    res[metric_name] = t1 - t0

                except Exception:
                    print(f"Error executing query: {query}")
                    print(traceback.format_exc())
                    raise
        return res

    def _query_values(
        self, query_columns: dict[str, str], column_info: ColumnInfo
    ) -> Dict[str, Any]:
        """
        Use this instead of _query_metrics when you need to do something else to
        the results before packaging them.

        This packages the results into a dict[metric name, metric value] instaed
        of a list of metrics.
        """

        (fq_table_name, _, _, _, database) = column_info
        res: Dict[str, Any] = {}

        query = f"""
            SELECT CURRENT_TIMESTAMP() as "_measured_at",
                {",\n".join(
                    f'{metric_query} AS "{metric_name}"'
                    for metric_name, metric_query
                    in query_columns.items()
                )}
            FROM {fq_table_name} as c
        """

        with self.conn.cursor(DictCursor) as cur:
            try:
                cur.execute(
                    f"USE DATABASE {database};"
                )
                cur.execute(query)
                results = cur.fetchone()
                if not results:
                    raise ValueError("No results found")

            except Exception:
                print(f"Error executing query: {query}")
                print(traceback.format_exc())
                raise
            res = results

        return res

    def _query_metrics(
        self, query_columns: dict[str, str], column_info: ColumnInfo
    ) -> List[Metric]:
        """
        Given the column info and a dict of metric names -> select clauses,
        - compose and run the query
        - package the results into a list of Metrics

        Synchronous. Only works on simple queries that don't require CTEs or
        joins. The FROM clause is hard-coded to column_info.fq_table_name.
        """

        (fq_table_name, column_name, _, _, database) = column_info
        metrics: List[Metric] = []

        query = f"""
            SELECT CURRENT_TIMESTAMP() as "_measured_at",
                {",\n".join(
                    f'{metric_query} AS "{metric_name}"'
                    for metric_name, metric_query
                    in query_columns.items()
                )}
            FROM {fq_table_name} as c
        """

        with self.conn.cursor(DictCursor) as cur:
            try:
                cur.execute(
                    f"USE DATABASE {database};"
                )
                cur.execute(query)
                # return cur.query_id

                # query_ids.append(cur.sfqid)
                results = cur.fetchone()
            except Exception:
                print(f"Error executing query: {query}")
                print(traceback.format_exc())
                raise

            for metric_name, metric_value in results.items():
                if metric_name.startswith("_"):
                    continue
                metrics.append(
                    Metric(
                        run_id=self.run_id,
                        target_table=fq_table_name,
                        target_column=column_name,
                        metric_name=metric_name,
                        metric_value=metric_value,
                        measured_at=results["_measured_at"],
                    )
                )
        return metrics

    def scan_numeric_column(
        self, column_info: ColumnInfo, simple_queries_only: bool = True
    ) -> List[Metric]:
        """
        Scan a single column of Numeric type. This involves simple stats as well
        as histograms and percentiles.

        Returns:
            List of Metrics for this column
        """

        (fq_table_name, column_name, data_type, data_type_simple, database) = column_info

        if data_type_simple != SimpleDataType.NUMERIC:
            # traceback.print_exc()
            raise ValueError(
                f"{fq_table_name}.{column_name} ({data_type}) is {data_type_simple.value}, not numeric"
            )

        query_columns = self._simple_queries(column_info)

        ## Get the simple metrics first
        query_values = self._query_values(query_columns, column_info)


        ## TODO add column_type back in
        metrics = self._package_metrics(column_info, query_values)
        if self.exclude_complex_queries: 
            return metrics

        percentile_query = f"""
                    WITH percentile_state AS (
                        SELECT
                            APPROX_PERCENTILE_ACCUMULATE("{column_name}") AS col_state
                        FROM {fq_table_name}
                    )
                    SELECT CURRENT_TIMESTAMP() as MEASURED_AT,
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
                    ) as NUMERIC_PERCENTILES
                    FROM percentile_state;
                """

        try:
            with self.conn.cursor(DictCursor) as cur:
                cur.execute(
                    f"USE DATABASE {database};"
                )
                cur.execute(percentile_query)
                results = cur.fetchone()
                assert results is not None
                measured_at = results["MEASURED_AT"]
                metrics.append(
                    Metric(
                        run_id=self.run_id,
                        target_table=fq_table_name,
                        target_column=column_name,
                        metric_name="numeric_percentiles",
                        metric_value=json.loads(results["NUMERIC_PERCENTILES"]),
                        measured_at=measured_at,
                    )
                )
        except Exception:
            print(f"Error running percentile query: {percentile_query}")
            print(traceback.format_exc())
            raise

        if query_values["numeric_max"] == query_values["numeric_min"]:
            ## If all values are the same, just put them in one bucket.
            metrics.append(
                Metric(
                    run_id=self.run_id,
                    target_table=fq_table_name,
                    target_column=column_name,
                    metric_name="numeric_histogram",
                    metric_value={
                        query_values["numeric_max"]: query_values["row_count"]
                        - query_values["null_count"],
                    },
                    measured_at=query_values["_measured_at"],
                )
            )
        else:
            num_buckets = 10
            try:
                histogram_query = f"""
                        WITH cte AS (
                            SELECT
                                MIN(t."{column_name}") AS COL_MIN,
                                MAX(t."{column_name}") AS COL_MAX,
                                (COL_MAX - COL_MIN) / {num_buckets} AS BUCKET_SIZE
                            FROM {fq_table_name} as t
                        ),
                        histogram_buckets AS (
                            SELECT
                                LEAST(GREATEST(WIDTH_BUCKET(t."{column_name}",
                                    (SELECT COL_MIN FROM cte),
                                    (SELECT COL_MAX FROM cte),
                                    {num_buckets}
                                ), 1), {num_buckets}) as bucket_i,
                                COUNT(*) as count
                            FROM {fq_table_name} AS t
                            GROUP BY bucket_i
                        ),
                        empty_buckets as (
                            SELECT
                                column1 as bucket_i,
                                0 as count
                            FROM VALUES {', '.join(f'({i})' for i in range(1, num_buckets + 1))}
                        ),
                        dense_buckets as (
                            select
                                bucket_i * (SELECT BUCKET_SIZE FROM cte) + (select COL_MIN FROM cte) as bucket,
                                sum(count) as count
                            from (
                                select * from histogram_buckets
                                UNION ALL
                                select * from empty_buckets
                            )
                            group by bucket_i
                        )
                        SELECT
                            CURRENT_TIMESTAMP() as MEASURED_AT,
                            OBJECT_AGG(bucket::VARCHAR, count) as NUMERIC_HISTOGRAM
                        FROM dense_buckets;
                    """
                with self.conn.cursor(DictCursor) as cur:
                    cur.execute(
                        f"USE DATABASE {database};"
                    )
                    cur.execute(histogram_query)
                    results = cur.fetchone()
                    assert results is not None
                    measured_at = results["MEASURED_AT"]
                    metrics.append(
                        Metric(
                            run_id=self.run_id,
                            target_table=fq_table_name,
                            target_column=column_name,
                            metric_name="numeric_histogram",
                            metric_value=json.loads(results["NUMERIC_HISTOGRAM"]),
                            measured_at=measured_at,
                        )
                    )
            except Exception:
                print(f"Error running histogram query: {histogram_query}")
                print(traceback.format_exc())
                raise

        return metrics

    def scan_datetime_column(self, column_info: ColumnInfo) -> List[Metric]:
        """
        Scan a single column of Datetime type. This involves simple stats as well
        as the datetime histogram.

        Returns:
            List of Metrics for this column
        """

        (fq_table_name, column_name, data_type, data_type_simple, database) = column_info

        if data_type_simple != SimpleDataType.DATETIME:
            raise ValueError(
                f"{fq_table_name}.{column_name} ({data_type}) is {data_type_simple.value}, not datetime"
            )

        query_columns = self._simple_queries(column_info)

        ## Get the simple metrics first
        metrics = self._query_metrics(query_columns, column_info)

        if self.exclude_complex_queries: 
            return metrics

        ## Then the histogram
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
                            greatest(1.0, ceil(date_diff_years / 30.0)) as years_per_bin  // used only with bucket_unit = year
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
                                    // year(min) + years_per_bin * floor((year - min) / years_per_bin)
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
                        CURRENT_TIMESTAMP() as MEASURED_AT,
                        OBJECT_AGG(bucket, count) as NUMERIC_HISTOGRAM
                    FROM formatted_buckets;
                """

        try:
            with self.conn.cursor(DictCursor) as cur:
                cur.execute(
                    f"USE DATABASE {database};"
                )
                cur.execute(histogram_query)
                results = cur.fetchone()
                assert results is not None
                measured_at = results["MEASURED_AT"]
                metrics.append(
                    Metric(
                        run_id=self.run_id,
                        target_table=fq_table_name,
                        target_column=column_name,
                        metric_name="datetime_histogram",
                        metric_value=json.loads(results["NUMERIC_HISTOGRAM"]),
                        measured_at=measured_at,
                    )
                )
        except Exception:
            print(f"Error running datetime histogram query: {histogram_query}")
            print(traceback.format_exc())
            raise

        return metrics

    def scan_column(
        self,
        column_info: ColumnInfo,
    ) -> List[Metric]:
        """
        Scan a single column

        Args:
            run_id: Unique identifier for this snapshot run
            fq_table_name: database.schema.table
            column_name: Name of the column to scan
            data_type: Snowflake data type of the column
            is_nullable: 'YES' or 'NO', because Snowflake is like that

        Returns:
            List of Metrics for this column
        """

        match column_info.data_type_simple:
            case SimpleDataType.NUMERIC:
                return self.scan_numeric_column(column_info)
            case SimpleDataType.DATETIME:
                return self.scan_datetime_column(column_info)
            case _:
                ## every other case, we'll just do the simple queries
                query_columns = self._simple_queries(column_info)
                return self._query_metrics(query_columns, column_info)

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
            cur.execute(
                f"USE DATABASE {database.database};"
            )
            cur.execute(query)

            for column_name, data_type, is_nullable, table_schema, table_name in cur:
                fq_table_name = f"{database.database}.{table_schema}.{table_name}"
                column_info_arr.append(
                    ColumnInfo(
                        fq_table_name=fq_table_name,
                        column_name=column_name,
                        data_type=data_type,
                        data_type_simple=get_simple_data_type(data_type),
                        database=database.database
                    )
                )

        return column_info_arr

    def scan_table_level_metrics(self, database: Database) -> Tuple[List[Metric], List[str]]:
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
        q =  f"""
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
        print(q)
        with self.conn.cursor() as cur:
            try:
                cur.execute(
                    f"USE DATABASE {database.database};"
                )
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
