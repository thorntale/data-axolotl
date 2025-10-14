import snowflake.connector
from typing import NamedTuple, List, Tuple
from snowflake.connector import DictCursor
from .state_dao import Metric
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
import time


## TODO: percentiles and histograms
##        complex_queries = {
##            "numeric_percentiles": f"""
##                        WITH percentile_state AS (
##                            SELECT
##                                APPROX_PERCENTILE_ACCUMULATE({column_name}) AS {column_name}_STATE
##                            FROM {fq_table_name}
##                        )
##                        SELECT OBJECT_CONSTRUCT(
##                            '10p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.10),
##                            '20p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.20),
##                            '30p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.30),
##                            '40p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.40),
##                            '50p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.50),
##                            '60p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.60),
##                            '70p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.70),
##                            '80p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.80),
##                            '90p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.90),
##                            '95p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.95),
##                            '99p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.99)
##                        ) as numeric_percentiles
##                        FROM percentile_state AS s;
##                    """,
##        }
##


class ColumnInfo(NamedTuple):
    fq_table_name: str
    column_name: str
    data_type: str
    is_nullable: str


class SnowflakeOptions(NamedTuple):
    user: str
    password: str
    account: str
    database: str
    warehouse: str
    table_schema: str


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
    "DOUBLE PRECISION" "REAL",
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


def get_simple_data_type(data_type: str) -> str:
    data_type = data_type.upper()
    if data_type == "BOOLEAN":
        return "boolean"
    if data_type in SNOWFLAKE_NUMERIC_TYPES:
        return "numeric"
    if data_type in SNOWFLAKE_TEXT_TYPES:
        return "string"
    if data_type in SNOWFLAKE_DATETIME_TYPES:
        return "datetime"
    if data_type in SNOWFLAKE_STRUCTURED_TYPES:
        return "structured"
    if data_type in SNOWFLAKE_UNSTRUCTURED_TYPES:
        return "unstructured"
    if data_type in SNOWFLAKE_VECTOR_TYPES:
        return "vector"
    else:
        return "other"


class SnowflakeConn:
    """
    Wraps a Snowflake conn in order to take a snapshot of one database and table_schema.
    """

    def __init__(self, options: SnowflakeOptions, run_id: int):
        self.conn = snowflake.connector.connect(
            **options._asdict(),
        )
        self.database = options.database
        self.table_schema = options.table_schema
        self.run_id = run_id

    def snapshot(self, batch_size=20) -> Iterator[List[Metric]]:
        """
        Capture metrics for all tables in the configured database/schema, both
        table-level and column-level for all tables

        Args:
            run_id: Unique identifier for this snapshot run

        Returns:
            List of Metric objects containing all collected metrics
        """

        t0 = time.time()
        (metrics, table_names) = self.scan_table_level_metrics()
        t1 = time.time()  ## Time it took to scan the table metrics

        print(
            f"Found {len(table_names)} tables in {round(t1-t0, 2)} seconds. Enumerating columns..."
        )

        yield metrics

        all_column_infos = [
            column_info
            for table in table_names
            for column_info in self.list_columns(table)
        ]

        print(f"Snapshotting {len(all_column_infos)} columns...")

        executor = ThreadPoolExecutor(max_workers=batch_size)
        try:
            for column_metrics in executor.map(
                self.scan_column, all_column_infos, chunksize=1
            ):
                yield column_metrics
        finally:
            # wait for all queries to complete before returning
            executor.shutdown(wait=True)
            t2 = time.time()  ## Time at which we finished scanning all the columns

            print(f"Snapshotted columns in {round(t2-t1, 2)} seconds")

    def scan_numeric_column(
        self,
        column_info: ColumnInfo,
    ) -> List[Metric]:
        """
        Get histogram and percentile metrics, if this column is a numeric or datetime type.
        """

        (fq_table_name, column_name, data_type, is_nullable) = column_info
        data_type_simple = get_simple_data_type(data_type)

        if data_type_simple != "numeric":
            print(f"not numeric: {fq_table_name}.{column_name}: {data_type_simple}")
            return []

        num_buckets = 10
        percentile_query = f"""
                    WITH percentile_state AS (
                        SELECT
                            APPROX_PERCENTILE_ACCUMULATE({column_name}) AS {column_name}_STATE
                        FROM {fq_table_name}
                    )
                    SELECT CURRENT_TIMESTAMP() as MEASURED_AT,
                    OBJECT_CONSTRUCT(
                        '10p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.10),
                        '20p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.20),
                        '30p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.30),
                        '40p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.40),
                        '50p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.50),
                        '60p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.60),
                        '70p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.70),
                        '80p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.80),
                        '90p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.90),
                        '95p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.95),
                        '99p', APPROX_PERCENTILE_ESTIMATE(s.{column_name}_STATE, 0.99)
                    ) as NUMERIC_PERCENTILES
                    FROM percentile_state AS s;
                """

        histogram_query = f"""
                    WITH cte AS (
                        SELECT
                            MIN({column_name}) AS {column_name}_MIN,
                            MAX({column_name}) AS {column_name}_MAX,
                            LEAST({column_name}_MAX - {column_name}_MIN, {num_buckets}) AS NUM_BUCKETS,
                            ({column_name}_MAX - {column_name}_MIN) / {num_buckets} AS BUCKET_SIZE
                        FROM {fq_table_name}
                    ),
                    histogram_buckets AS (
                        SELECT
                            WIDTH_BUCKET({column_name},
                                (SELECT {column_name}_MIN FROM cte),
                                (SELECT {column_name}_MAX FROM cte),
                                (SELECT NUM_BUCKETS FROM cte)
                            ) * (SELECT BUCKET_SIZE FROM cte) as bucket,
                            COUNT(*) as count
                        FROM {fq_table_name}
                        GROUP BY bucket
                    )
                    SELECT
                        CURRENT_TIMESTAMP() as MEASURED_AT,
                        OBJECT_AGG(bucket::VARCHAR, count) as NUMERIC_HISTOGRAM
                    FROM histogram_buckets;
                """

        metrics = []

        try:
            ## TODO: is it more efficient to merge these into one join instead of repeated scans?
            with self.conn.cursor(DictCursor) as cur:
                cur.execute(percentile_query)
                results = cur.fetchone()
                measured_at = results["MEASURED_AT"]
                metrics.append(
                    Metric(
                        run_id=self.run_id,
                        target_table=fq_table_name,
                        target_column=column_name,
                        metric_name="numeric_percentiles",
                        metric_value=results["NUMERIC_PERCENTILES"],
                        measured_at=measured_at,
                    )
                )
        except Exception as e:
            print(f"Error running percentile query: {e}, {percentile_query}")
            raise

        try:
            with self.conn.cursor(DictCursor) as cur:
                cur.execute(histogram_query)
                results = cur.fetchone()
                measured_at = results["MEASURED_AT"]
                metrics.append(
                    Metric(
                        run_id=self.run_id,
                        target_table=fq_table_name,
                        target_column=column_name,
                        metric_name="numeric_histogram",
                        metric_value=results["NUMERIC_HISTOGRAM"],
                        measured_at=measured_at,
                    )
                )
        except Exception as e:
            print(f"Error running histogram query: {e}, {histogram_query}")
            raise

        return metrics

    ## Hstograms and percentiles

    def scan_datetime_column(
        self,
        column_info: ColumnInfo,
    ) -> List[Metric]:
        """
        Get histogram and percentile metrics, if this column is a numeric or datetime type.
        """

        (fq_table_name, column_name, data_type, is_nullable) = column_info
        data_type_simple = get_simple_data_type(data_type)

        if data_type_simple != "datetime":
            print(f"not datetime: {fq_table_name}.{column_name}: {data_type_simple}")
            return []
        print(f"datetime: {fq_table_name}.{column_name}: {data_type_simple}")
        num_buckets = 10

        histogram_query = f"""
                    WITH date_range AS (
                        SELECT
                            MIN({column_name}) AS {column_name}_MIN,
                            MAX({column_name}) AS {column_name}_MAX,
                            DATEDIFF(day, {column_name}_MIN, {column_name}_MAX) AS DATE_DIFF_DAYS,
                            DATEDIFF(hour, {column_name}_MIN, {column_name}_MAX) AS DATE_DIFF_HOURS,
                            DATEDIFF(month, {column_name}_MIN, {column_name}_MAX) AS DATE_DIFF_MONTHS,
                            DATEDIFF(year, {column_name}_MIN, {column_name}_MAX) AS DATE_DIFF_YEARS
                        FROM {fq_table_name}
                    ),
                    bucket_config AS (
                        SELECT
                            dr.*,
                            CASE
                                -- Less than 1 day: bucket by hours (up to 24 buckets)
                                WHEN dr.DATE_DIFF_DAYS < 1 THEN 'hour'
                                -- Less than 31 days: bucket by days (up to 31 buckets)
                                WHEN dr.DATE_DIFF_DAYS < 31 THEN 'day'
                                -- Less than 1 year: bucket by months (up to 12 buckets)
                                WHEN dr.DATE_DIFF_DAYS < 365 THEN 'month'
                                -- Greater than 1 year: bucket by years (max 10 buckets)
                                ELSE 'year'
                            END AS BUCKET_UNIT,
                            CASE
                                WHEN dr.DATE_DIFF_DAYS < 1 THEN LEAST(dr.DATE_DIFF_HOURS, 24)
                                WHEN dr.DATE_DIFF_DAYS < 31 THEN LEAST(dr.DATE_DIFF_DAYS, 31)
                                WHEN dr.DATE_DIFF_DAYS < 365 THEN LEAST(dr.DATE_DIFF_MONTHS, 12)
                                ELSE LEAST(dr.DATE_DIFF_YEARS, 10)
                            END AS NUM_BUCKETS
                        FROM date_range dr
                    ),
                    histogram_buckets AS (
                        SELECT
                            CASE bc.BUCKET_UNIT
                                WHEN 'hour' THEN DATE_TRUNC('hour', {column_name})
                                WHEN 'day' THEN DATE_TRUNC('day', {column_name})
                                WHEN 'month' THEN DATE_TRUNC('month', {column_name})
                                WHEN 'year' THEN
                                    CASE
                                        -- If more than 10 years, compress into 10 buckets
                                        WHEN bc.DATE_DIFF_YEARS > 10 THEN
                                            DATEADD('year',
                                                FLOOR(DATEDIFF('year', bc.{column_name}_MIN, {column_name}) /
                                                    CEIL(bc.DATE_DIFF_YEARS / 10.0)) *
                                                CEIL(bc.DATE_DIFF_YEARS / 10.0),
                                                bc.{column_name}_MIN)
                                        ELSE DATE_TRUNC('year', {column_name})
                                    END
                            END AS bucket,
                            COUNT(*) as count
                        FROM {fq_table_name}
                        CROSS JOIN bucket_config bc
                        GROUP BY bucket, bc.BUCKET_UNIT, bc.DATE_DIFF_YEARS, bc.{column_name}_MIN
                    )
                    SELECT
                        CURRENT_TIMESTAMP() as MEASURED_AT,
                        OBJECT_AGG(bucket::VARCHAR, count) as NUMERIC_HISTOGRAM
                    FROM histogram_buckets;
                """
        print(histogram_query)

        metrics = []
        try:
            with self.conn.cursor(DictCursor) as cur:
                cur.execute(histogram_query)
                results = cur.fetchone()
                measured_at = results["MEASURED_AT"]
                metrics.append(
                    Metric(
                        run_id=self.run_id,
                        target_table=fq_table_name,
                        target_column=column_name,
                        metric_name="numeric_histogram",
                        metric_value=results["NUMERIC_HISTOGRAM"],
                        measured_at=measured_at,
                    )
                )
        except Exception as e:
            print(f"Error running histogram query: {e}, {histogram_query}")
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
        (fq_table_name, column_name, data_type, is_nullable) = column_info

        data_type_simple = get_simple_data_type(data_type)
        # print(f"scanning {fq_table_name}.{column_name}: {data_type_simple}")

        # metric name -> metric query
        ## TODO: if table size is too large, use approx_count_distinct instead

        query_columns = {
            "distinct_rate": f"100.0 * COUNT(DISTINCT({column_name})) / COUNT(*)",
        }

        if is_nullable == "YES":
            query_columns.update(
                {
                    "null_count": f"COUNT_IF({column_name} IS NULL)",
                    "null_pct": f"100.0 * COUNT_IF({column_name} IS NOT NULL) / COUNT(*)",
                }
            )

        if data_type_simple == "numeric" or data_type_simple == "datetime":
            query_columns.update(
                {
                    "numeric_min": f"MIN({column_name})",
                    "numeric_max": f"MAX({column_name})",
                    "numeric_mean": f"AVG({column_name})",
                    "numeric_mean": f"VARIANCE({column_name})",
                }
            )

        if data_type_simple == "string":
            query_columns.update(
                {
                    "string_avg_length": f"AVG(LEN({column_name}))",
                }
            )
        if data_type_simple == "boolean":
            query_columns.update(
                {
                    "true_count": "COUNT_IF({column_name} = TRUE)",
                    "false_count": "COUNT_IF({column_name} = FALSE)",
                }
            )
        ## TODO: Stats for the other types

        query = f"""
            SELECT CURRENT_TIMESTAMP() as MEASURED_AT,
            {",\n".join([f"{metric_query} AS {metric_name}" for (metric_name, metric_query) in query_columns.items()])} 
            FROM {fq_table_name}
        """

        metrics: List[Metric] = []
        # query_ids = []

        print(f"scanning {fq_table_name}.{column_name}: {data_type_simple}")
        if data_type_simple == "datetime":
            metrics.extend(self.scan_datetime_column(column_info))
        elif data_type_simple == "numeric":
            metrics.extend(self.scan_numeric_column(column_info))

        with self.conn.cursor(DictCursor) as cur:
            cur.execute(query)
            # return cur.query_id

            # query_ids.append(cur.sfqid)
            results = cur.fetchone()
            measured_at = results["MEASURED_AT"]  # snowflake uppercases column names.
            for metric_name in query_columns:
                metrics.append(
                    Metric(
                        run_id=self.run_id,
                        target_table=fq_table_name,
                        target_column=column_name,
                        metric_name=metric_name,
                        metric_value=results[metric_name.upper()],
                        measured_at=measured_at,
                    )
                )

        return metrics
        # return query_id

    def list_columns(self, table_name: str) -> List[ColumnInfo]:
        """
        List all columns in a table.

        Args:
            table_name: Name of the table to list columns for

        Returns:
            ColumnInfo: all the information to scan the column
        """
        column_info_arr: List[ColumnInfo] = []

        fq_table_name = f"{self.database}.{self.table_schema}.{table_name}"
        with self.conn.cursor() as cur:
            query = f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_CATALOG = '{self.database}'
                    AND TABLE_SCHEMA = '{self.table_schema}'
                    AND TABLE_NAME = '{table_name}'
                """
            cur.execute(query)

            for column_name, data_type, is_nullable in cur:
                column_info_arr.append(
                    ColumnInfo(
                        fq_table_name=fq_table_name,
                        column_name=column_name,
                        data_type=data_type,
                        is_nullable=is_nullable,
                    )
                )

        return column_info_arr

    def scan_table_level_metrics(self) -> Tuple[List[Metric], List[str]]:
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
        table_names = []  ## fully qualified table names

        with self.conn.cursor() as cur:
            try:
                cur.execute(
                    f"""
                    SELECT
                        TABLE_CATALOG, 
                        TABLE_SCHEMA, 
                        TABLE_NAME, 
                        ROW_COUNT, 
                        BYTES, 
                        LAST_ALTERED, 
                        CURRENT_TIMESTAMP() as measured_at
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_CATALOG = '{self.database}'
                AND TABLE_SCHEMA = '{self.table_schema}';
                    """
                )

            except Exception as e:
                print(f"Error running table: {e}")
                raise

            for (
                table_catalog,
                table_schema,
                table_name,
                row_count,
                table_bytes,
                last_altered,
                measured_at,
            ) in cur:
                table_names.append(table_name)
                fq_table_name = f"{table_catalog}.{table_schema}.{table_name}"

                try:
                    metrics.extend(
                        [
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
                        ]
                    )
                except Exception as e:
                    print(f"Error: {e}")
                    raise

        return metrics, table_names
