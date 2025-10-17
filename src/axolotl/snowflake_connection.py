import snowflake.connector
from typing import NamedTuple, List, Tuple, TypedDict, NotRequired, Dict, Any
from snowflake.connector import DictCursor
from .state_dao import Metric
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
import time
import math
import json

import traceback


class ColumnInfo(NamedTuple):
    fq_table_name: str
    column_name: str
    data_type: str
    is_nullable: str


class SnowflakeOptions(TypedDict):
    # Required fields
    user: str
    account: str
    database: str
    warehouse: str

    # Password authentication (mutually exclusive with private key auth)
    password: NotRequired[str]

    # Private key authentication (mutually exclusive with password)
    private_key: NotRequired[bytes]
    private_key_path: NotRequired[str]
    private_key_passphrase: NotRequired[str]

    # Optional additional fields that can be passed to snowflake.connector.connect()
    role: NotRequired[str]
    schema: NotRequired[str]
    authenticator: NotRequired[str]
    session_parameters: NotRequired[dict]
    timezone: NotRequired[str]
    autocommit: NotRequired[bool]
    client_session_keep_alive: NotRequired[bool]
    validate_default_parameters: NotRequired[bool]
    paramstyle: NotRequired[str]
    application: NotRequired[str]


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


## TODO: turn these into enums
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
    Wraps a Snowflake conn in order to take a snapshot of one database and
    schema.
    """

    def __init__(self, options: SnowflakeOptions, run_id: int):
        self.database = options["database"]
        ## note that we rename schema to table_schema here
        self.table_schema = options["schema"]
        self.run_id = run_id
        try:
            self.conn = snowflake.connector.connect(**options)
        except Exception as e:
            print(e)
            ## FIXME: don't print passwords here
            print(f"Failed to create snowflake connection: {options}")
            raise e

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
        # Return None to propagate any exceptions
        return None

    def snapshot(self, batch_size=10) -> Iterator[List[Metric]]:
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

    def _common_queries(
        self,
        column_info: ColumnInfo,
    ) -> dict[str, str]:
        col_sql = f'c."{column_info.column_name}"'
        return {
            "data_type": f"'{column_info.data_type}'",
            "row_count": "COUNT(*)",
            "null_count": f"COUNT_IF({col_sql} IS NULL)",
            "null_pct": f'100.0 * "null_count" / "row_count"',
            "distinct_count": f"COUNT(DISTINCT({col_sql}))",
            "distinct_rate": f'100.0 * "distinct_count" / ("row_count" - "null_count")',
        }

    def _package_metrics(
        self, column_info: ColumnInfo, query_results: Dict[str, Any]
    ) -> List[Metric]:
        """
        Takes the output of _query_values and packages them into metrics list
        """
        metrics: List[Metric] = []
        (fq_table_name, column_name, data_type, is_nullable) = column_info

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

    def _query_values(
        self, query_columns: dict[str, str], column_info: ColumnInfo
    ) -> Dict[str, Any]:
        """
        Use this instead of _query_metrics when you need to do something else to
        the results before packaging them.

        This packages the results into a dict[metric name, metric value] instaed
        of a list of metrics.
        """

        (fq_table_name, column_name, data_type, is_nullable) = column_info
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
                cur.execute(query)
                # return cur.query_id

                # query_ids.append(cur.sfqid)
                results = cur.fetchone()
                if not results:
                    raise ValueError('No results found')

            except Exception as e:
                print(e)
                print(query)
                raise e
            res = results
            # for metric_name, metric_value in results.items():
            #    res[metric_name] = metric_value

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

        (fq_table_name, column_name, data_type, is_nullable) = column_info
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
                cur.execute(query)
                # return cur.query_id

                # query_ids.append(cur.sfqid)
                results = cur.fetchone()
                assert results is not None
            except Exception as e:
                print(e)
                print(query)
                raise e

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

    def scan_text_or_bool_column(self, column_info: ColumnInfo) -> List[Metric]:
        """
        Scan a single column of Text or Boolean type. These are simple and
        hopefully should be efficient.

        Returns:
            List of Metrics for this column
        """
        (fq_table_name, column_name, data_type, is_nullable) = column_info
        data_type_simple = get_simple_data_type(data_type)

        if data_type_simple != "string" and data_type_simple != "boolean":
            raise TypeError(f"{fq_table_name}.{column_name} ({data_type}) is {data_type_simple}, not string or boolean")

        col_sql = f'c."{column_name}"'
        query_columns = self._common_queries(column_info)

        if data_type_simple == "string":
            query_columns.update(
                {
                    "string_avg_length": f"AVG(LEN({col_sql}))",
                }
            )
        elif data_type_simple == "boolean":
            query_columns.update(
                {
                    "true_count": f"COUNT_IF({col_sql} = TRUE)",
                    "false_count": f"COUNT_IF({col_sql} = FALSE)",
                }
            )
        return self._query_metrics(query_columns, column_info)

    def scan_numeric_column(self, column_info: ColumnInfo) -> List[Metric]:
        """
        Scan a single column of Numeric type. This involves simple stats as well
        as histograms and percentiles.

        Returns:
            List of Metrics for this column
        """

        (fq_table_name, column_name, data_type, is_nullable) = column_info
        data_type_simple = get_simple_data_type(data_type)

        if data_type_simple != "numeric":
            # traceback.print_exc()
            raise ValueError(
                f"{fq_table_name}.{column_name} ({data_type}) is {data_type_simple}, not numeric"
            )

        col_sql = f'c."{column_name}"'
        query_columns = self._common_queries(column_info)

        query_columns.update(
            {
                "numeric_min": f"MIN({col_sql})",
                "numeric_max": f"MAX({col_sql})",
                "numeric_mean": f"AVG({col_sql})",
                # cast to float to avoid precision overflow errors
                "numeric_stddev": f"STDDEV({col_sql}::float)",
            }
        )

        ## Get the simple metrics first
        query_values = self._query_values(query_columns, column_info)
        metrics = self._package_metrics(column_info, query_values)

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
        except Exception as e:
            print(f"Error running percentile query: {e}, {percentile_query}")
            raise e

        if query_values["numeric_max"] == query_values["numeric_min"]:
            ## If all values are the same, just put them in one bucket.
            metrics.append(
                Metric(
                    run_id=self.run_id,
                    target_table=fq_table_name,
                    target_column=column_name,
                    metric_name="numeric_histogram",
                    metric_value={
                        query_values["numeric_max"]: query_values["row_count"] - query_values["null_count"],
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
            except Exception as e:
                print(f"Error running histogram query: {e}, {histogram_query}")
                raise e

        return metrics

    def scan_datetime_column(self, column_info: ColumnInfo) -> List[Metric]:
        """
        Scan a single column of Datetime type. This involves simple stats as well
        as the datetime histogram.

        Returns:
            List of Metrics for this column
        """

        (fq_table_name, column_name, data_type, is_nullable) = column_info
        data_type_simple = get_simple_data_type(data_type)

        if data_type_simple != "datetime":
            raise ValueError(
                f"{fq_table_name}.{column_name} ({data_type}) is {data_type_simple}, not datetime"
            )

        col_sql = f'c."{column_name}"'
        query_columns = self._common_queries(column_info)

        query_columns.update(
            {
                "numeric_min": f"MIN({col_sql})",
                "numeric_max": f"MAX({col_sql})",
            }
        )

        ## Get the simple metrics first
        metrics = self._query_metrics(query_columns, column_info)

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
        except Exception as e:
            print(f"Error running datetime histogram query: {e}, {histogram_query}")
            raise e

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

        match data_type_simple:
            case "numeric":
                return self.scan_numeric_column(column_info)
            case "datetime":
                return self.scan_datetime_column(column_info)
            case "string" | "boolean":
                return self.scan_text_or_bool_column(column_info)
            case _:
                ## unknown case, we'll just do the simple queries
                query_columns = self._common_queries(column_info)
                return self._query_metrics(query_columns, column_info)

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
        table_names = []
        fq_table_names = []
        g_measured_at = None

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
                        CREATED,
                        LAST_ALTERED,
                        CURRENT_TIMESTAMP() as measured_at
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_CATALOG = '{self.database}'
                    AND TABLE_SCHEMA = '{self.table_schema}';
                """
                )

            except Exception as e:
                print(f"Error running table: {e}")
                raise e

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
                assert results is not None
            except Exception as e:
                print(f"Error getting table update times: {e}")
                raise e

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
