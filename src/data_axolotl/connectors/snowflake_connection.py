import time
import json
import re
import traceback
import pdb
import pandas
from snowflake.connector.pandas_tools import write_pandas

from collections.abc import Iterator
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
    override,
)

import snowflake.connector
from snowflake.connector import DictCursor
from rich.console import Console

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..config import DataAxolotlConfig, SnowflakeConnectionConfig
from ..connectors.state_dao import Metric
from .identifiers import FqTable
from ..live_run_console import LiveConsole
from ..metric_query import QueryStatus, MetricQuery
from ..snowflake_utils import (
    extract_histogram,
    extract_percentiles,
    extract_simple_metrics,
    extract_datetime_histogram,
    get_simple_data_type,
    query_priority,
)

from ..database_types import SimpleDataType, ColumnInfo
from ..timeouts import Timeout
from .base_connection import BaseConnection
from .identifiers import IncludeDirective
from .parallel_query_executor import ParallelQueryExecutor


class SnowflakeConn(BaseConnection):
    """
    Wraps a Snowflake conn in order to take a snapshot of a list of databases
    and schemas.
    """

    def __init__(
        self,
        data_axolotl_config: DataAxolotlConfig,
        connection_config: SnowflakeConnectionConfig,
        run_id: int,
        console: Console | LiveConsole = Console(),
    ):
        """
        Initialize a Snowflake connection.

        Args:
            config: DataAxolotlConfig object containing all configuration
            connection_name: Name of the connection to use from config.connections
            run_id: Unique identifier for this snapshot run
        """
        super().__init__(
            data_axolotl_config,
            connection_config,
            run_id,
            console,
        )

        self.max_threads = connection_config.max_threads or data_axolotl_config.max_threads
        self.conn_timeout = Timeout(
            timeout_seconds=connection_config.connection_timeout_seconds
            or data_axolotl_config.connection_timeout_seconds,
            detail=f"connection:{connection_config.name}",
        )
        self.per_query_timeout_seconds = (
            connection_config.query_timeout_seconds or data_axolotl_config.query_timeout_seconds
        )
        self.exclude_expensive_queries = (
            connection_config.exclude_expensive_queries
            if connection_config.exclude_expensive_queries is not None
            else data_axolotl_config.exclude_expensive_queries
        )
        self.exclude_complex_queries = (
            connection_config.exclude_complex_queries
            if connection_config.exclude_complex_queries is not None
            else data_axolotl_config.exclude_complex_queries
        )

    @override
    @staticmethod
    def get_conn(
        console: Console | LiveConsole,
        params: Dict[str, Any],
    ):
        try:
            return snowflake.connector.connect(
                **params,
                paramstyle='qmark',
            )
        except Exception:
            # TODO: Don't print sensitive connection params
            console.print(
                f"Failed to create snowflake connection: {params.get('connection_name')}"
            )
            traceback.format_exc()
            raise

    @override
    def snapshot(self, run_timeout: Timeout) -> Iterator[Metric]:
        """
        Capture metrics for all tables in the configured schemas, both
        table-level and column-level for all tables

        Args:
            run_timeout: Timeout object for the overall run

        Returns:
            List of Metric objects containing all collected metrics
        """
        self.conn_timeout.start()
        yield from self._snapshot_threaded(run_timeout)
        duration = self.conn_timeout.stop()

        self.console.print(f"Finished ({self.connection_config.name}) in {round(duration, 2)} seconds")

    @override
    def list_only(self, run_timeout: Timeout) -> Iterator[str]:
        all_metric_queries, table_metrics = self._scan_for_queries()
        for m in table_metrics:
            yield f"{m.target_table} : {m.metric_name}"
        for q in all_metric_queries:
            for metric_name in q.metrics:
                yield f"{q.fq_table_name}.{q.column_name} : {metric_name}"

    @override
    def state_query(self, query_string: str, data: List[Any] = []) -> List[List[Any]]:
        assert self.conn
        with self.conn.cursor() as cur:
            result = cur.execute(query_string, data)
            return result.fetchall()
            # print(f"{query_string=}, {data=}")
            # l = []
            # for r in result:
            #     print(f"{r!r}")
            #     l += [r]
            # return l

    @override
    def state_bulk_upload(self, table: str, cols: List[str], data: List[List[Any]]):
        assert self.conn

        parsed = FqTable.from_string(table)
        # write_pandas needs a correctly-cased table name, which we may not have
        with self.conn.cursor() as cur:
            cur.execute(f"""
                select table_catalog, table_schema, table_name
                from {FqTable.escape(parsed.database)}.information_schema.tables
                where
                    lower(table_schema) = lower(?)
                    and lower(table_name) = lower(?)
            """, (parsed.schema, parsed.table))
            fq_table = FqTable(*cur.fetchone())

        self.conn.cursor().execute(f"use database {FqTable.escape(fq_table.database)}")
        self.conn.cursor().execute(f"use schema {FqTable.escape(fq_table.schema)}")
        df = pandas.DataFrame(data, columns=cols)
        success, num_chunks, num_rows, output = write_pandas(
            conn=self.conn,
            df=df,
            database=fq_table.database,
            schema=fq_table.schema,
            table_name=fq_table.table,
            # use_logical_type=True,
        )
        assert success

    @override
    def escape_state_table(self, prefix: str, table: str) -> str:
        # use IncludeDirective as a shortcut for parsing prefix
        if not prefix:
            raise ValueError('A prefix of the form `database.schema` is required when using snowflake to store state.')
        try:
            parsed = IncludeDirective.from_string(prefix)
            if (
                not parsed.database
                or not parsed.schema
                or parsed.table
                or parsed.column
                or parsed.metric
            ):
                raise ValueError('')
        except Exception:
            raise ValueError(f'Invalid prefix `{prefix}`. Expected prefix of the format database.schema')

        return str(FqTable(
            parsed.database,
            parsed.schema,
            table,
        ))

    def _scan_for_queries(self) -> Tuple[List[MetricQuery], List[Metric]]:
        # scan the db and generate metric queries & table metrics
        t_start = time.monotonic()

        databases = self._list_included_databases()

        table_metrics, table_names = self._scan_tables(databases)
        t_tablescan = time.monotonic()  ## Time it took to scan the table metrics
        self.console.print(
            f"Found {len(table_names)} tables in {round(t_tablescan - t_start, 2)} seconds. Enumerating columns..."
        )

        all_column_infos: List[ColumnInfo] = self._list_columns(databases)

        print(f"Generating queries for {len(all_column_infos)} columns...")
        all_metric_queries: List[MetricQuery] = []
        for c_info in all_column_infos:
            all_metric_queries.extend(self._generate_column_queries(c_info))

        return all_metric_queries, table_metrics

    def _cancel_queries(self, query_ids: List[str]):
        assert self.conn
        with self.conn.cursor() as cur:
            for query_id in query_ids:
                try:
                    cur.execute(f"SELECT SYSTEM$CANCEL_QUERY('{query_id}')")
                except Exception:
                    self.console.print(f"Error canceling query: {query_id}")
                    traceback.format_exc()

    def _snapshot_threaded(
        self,
        run_timeout: Timeout,
    ) -> Iterator[Metric]:
        assert self.conn
        if run_timeout.is_timed_out():
            raise TimeoutError("run already timed out")

        all_metric_queries, table_metrics = self._scan_for_queries()
        yield from table_metrics

        self.console.print(f"Snapshotting using {len(all_metric_queries)} queries...")

        class SfExecutor(ParallelQueryExecutor):
            max_workers = self.max_threads
            console = self.console

            @staticmethod
            def begin(metric_query: MetricQuery) -> str:
                assert self.conn
                with self.conn.cursor() as cur:
                    cur.execute_async(metric_query.query)
                    assert cur.sfqid
                    return cur.sfqid

            @staticmethod
            def resolve(metric_query: MetricQuery, qid: str, timeout: Timeout) -> List[Metric]:
                assert self.conn
                while self.conn.is_still_running(self.conn.get_query_status(qid)):
                    time.sleep(0.5)
                    if timeout.is_timed_out():
                        self._cancel_queries([qid])
                        self.console.print(
                            f"Timed out: ({qid}): {metric_query.fq_table_name}.{metric_query.column_name} ({metric_query.query_detail}) at {metric_query.timeout_at} (current time {time.monotonic()})"
                        )
                        raise TimeoutError(
                            f"Timed out: {metric_query.fq_table_name}.{metric_query.column_name} ({metric_query.query_detail})"
                        )

                with self.conn.cursor(DictCursor) as cur:
                    cur.get_results_from_sfqid(qid)
                    results = cur.fetchone()
                    assert results

                    extracted_metrics = metric_query.result_extractor(
                        self.run_id,
                        metric_query.fq_table_name,
                        metric_query.column_name,
                        results,
                    )
                    return extracted_metrics

        yield from SfExecutor(all_metric_queries).run(
            total_timeout=run_timeout,
            per_query_timeout_seconds=self.per_query_timeout_seconds,
        )

    def _simple_queries(
        self,
        column_info: ColumnInfo,
    ) -> Dict[str, str]:
        """
        The expensive queries are distinct_count (and therefore distinct_rate), and string_avg_length
        """

        data_type_simple = column_info.data_type_simple
        col_sql = f'c."{column_info.column_name}"'
        queries = {
            "data_type": f"'{column_info.data_type}'",
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

        return {
            metric: query
            for metric, query in queries.items()
            if self.connection_config.include_column_metric(
                column_info.table, column_info.column_name, metric
            )
        }

    def _generate_column_queries(self, column_info: ColumnInfo) -> List[MetricQuery]:
        """
        Generate a list of MetricQuery objects for a given column based on its type.
        This function does not execute queries, only creates the query objects in QUEUED state.

        Args:
            column_info: Information about the column to generate queries for

        Returns:
            List of MetricQuery objects in QUEUED state
        """
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
            FROM {column_info.table} as c
        """

        queries.append(
            MetricQuery(
                query=simple_query,
                query_id="",
                status=QueryStatus.QUEUED,
                timeout_at=0.0,
                fq_table_name=column_info.table,
                column_name=column_info.column_name,
                metrics=list(query_columns.keys()),
                query_detail="simple_metrics",
                result_extractor=extract_simple_metrics,
            )
        )

        # 2. Generate complex queries based on data type (if not excluded)
        if not self.exclude_complex_queries:
            if column_info.data_type_simple == SimpleDataType.NUMERIC:
                # Percentiles query
                percentile_query = f"""
                    WITH percentile_state AS (
                        SELECT
                            APPROX_PERCENTILE_ACCUMULATE("{column_info.column_name}") AS col_state
                        FROM {column_info.table}
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
                queries.append(
                    MetricQuery(
                        query=percentile_query,
                        query_id="",
                        status=QueryStatus.QUEUED,
                        timeout_at=0.0,
                        fq_table_name=column_info.table,
                        column_name=column_info.column_name,
                        metrics=["numeric_percentiles"],
                        query_detail="numeric_percentiles",
                        result_extractor=extract_percentiles,
                    )
                )

                # Histogram query - handles both min==max and min!=max cases in one query
                num_buckets = 10
                histogram_query = f"""
                    WITH minmax AS (
                        SELECT
                            MIN(t."{column_info.column_name}") AS COL_MIN,
                            MAX(t."{column_info.column_name}") AS COL_MAX,
                            COUNT(*) AS ROW_COUNT,
                            COUNT_IF(t."{column_info.column_name}" IS NULL) AS NULL_COUNT
                        FROM {column_info.table} as t
                    ),
                    histogram_buckets AS (
                        SELECT
                            LEAST(GREATEST(WIDTH_BUCKET(t."{column_info.column_name}",
                                (SELECT COL_MIN FROM minmax),
                                (SELECT COL_MAX FROM minmax),
                                {num_buckets}
                            ), 1), {num_buckets}) as BUCKET_I,
                            COUNT(*) as COUNT
                        FROM {column_info.table} AS t
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

                queries.append(
                    MetricQuery(
                        query=histogram_query,
                        query_id="",
                        status=QueryStatus.QUEUED,
                        timeout_at=0.0,
                        fq_table_name=column_info.table,
                        column_name=column_info.column_name,
                        metrics=["numeric_histogram"],
                        query_detail="numeric_histogram",
                        result_extractor=extract_histogram,
                    )
                )

            elif column_info.data_type_simple == SimpleDataType.DATETIME:
                # Datetime histogram query
                histogram_query = f"""
                    with bucket_config as (
                        select
                            min(t."{column_info.column_name}")
                                as col_min,
                            max(t."{column_info.column_name}")
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
                        from {column_info.table} as t
                    ),
                    histogram_buckets AS (
                        SELECT
                            CASE bc.BUCKET_UNIT
                                WHEN 'hour' THEN
                                    DATE_TRUNC('hour', t."{column_info.column_name}")
                                WHEN 'day' THEN
                                    DATE_TRUNC('day', t."{column_info.column_name}")
                                WHEN 'month' THEN
                                    DATE_TRUNC('month', t."{column_info.column_name}")
                                WHEN 'year' THEN
                                    DATEADD('year',
                                        bc.years_per_bin * floor((year(t."{column_info.column_name}") - year(bc.col_min)) / bc.years_per_bin),
                                        DATE_TRUNC('year', bc.col_min)
                                    )
                            END AS bucket_timestamp,
                            bc.BUCKET_UNIT,
                            COUNT(*) as count
                        FROM {column_info.table} as t
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
                queries.append(
                    MetricQuery(
                        query=histogram_query,
                        query_id="",
                        status=QueryStatus.QUEUED,
                        timeout_at=0.0,
                        fq_table_name=column_info.table,
                        column_name=column_info.column_name,
                        metrics=["datetime_histogram"],
                        query_detail="datetime_histogram",
                        result_extractor=extract_datetime_histogram,
                    )
                )

        return queries

    def _list_columns(self, databases: List[str]) -> List[ColumnInfo]:
        """
        List all columns, respecting included and excluded schemas

        Args:
        Returns:
            ColumnInfo[]: all the information to scan the column
        """
        column_info_arr: List[ColumnInfo] = []

        assert self.conn
        with self.conn.cursor() as cur:
            cur.execute(
                " union all ".join(
                    f"""
                    select
                        table_catalog,
                        column_name,
                        data_type,
                        is_nullable,
                        table_schema,
                        table_name
                    from "{database}".information_schema.columns
                """
                    for database in databases
                )
            )

            for (
                table_catalog,
                column_name,
                data_type,
                is_nullable,
                table_schema,
                table_name,
            ) in cur:
                fq_table = FqTable(table_catalog, table_schema, table_name)
                if self.connection_config.include_column(fq_table, column_name):
                    column_info_arr.append(
                        ColumnInfo(
                            table=fq_table,
                            column_name=column_name,
                            data_type=data_type,
                            data_type_simple=get_simple_data_type(data_type),
                        )
                    )

        return column_info_arr

    def _list_included_databases(self) -> List[str]:
        assert self.conn
        with self.conn.cursor(DictCursor) as cur:
            cur.execute("show databases")
            all_databases = [row["name"] for row in cur.fetchall()]

        return [
            database
            for database in all_databases
            if self.connection_config.database_is_included_at_all(database)
        ]

    def _scan_tables(self, databases: List[str]) -> Tuple[List[Metric], List[FqTable]]:
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
        fq_table_names: List[FqTable] = []

        assert self.conn
        for database in databases:
            with self.conn.cursor(DictCursor) as cur:
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
                        FROM "{database}".INFORMATION_SCHEMA.TABLES
                    """
                    )

                except Exception:
                    self.console.print("Error running table scan query")
                    traceback.format_exc()
                    raise

                for row in cur:
                    fq_table_name = FqTable(
                        row["TABLE_CATALOG"],
                        row["TABLE_SCHEMA"],
                        row["TABLE_NAME"],
                    )
                    measured_at = row["MEASURED_AT"]

                    if self.connection_config.include_table_at_all(fq_table_name):
                        fq_table_names.append(fq_table_name)
                    else:
                        continue

                    if not self.connection_config.include_table_metrics(fq_table_name):
                        continue

                    metrics += [
                        Metric(
                            run_id=self.run_id,
                            target_table=fq_table_name,
                            target_column=None,
                            metric_name="row_count",
                            metric_value=row["ROW_COUNT"],
                            measured_at=measured_at,
                        ),
                        Metric(
                            run_id=self.run_id,
                            target_table=fq_table_name,
                            target_column=None,
                            metric_name="bytes",
                            metric_value=row["BYTES"],
                            measured_at=measured_at,
                        ),
                        Metric(
                            run_id=self.run_id,
                            target_table=fq_table_name,
                            target_column=None,
                            metric_name="created_at",
                            metric_value=row["CREATED"],
                            measured_at=measured_at,
                        ),
                        Metric(
                            run_id=self.run_id,
                            target_table=fq_table_name,
                            target_column=None,
                            metric_name="altered_at",
                            metric_value=row["LAST_ALTERED"],
                            measured_at=measured_at,
                        ),
                    ]

        with self.conn.cursor() as cur:
            try:
                tables = [
                    t
                    for t in fq_table_names
                    if self.connection_config.include_table_metrics(fq_table_name)
                ]
                if not tables:
                    results = []
                else:
                    cur.execute(
                        f"""
                        select {','.join(
                            f"TO_TIMESTAMP_TZ(SYSTEM$LAST_CHANGE_COMMIT_TIME('{table}') / 1e9)"
                            for table in tables
                        )}
                    """
                    )
                    results = cur.fetchone()
                    assert results
            except Exception:
                self.console.print("Error getting table update times")
                #self.console.print(traceback.format_exc())
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
                for col, table_name in zip(results, tables)
            ]

        return metrics, fq_table_names
