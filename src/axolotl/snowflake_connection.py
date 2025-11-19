import time
import json
import re
import traceback
import pdb

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed, Future
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

import snowflake.connector
from snowflake.connector import DictCursor
from rich.console import Console

from .config import AxolotlConfig, SnowflakeConnectionConfig
from .state_dao import Metric, FqTable
from .live_run_console import LiveConsole
from .metric_query import QueryStatus, MetricQuery
from .utils import is_timed_out
from .snowflake_utils import (
    extract_histogram,
    extract_percentiles,
    extract_simple_metrics,
    extract_datetime_histogram,
    get_simple_data_type,
    query_priority,
)

from .database_types import SimpleDataType, ColumnInfo


class SnowflakeConn:
    """
    Wraps a Snowflake conn in order to take a snapshot of a list of databases
    and schemas.
    """

    def __init__(
        self,
        config: AxolotlConfig,
        connection_config: SnowflakeConnectionConfig,
        run_id: int,
        console: Console | LiveConsole = Console(),
    ):
        """
        Initialize a Snowflake connection.

        Args:
            config: AxolotlConfig object containing all configuration
            connection_name: Name of the connection to use from config.connections
            run_id: Unique identifier for this snapshot run
        """
        self.console = console

        # Store connection options and metrics config
        self.connection_config = connection_config

        self.max_threads = connection_config.max_threads or config.max_threads
        self.run_timeout_seconds = config.run_timeout_seconds
        self.connection_timeout_seconds = (
            connection_config.connection_timeout_seconds
            or config.connection_timeout_seconds
        )
        self.query_timeout_seconds = (
            connection_config.query_timeout_seconds or config.query_timeout_seconds
        )
        self.exclude_expensive_queries = (
            connection_config.exclude_expensive_queries
            if connection_config.exclude_expensive_queries is not None
            else config.exclude_expensive_queries
        )
        self.exclude_complex_queries = (
            connection_config.exclude_complex_queries
            if connection_config.exclude_complex_queries is not None
            else config.exclude_complex_queries
        )

        self.run_id = run_id

    def __enter__(self):
        self.conn = SnowflakeConn.get_conn(self.console, self.connection_config.params)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
        # Return None to propagate any exceptions
        return None

    @staticmethod
    def get_conn(
        console,
        conn_params,
    ):
        try:
            return snowflake.connector.connect(**conn_params)

        except Exception:
            # TODO: Don't print sensitive connection params
            console.print(
                f"Failed to create snowflake connection: {conn_params.get('connection_name')}"
            )
            console.print(traceback.format_exc())
            raise

    def snapshot(self, run_deadline: float) -> Iterator[Metric]:
        """
        Capture metrics for all tables in the configured schemas, both
        table-level and column-level for all tables

        Args:
            run_deadline: Time at which this entire run should be canceled

        Returns:
            List of Metric objects containing all collected metrics
        """
        t_conn_start = time.monotonic()
        yield from self._snapshot_threaded(run_deadline)
        print(f"Finished in {round(time.monotonic() - t_conn_start, 2)} seconds")

    def list_only(self, run_deadline: float) -> Iterator[str]:
        all_metric_queries, table_metrics = self._scan_for_queries()
        for m in table_metrics:
            yield f"{m.target_table} : {m.metric_name}"
        for q in all_metric_queries:
            for metric_name in q.metrics:
                yield f"{q.fq_table_name}.{q.column_name} : {metric_name}"

    def _scan_for_queries(self) -> Tuple[List[MetricQuery], List[Metric]]:
        # scan the db and generate metric queries & table metrics
        t_start = time.monotonic()

        databases = self._list_included_databases()

        table_metrics, table_names = self._scan_tables(databases)
        t_tablescan = time.monotonic()  ## Time it took to scan the table metrics
        self.console.print(
            f"Found {len(table_names)} tables in {round(t_tablescan - t_start, 2)} seconds. Enumerating columns..."
        )

        all_column_infos: List[ColumnInfo] = self.list_columns(databases)

        print(f"Generating queries for {len(all_column_infos)} columns...")
        all_metric_queries: List[MetricQuery] = []
        for c_info in all_column_infos:
            all_metric_queries.extend(self._generate_column_queries(c_info))

        return all_metric_queries, table_metrics

    def _start_metric_query(
        self, metric_query: MetricQuery, per_query_timeout: float
    ) -> MetricQuery:
        """
        Execute a MetricQuery asynchronously and return an updated MetricQuery with
        the query_id and status set to STARTED.

        Args:
            metric_query: MetricQuery with query string and status=QUEUED
            per_query_timeout: float in seconds indicating how long this query may take

        Returns:
            Updated MetricQuery with query_id, timeout_at, and status=STARTED or ERROR
        """
        try:
            with self.conn.cursor(DictCursor) as cur:
                cur.execute_async(metric_query.query)
                assert cur.sfqid is not None
                return metric_query._replace(
                    query_id=cur.sfqid,
                    timeout_at=time.monotonic() + per_query_timeout,
                    status=QueryStatus.STARTED,
                )
        except Exception:
            return metric_query._replace(status=QueryStatus.ERROR)

    def _cancel_queries(self, query_ids: List[str]):
        with self.conn.cursor() as cur:
            for query_id in query_ids:
                try:
                    cur.execute(f"SELECT SYSTEM$CANCEL_QUERY('{query_id}')")
                except Exception:
                    self.console.print(f"Error canceling query: {query_id}")
                    self.console.print(traceback.format_exc())

    def _snapshot_threaded(
        self,
        run_deadline: float,
    ) -> Iterator[Metric]:
        if is_timed_out([run_deadline]):
            raise TimeoutError("run already timed out")

        t_connection_start = time.monotonic()
        connection_deadline = t_connection_start + self.connection_timeout_seconds
        all_metric_queries, table_metrics = self._scan_for_queries()
        yield from table_metrics

        all_metric_queries.sort(key=query_priority)

        self.console.print(f"Snapshotting using {len(all_metric_queries)} queries...")
        num_queries = len(all_metric_queries)
        num_successful = 0  # Count of successful queries
        num_failed = 0  # Count of failed queries

        # FIXME: this is immoral and depraved. We should be locking this.
        running_queries: Dict[str, MetricQuery] = {}

        def _update():
            if isinstance(self.console, LiveConsole):
                self.console.update(
                    num_successful,
                    num_failed,
                    num_queries,
                    list(running_queries.values()),
                )

        def _run_and_wait(metric_query: MetricQuery, index: int) -> List[Metric]:
            metric_query = self._start_metric_query(
                metric_query, self.query_timeout_seconds
            )
            qid = metric_query.query_id
            assert qid

            all_metric_queries[index] = metric_query

            if metric_query.status != QueryStatus.STARTED:
                # Query failed to start
                self.console.print(
                    f"Failed to start: {metric_query.fq_table_name}.{metric_query.column_name} [{metric_query.query_detail}]"
                )
                raise Exception("Failed to start query")

            running_queries[qid] = metric_query
            while self.conn.is_still_running(
                self.conn.get_query_status(metric_query.query_id)
            ):
                time.sleep(0.1)
                if is_timed_out([metric_query.timeout_at, connection_deadline, run_deadline]):
                    del running_queries[qid]
                    self._cancel_queries([metric_query.query_id])
                    all_metric_queries[index] = all_metric_queries[index]._replace(status=QueryStatus.ERROR)
                    self.console.print(
                        f"Timed out: ({qid}): {metric_query.fq_table_name}.{metric_query.column_name} ({metric_query.query_detail}) at {metric_query.timeout_at} (current time {time.monotonic()})"
                    )
                    raise TimeoutError(
                        f"Timed out: {metric_query.fq_table_name}.{metric_query.column_name} ({metric_query.query_detail})"
                    )
            del running_queries[qid]

            try:
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

                    all_metric_queries[index] = all_metric_queries[index]._replace(status=QueryStatus.DONE)
                    return extracted_metrics
            except Exception as e:
                all_metric_queries[index] = all_metric_queries[index]._replace(status=QueryStatus.ERROR)
                self.console.print(
                    f"Error processing results for ({qid}): {metric_query.fq_table_name}.{metric_query.column_name} ({metric_query.query_detail})"
                )
                raise e

        # subtract the time we took to query the table stats and do setup, and
        # that's what we'll give the executor to run the column queries.
        executor = ThreadPoolExecutor(max_workers=self.max_threads)
        # future_to_mq = {
        #    executor.submit(_run_and_wait, mq): mq for mq in all_metric_queries
        # }

        futures = [
            executor.submit(_run_and_wait, mq, i)
            for i, mq in enumerate(all_metric_queries)
        ]

        all_queries_timeout = self.connection_timeout_seconds - (
            time.monotonic() - t_connection_start
        )
        run_timeout = run_deadline - time.monotonic()

        for future in as_completed(futures, timeout=min(run_timeout, all_queries_timeout)):
            # mq = future_to_mq[future]

            try:
                result = future.result()
                # mq._replace(status=QueryStatus.DONE)
                num_successful += 1
            except Exception as exc:
                # mq._replace(status=QueryStatus.ERROR)
                num_failed += 1
                _update()

            else:
                _update()
                yield from result
        self.console.print(all_metric_queries[0].status)

        # By this time, we've for db_timeout amount of time; shut down the executor
        executor.shutdown(wait=False, cancel_futures=True)

        if len(running_queries) > 0:
            self.console.print(f"canceling {len(running_queries)} queries")
            self._cancel_queries([rq.query_id for rq in running_queries.values()])
        return

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

    def list_columns(self, databases: List[str]) -> List[ColumnInfo]:
        """
        List all columns, respecting included and excluded schemas

        Args:
        Returns:
            ColumnInfo[]: all the information to scan the column
        """
        column_info_arr: List[ColumnInfo] = []

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
                    self.console.print(traceback.format_exc())
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
                self.console.print(traceback.format_exc())
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
