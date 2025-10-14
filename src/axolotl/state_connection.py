import sqlite3

import snowflake.connector
from typing import NamedTuple, List, Optional, Tuple
from snowflake.connector import DictCursor
from .state_dao import StateDAO, Metric
from datetime import datetime


class SnowflakeOptions(NamedTuple):
    user: str
    password: str
    account: str
    database: str
    warehouse: str
    table_schema: str


class MetricQuery(NamedTuple):
    target_column: str
    target_table: Optional[str]
    metric_name: str
    metric_query: str


class TableSummary(NamedTuple):
    table_name: str
    table_catalog: str
    table_schema: str
    row_count: int
    bytes: int
    last_altered: datetime


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

    def __init__(self, options: SnowflakeOptions):
        self.conn = snowflake.connector.connect(
            **options._asdict(),
        )
        self.database = options.database
        self.table_schema = options.table_schema

    def snapshot(self, run_id: str) -> List[Metric]:
        """
        Capture metrics for all tables in the configured database/schema, both
        table-level and column-level for all tables

        Args:
            run_id: Unique identifier for this snapshot run

        Returns:
            List of Metric objects containing all collected metrics
        """
        (metrics, table_names) = self.scan_table_level_metrics(run_id)
        for table in table_names:
            metrics.extend(self.snapshot_table(run_id, table))

        return metrics

    def scan_column(
        self,
        run_id: str,
        fq_table_name: str,
        column_name: str,
        data_type: str,
        is_nullable: str,
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
        data_type_simple = get_simple_data_type(data_type)

        # use c."column_name" form to avoid conflicts with outputs
        col_sql = f'c."{column_name}"'

        # metric name -> metric query
        query_columns = {
            "data_type": f"'{data_type}'",
            "row_count": "COUNT(*)",
            "null_count": f"COUNT_IF({col_sql} IS NULL)",
            "null_pct": f'100.0 * "null_count" / "row_count"',
        }

        if data_type_simple == "numeric":
            query_columns.update(
                {
                    "numeric_min": f"MIN({col_sql})",
                    "numeric_max": f"MAX({col_sql})",
                    "numeric_mean": f"AVG({col_sql})",
                    "numeric_mean": f"VARIANCE({col_sql})",
                }
            )
            ## TODO: percentiles and histograms
        elif data_type_simple == "string":
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
        ## TODO: Stats for the other types

        def run(query: str) -> Dict[str, Any]:
            with self.conn.cursor(DictCursor) as cur:
                try:
                    cur.execute(query)
                    return cur.fetchone()
                except:
                    print(query)
                    raise

        results = run(f"""
            SELECT CURRENT_TIMESTAMP() as "_measured_at",
                {",\n".join(
                    f'{metric_query} AS "{metric_name}"'
                    for metric_name, metric_query
                    in query_columns.items()
                )}
            FROM {fq_table_name} as c
        """)

        # Stage 2
        non_null_count = results['row_count'] - results['null_count']
        results.update(run(f"""
            SELECT CURRENT_TIMESTAMP() as "_measured_at",
                {
                    f'COUNT(DISTINCT {col_sql})'
                    if non_null_count <= 1e7 else
                    f'APPROX_COUNT_DISTINCT({col_sql})'
                } as "distinct_count",
                100.0 * "distinct_count" / {non_null_count} as "distinct_rate"
            FROM {fq_table_name} as c
        """))

        return [
            Metric(
                run_id=run_id,
                target_table=fq_table_name,
                target_column=column_name,
                metric_name=metric_name,
                metric_value=metric_value,
                measured_at=results["_measured_at"],
            )
            for metric_name, metric_value in results.items()
            if not metric_name.startswith("_")
        ]

    def snapshot_table(self, run_id: str, table_name: str) -> List[Metric]:
        """
        Get all the columns in a table then snapshot each column.

        Args:
            run_id: Unique identifier for this snapshot run
            table_name: Name of the table to scan (not fully qualified)

        Returns:
            List of Metrics for all columns in the table
        """

        print("scanning table", table_name)

        fq_table_name = f"{self.database}.{self.table_schema}.{table_name}"

        metrics: List[Metric] = []
        with self.conn.cursor() as cur:
            query = f"""
                SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_CATALOG = '{self.database}'
                    AND TABLE_SCHEMA = '{self.table_schema}'
                    AND TABLE_NAME = '{table_name}'
                """
            cur.execute(query)

            ## FIXME: Dispatch these async
            for column_name, data_type, is_nullable in cur:
                metrics.extend(
                    self.scan_column(
                        run_id, fq_table_name, column_name, data_type, is_nullable
                    )
                )
        return metrics

    def scan_table_level_metrics(self, run_id: str) -> Tuple[List[Metric], List[str]]:
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
                                run_id=run_id,
                                target_table=fq_table_name,
                                target_column=None,
                                metric_name="row_count",
                                metric_value=row_count,
                                measured_at=measured_at,
                            ),
                            Metric(
                                run_id=run_id,
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


def get_conn(options=None):
    # TODO: update this to take options
    conn = sqlite3.connect("local.db", isolation_level=None)
    return conn

