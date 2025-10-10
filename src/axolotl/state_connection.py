import sqlite3

# from .snapshot_snowflake import scan_database
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



class SnowflakeConn:
    """
    hacky mid-refactor, sorry
    """

    def __init__(self, options: SnowflakeOptions):
        self.conn = snowflake.connector.connect(
            **options._asdict(),
        )
        self.database = options.database
        self.table_schema = options.table_schema
    

    def snapshot(self, run_id: str) -> List[Metric]:
        metrics, table_names = self.scan_table_level_metrics(run_id)
        return metrics

    def scan_table_level_metrics(self, run_id: str) -> Tuple[List[Metric], List[str]]:
        metrics = []
        table_names = []  ## fully qualified table names

        with self.conn.cursor() as cur:
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

            for (
                table_catalog,
                table_schema,
                table_name,
                row_count,
                table_bytes,
                last_altered,
                measured_at,
            ) in cur:
                table_names.append(f"{table_catalog}.{table_schema}.{table_name}")
                print("??")

                try:
                    metrics.extend(
                        [
                            Metric(
                                run_id=run_id,
                                target_table=table_name,
                                target_column=None,
                                metric_name="row_count",
                                metric_value=row_count,
                                measured_at=measured_at,
                            ),
                            Metric(
                                run_id=run_id,
                                target_table=table_name,
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


## TODO: this is all old stuff that needs to be refactored

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


def get_conn(options=None):
    # TODO: update this to take options
    conn = sqlite3.connect("local.db", isolation_level=None)
    return conn


def get_snowflake_conn(
    options: SnowflakeOptions,
) -> snowflake.connector.SnowflakeConnection:
    conn = snowflake.connector.connect(
        **options._asdict(),
    )

    # scan_database(conn, options)
    return conn


def scan_database(
    conn: snowflake.connector.SnowflakeConnection,
    options: SnowflakeOptions,
    state: StateDAO,
    run_id: int,
):
    tables = []

    with conn.cursor(DictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ROW_COUNT, BYTES, LAST_ALTERED
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_CATALOG = '{options.database}'
                AND TABLE_SCHEMA = '{options.table_schema}';
            """
        )

        for table_rec in cur:
            print(f"{table_rec}")
            tables.append(table_rec)

    for t in tables:
        scan_table(conn, t, state, run_id)


def scan_table(
    conn: snowflake.connector.SnowflakeConnection,
    table_rec: dict,
    state: StateDAO,
    run_id: int,
):
    fq_tablename = f"{table_rec["TABLE_CATALOG"]}.{table_rec["TABLE_SCHEMA"]}.{table_rec["TABLE_NAME"]}"
    cols: List[List[MetricQuery]] = []

    with conn.cursor(DictCursor) as cur:
        cur.execute(
            f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_CATALOG = '{table_rec["TABLE_CATALOG"]}'
                AND TABLE_SCHEMA = '{table_rec["TABLE_SCHEMA"]}'
                AND TABLE_NAME = '{table_rec["TABLE_NAME"]}';
            """
        )

        for columnn_rec in cur:
            c_name = columnn_rec["COLUMN_NAME"]
            query_columns: List[MetricQuery] = []

            query_columns.extend(
                [
                    MetricQuery(
                        target_column=c_name,
                        target_table=fq_tablename,
                        metric_name="distinct_rate",
                        metric_query=f"100.0 * COUNT(DISTINCT({c_name})) / COUNT(*)",
                    ),
                ]
            )

            if columnn_rec["IS_NULLABLE"] == "YES":
                query_columns.extend(
                    [
                        MetricQuery(
                            target_column=c_name,
                            target_table=fq_tablename,
                            metric_name="null_count",
                            metric_query=f"COUNT_IF({c_name} IS NULL)",
                        ),
                        MetricQuery(
                            target_column=c_name,
                            target_table=fq_tablename,
                            metric_name="null_pct",
                            metric_query=f"100.0 * COUNT_IF({c_name} IS NOT NULL) / COUNT(*)",
                        ),
                    ]
                )

            ## These data type selectors should be fully distinct
            if columnn_rec["DATA_TYPE"].upper() in SNOWFLAKE_NUMERIC_TYPES:
                query_columns.extend(
                    [
                        MetricQuery(
                            target_column=c_name,
                            target_table=fq_tablename,
                            metric_name="min",
                            metric_query=f"MIN({c_name})",
                        ),
                        MetricQuery(
                            target_column=c_name,
                            target_table=fq_tablename,
                            metric_name="max",
                            metric_query=f"MAX({c_name})",
                        ),
                        MetricQuery(
                            target_column=c_name,
                            target_table=fq_tablename,
                            metric_name="mean",
                            metric_query=f"AVG({c_name})",
                        ),
                        MetricQuery(
                            target_column=c_name,
                            target_table=fq_tablename,
                            metric_name="variance",
                            metric_query=f"VARIANCE({c_name})",
                        ),
                    ]
                    ## TODO: percentiles and histograms
                )

            elif columnn_rec["DATA_TYPE"].upper() in SNOWFLAKE_TEXT_TYPES:
                query_columns.extend(
                    [
                        MetricQuery(
                            target_column=c_name,
                            target_table=fq_tablename,
                            metric_name="avg_length",
                            metric_query=f"AVG(LEN({c_name}))",
                        ),
                    ]
                )

            elif columnn_rec["DATA_TYPE"].upper() == "BOOLEAN":
                query_columns.extend(
                    [
                        MetricQuery(
                            target_column=c_name,
                            target_table=fq_tablename,
                            metric_name="true_count",
                            metric_query=f"COUNT_IF({c_name} = TRUE)",
                        ),
                        MetricQuery(
                            target_column=c_name,
                            target_table=fq_tablename,
                            metric_name="false_count",
                            metric_query=f"COUNT_IF({c_name} = FALSE)",
                        ),
                    ],
                )

            cols.append(query_columns)

    print(len(cols))

    for query_columns in cols:
        query = f"""
            SELECT {",\n".join([f"{m.metric_query} AS {m.metric_name}" for m in query_columns])} 
            FROM {fq_tablename}
        """

        print(query)
        print(query_columns[0].target_column)
        with conn.cursor(DictCursor) as cur:
            cur.execute(query)

            for r in cur:
                print(f"{r}")
                for k in r.keys():
                    state.record_metric(
                        run_id, fq_tablename, query_columns[0].target_column, k, r[k]
                    )

    # return query_columns
