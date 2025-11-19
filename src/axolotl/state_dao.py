from rich.console import Console
from typing import List
from typing import Any
from typing import Optional
from typing import NamedTuple
from datetime import datetime, date
from datetime import timezone
from contextlib import contextmanager
import simplejson as json  # for Decimal support
import traceback
import re


class Run(NamedTuple):
    run_id: int
    started_at: datetime
    finished_at: Optional[datetime]
    successful: Optional[bool]

ident = r'([^":.]+|"(""|[^"])+")'
metric = r'([^.:]+)'
fq_table_regex = (
    rf'^(?P<database>{ident})\.(?P<schema>{ident})\.(?P<table>{ident})$'
)
include_derective_regexs = [
    rf'^(?P<database>{ident})$',
    rf'^(?P<database>{ident})\.(?P<schema>{ident})$',
    rf'^(?P<database>{ident})\.(?P<schema>{ident})\.(?P<table>{ident})$',
    rf'^(?P<database>{ident})\.(?P<schema>{ident})\.(?P<table>{ident}):(?P<metric>{metric})$',
    rf'^(?P<database>{ident})\.(?P<schema>{ident})\.(?P<table>{ident})\.(?P<column>{ident})$',
    rf'^(?P<database>{ident})\.(?P<schema>{ident})\.(?P<table>{ident})\.(?P<column>{ident}):(?P<metric>{metric})$',
]

def parse_ident(sql_ident: str) -> str:
    if sql_ident[0] == '"' and sql_ident[-1] == '"':
        return sql_ident[1:-1].replace('""', '"')
    elif '"' in sql_ident:
        raise TypeError(f'Unparsable sql identifier {sql_ident!r}')
    else:
        return sql_ident
def parse_maybe_ident(sql_ident: str | None) -> str | None:
    if sql_ident is None:
        return None
    else:
        return parse_ident(sql_ident)

class FqTable(NamedTuple):
    database: str
    schema: str
    table: str

    def __str__(self):
        return '.'.join(
            p if re.match(r'[a-zA-Z_][a-zA-Z0-9_]*', p)
            else '"' + p.replace('"', '""') + '"'
            for p in [
                self.database,
                self.schema,
                self.table,
            ]
        )

    @staticmethod
    def from_string(v: str) -> FqTable:
        m = re.match(fq_table_regex, v)
        if not m:
            raise ValueError(f'Cannot parse fully qualified table name from {v}')
        return FqTable(
            parse_ident(m.group('database')),
            parse_ident(m.group('schema')),
            parse_ident(m.group('table')),
        )

class IncludeDerictive(NamedTuple):
    database: str
    schema: str | None
    table: str | None
    column: str | None
    metric: str | None

    @staticmethod
    def from_string(v: str) -> IncludeDerictive:
        for regex in include_derective_regexs:
            m = re.match(regex, v)
            if m:
                groups = m.groupdict()
                return IncludeDerictive(
                    parse_ident(groups['database']),
                    parse_maybe_ident(groups.get('schema')),
                    parse_maybe_ident(groups.get('table')),
                    parse_maybe_ident(groups.get('column')),
                    parse_maybe_ident(groups.get('metric')),
                )
        raise ValueError(f'Cannot parse include/exclude directive from {v}')

    @property
    def database_lower(self):
        return None if self.database is None else self.database.lower()
    @property
    def schema_lower(self):
        return None if self.schema is None else self.schema.lower()
    @property
    def table_lower(self):
        return None if self.table is None else self.table.lower()
    @property
    def column_lower(self):
        return None if self.column is None else self.column.lower()
    @property
    def metric_lower(self):
        return None if self.metric is None else self.metric.lower()


class Metric(NamedTuple):
    run_id: int
    target_table: FqTable
    target_column: Optional[str]
    metric_name: str
    metric_value: Any
    measured_at: datetime

    def matches_filter(self, filters: List[str]) -> bool:
        def is_sublist_of(needle, haystack):
            return any(
                needle == haystack[i:i + len(needle)]
                for i in range(0, len(haystack) - len(needle) + 1)
            )
        if not filters:
            return True
        haystack = f"{self.target_table}.{self.target_column}".lower().split('.')
        return any(
            is_sublist_of(f.lower().split('.'), haystack)
            for f in filters
        )

class StateDAO:
    """
    Database Access Object for the internal state db.
    conn - a Connection object for the db
    table_prefix - what to put before the table name when making queries
    """
    def __init__(self, conn, table_prefix: str = ""):
        self.conn = conn
        self.table_prefix = table_prefix
        self.setup_db_tables()

    def query(self, query_string: str, data: List[Any] = []) -> List[List[Any]]:
        cursor = self.conn.cursor()
        result = cursor.execute(query_string, data)
        return result.fetchall()

    def setup_db_tables(self):
        p = self.table_prefix
        self.query(f"""
            CREATE TABLE IF NOT EXISTS {p}thorntale_run (
                run_id INTEGER PRIMARY KEY,
                started_at DATETIME NOT NULL,
                finished_at DATETIME DEFAULT NULL,
                successful INTEGER DEFAULT NULL
            );
        """)

        self.query(f"""
            CREATE TABLE IF NOT EXISTS {p}thorntale_metric (
                run_id INTEGER NOT NULL,
                target_table TEXT NOT NULL,
                target_column TEXT DEFAULT NULL,
                metric_name TEXT NOT NULL,
                metric_value TEXT,
                value_is_datetime INTEGER NOT NULL DEFAULT 0,
                measured_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (run_id, target_table, target_column, metric_name)
            );
        """)

        try:
            self.query(f"""
                ALTER TABLE {p}thorntale_metric ADD COLUMN value_is_datetime INTEGER NOT NULL DEFAULT 0;
            """)
        except:
            # if the table was already updated this will fail, which is fine
            pass

    @contextmanager
    def make_run(self):
        run_id = self._make_new_run()
        try:
            yield run_id
            self._end_run(run_id, True)
        except Exception as e:
            self._end_run(run_id, False)
            console = Console()
            console.print(traceback.format_exc())
            console.print('Run failed!!!')
            console.print(e)

    def _make_new_run(self) -> int:
        """ returns the run id """
        p = self.table_prefix
        next_id = self.query(f"""SELECT max(run_id) + 1 FROM {p}thorntale_run""")[0][0] or 1
        self.query(
            f"""
                INSERT INTO {p}thorntale_run (run_id, started_at)
                VALUES (?, ?)
            """,
            [next_id, datetime.utcnow()],
        )
        return next_id

    def _end_run(self, run_id: int, successful: bool):
        p = self.table_prefix
        self.query(
            f"""
                UPDATE {p}thorntale_run SET finished_at = ?, successful = ? WHERE run_id = ?
            """,
            [datetime.utcnow(), 1 if successful else 0, run_id],
        )

    def get_all_runs(self) -> List[Run]:
        p = self.table_prefix
        return [
            Run(
                row[0],
                datetime.fromisoformat(row[1]).replace(tzinfo=timezone.utc),
                datetime.fromisoformat(row[2]).replace(tzinfo=timezone.utc) if row[2] else None,
                None if row[3] is None else False if row[3] == 0 else True,
            )
            for row
            in self.query(f"""
                SELECT run_id, started_at, finished_at, successful
                FROM {p}thorntale_run
            """)
        ]

    def get_latest_successful_run_id(self) -> Optional[int]:
        return max(
            run.run_id
            for run in self.get_all_runs()
            if run.successful
        )

    def delete_run(self, run_id: int):
        p = self.table_prefix
        self.query(
            f"DELETE FROM {p}thorntale_run WHERE run_id = ?",
            [run_id],
        )
        self.query(
            f"DELETE FROM {p}thorntale_metric WHERE run_id = ?",
            [run_id],
        )

    def record_metric(self, metric: Metric):
        p = self.table_prefix
        serialized_value = (
            metric.metric_value.isoformat()
            if isinstance(metric.metric_value, (datetime, date)) else
            json.dumps(metric.metric_value)
        )
        self.query(
            f"""
                INSERT INTO {p}thorntale_metric (
                    run_id,
                    target_table,
                    target_column,
                    metric_name,
                    metric_value,
                    value_is_datetime,
                    measured_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                metric.run_id,
                str(metric.target_table),
                metric.target_column,
                metric.metric_name,
                serialized_value,
                1 if isinstance(metric.metric_value, (datetime, date)) else 0,
                metric.measured_at,
            ],
        )

    def get_metrics(
        self,
        run_id: Optional[str] = None,
        run_id_lte: Optional[int] = None,
        only_successful: Optional[bool] = False,
        target_table: Optional[str] = None,
        target_column: Optional[str] = None,
    ) -> List[Metric]:
        p = self.table_prefix
        where_clause = ""
        where_values: List[Any] = []

        if run_id is not None:
            where_clause += " AND run_id = ?"
            where_values += [run_id]

        if run_id_lte is not None:
            where_clause += " AND run_id <= ?"
            where_values += [run_id_lte]

        if only_successful:
            where_clause += f" AND run_id in (select run_id from {p}thorntale_run where successful)"

        if target_table is not None:
            where_clause += " AND target_table = ?"
            where_values += [target_table]

        if target_column is not None:
            where_clause += " AND target_column = ?"
            where_values += [target_column]

        def ensure_tz(dt):
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt

        def parse_datetime(d: str) -> date | datetime:
            if ' ' in d or 'T' in d:
                return ensure_tz(datetime.fromisoformat(d))
            else:
                return date.fromisoformat(d)

        return [
            Metric(
                row[0],
                FqTable.from_string(row[1]),
                row[2],
                row[3],
                parse_datetime(row[4]) if row[5] else json.loads(row[4]),
                ensure_tz(datetime.fromisoformat(row[6])),
            )
            for row
            in self.query(f"""
                SELECT run_id, target_table, target_column, metric_name, metric_value, value_is_datetime, measured_at
                FROM {p}thorntale_metric
                WHERE 1 = 1 {where_clause}
            """, where_values)
        ]
