from __future__ import annotations
from rich.console import Console
from rich.panel import Panel
from typing import List
from typing import Any
from typing import Optional
from typing import NamedTuple
from typing import Self
from typing import Iterator
from typing import cast
from datetime import datetime, date
from datetime import timezone
from contextlib import contextmanager
import simplejson as json  # for Decimal support
import traceback
import re
import typer
from collections.abc import Iterable

from .identifiers import FqTable, IncludeDirective
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .base_connection import BaseConnection


class Run(NamedTuple):
    run_id: int
    started_at: datetime
    finished_at: Optional[datetime]
    successful: Optional[bool]

class Metric(NamedTuple):
    run_id: int
    target_table: FqTable
    target_column: Optional[str]
    metric_name: str
    metric_value: Any
    measured_at: datetime

    def matches_includes(self, includes: List[IncludeDirective]) -> bool:
        if not includes:
            return True
        for inc in includes:
            if (
                inc.database_lower == self.target_table.database.lower()
                and inc.schema_lower is None
                and inc.table_lower is None
                and inc.column_lower is None
                and inc.metric_lower is None
            ):
                return True
            if (
                inc.database_lower == self.target_table.database.lower()
                and inc.schema_lower == self.target_table.schema.lower()
                and inc.table_lower is None
                and inc.column_lower is None
                and inc.metric_lower is None
            ):
                return True
            if (
                inc.database_lower == self.target_table.database.lower()
                and inc.schema_lower == self.target_table.schema.lower()
                and inc.table_lower == self.target_table.table.lower()
                and inc.column_lower is None
                and inc.metric_lower is None
            ):
                return True
            if self.target_column is None:
                if (
                    inc.database_lower == self.target_table.database.lower()
                    and inc.schema_lower == self.target_table.schema.lower()
                    and inc.table_lower == self.target_table.table.lower()
                    and inc.column_lower is None
                    and inc.metric_lower == self.metric_name.lower()
                ):
                    return True
            else:
                if (
                    inc.database_lower == self.target_table.database.lower()
                    and inc.schema_lower == self.target_table.schema.lower()
                    and inc.table_lower == self.target_table.table.lower()
                    and inc.column_lower == self.target_column.lower()
                    and inc.metric_lower == self.metric_name.lower()
                ):
                    return True
                if (
                    inc.database_lower == self.target_table.database.lower()
                    and inc.schema_lower == self.target_table.schema.lower()
                    and inc.table_lower == self.target_table.table.lower()
                    and inc.column_lower == self.target_column.lower()
                    and inc.metric_lower is None
                ):
                    return True
        return False


class StateDAO:
    """
    Database Access Object for the internal state db.
    conn - a Connection object for the db
    table_prefix - what to put before the table name when making queries
    """
    def __init__(
        self,
        conn: BaseConnection,
        table_prefix: str = "",
    ):
        self.conn = conn
        self.table_prefix = table_prefix
        self.setup_db_tables()

    def setup_db_tables(self):
        # TODO: handle errors here
        p = self.table_prefix
        try:
            self.conn.state_query(f"""
                CREATE TABLE IF NOT EXISTS {self.conn.escape_state_table(self.table_prefix, 'thorntale_run')} (
                    run_id INTEGER PRIMARY KEY,
                    started_at DATETIME NOT NULL,
                    finished_at DATETIME DEFAULT NULL,
                    successful INTEGER DEFAULT NULL
                );
            """)

            self.conn.state_query(f"""
                CREATE TABLE IF NOT EXISTS {self.conn.escape_state_table(self.table_prefix, 'thorntale_metric')} (
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
        except Exception as e:
            console = Console()
            console.print(Panel(str(e)))
            console.print(f"Error creating state tables. Please check your state configuration.")
            raise typer.Exit()

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
        next_id = self.conn.state_query(f"""SELECT max(run_id) + 1 FROM {self.conn.escape_state_table(self.table_prefix, 'thorntale_run')}""")[0][0] or 1
        self.conn.state_query(
            f"""
                INSERT INTO {self.conn.escape_state_table(self.table_prefix, 'thorntale_run')} (run_id, started_at)
                VALUES (?, ?)
            """,
            [next_id, datetime.utcnow()],
        )
        return next_id

    def _end_run(self, run_id: int, successful: bool):
        p = self.table_prefix
        self.conn.state_query(
            f"""
                UPDATE {self.conn.escape_state_table(self.table_prefix, 'thorntale_run')} SET finished_at = ?, successful = ? WHERE run_id = ?
            """,
            [datetime.utcnow(), 1 if successful else 0, run_id],
        )

    def get_all_runs(self) -> List[Run]:
        p = self.table_prefix
        result = self.conn.state_query(f"""
            SELECT run_id, started_at, finished_at, successful
            FROM {self.conn.escape_state_table(self.table_prefix, 'thorntale_run')}
        """)
        def parse_dt(val: datetime | str | None) -> datetime | None:
            if not val:
                return None
            elif isinstance(val, str):
                return datetime.fromisoformat(val).replace(tzinfo=timezone.utc)
            elif isinstance(val, datetime):
                return val
            else:
                assert False, f'Invalid datetime value {val}'
        return [
            Run(
                row[0],
                cast(datetime, parse_dt(row[1])),
                parse_dt(row[2]),
                None if row[3] is None else False if row[3] == 0 else True,
            )
            for row
            in result
        ]

    def get_latest_successful_run_id(self) -> Optional[int]:
        return max(
            run.run_id
            for run in self.get_all_runs()
            if run.successful
        )

    def delete_run(self, run_id: int):
        p = self.table_prefix
        self.conn.state_query(
            f"DELETE FROM {self.conn.escape_state_table(self.table_prefix, 'thorntale_run')} WHERE run_id = ?",
            [run_id],
        )
        self.conn.state_query(
            f"DELETE FROM {self.conn.escape_state_table(self.table_prefix, 'thorntale_metric')} WHERE run_id = ?",
            [run_id],
        )

    def record_metric(self, metrics: Iterable[Metric]):
        # prolly important this is the same order as the table column definition
        cols = [
            'RUN_ID',
            'TARGET_TABLE',
            'TARGET_COLUMN',
            'METRIC_NAME',
            'METRIC_VALUE',
            'VALUE_IS_DATETIME',
            'MEASURED_AT',
        ]
        def metric_row(metric: Metric) -> List[Any]:
            return [
                metric.run_id,
                str(metric.target_table),
                metric.target_column,
                metric.metric_name,
                (
                    metric.metric_value.isoformat()
                    if isinstance(metric.metric_value, (datetime, date)) else
                    json.dumps(metric.metric_value)
                ),
                1 if isinstance(metric.metric_value, (datetime, date)) else 0,
                metric.measured_at.isoformat(),
            ]

        metrics_list = list(metrics)
        try:
            # try the bulk upload, then fallback to INSERT if not supported
            self.conn.state_bulk_upload(
                self.conn.escape_state_table(self.table_prefix, 'thorntale_metric'),
                cols,
                [metric_row(m) for m in metrics_list],
            )
        except NotImplementedError:
            for metric in metrics_list:
                self.conn.state_query(
                    f"""
                        INSERT INTO {self.conn.escape_state_table(self.table_prefix, 'thorntale_metric')} (
                            {','.join(cols)}
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    metric_row(metric),
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
            where_clause += f" AND run_id in (select run_id from {self.conn.escape_state_table(self.table_prefix, 'thorntale_run')} where successful != 0)"

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

        def parse_datetime(d: str | datetime | date) -> date | datetime:
            if isinstance(d, str):
                if ' ' in d or 'T' in d:
                    return ensure_tz(datetime.fromisoformat(d))
                else:
                    return date.fromisoformat(d)
            else:
                return d

        return [
            Metric(
                row[0],
                FqTable.from_string(row[1]),
                row[2],
                row[3],
                parse_datetime(row[4]) if row[5] else json.loads(row[4]),
                ensure_tz(parse_datetime(row[6])),
            )
            for row
            in self.conn.state_query(f"""
                SELECT run_id, target_table, target_column, metric_name, metric_value, value_is_datetime, measured_at
                FROM {self.conn.escape_state_table(self.table_prefix, 'thorntale_metric')}
                WHERE 1 = 1 {where_clause}
                ORDER BY 1, 2, 3, 4, 5
            """, where_values)
        ]
