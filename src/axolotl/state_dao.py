from typing import List
from typing import Any
from typing import Optional
from typing import NamedTuple
from datetime import datetime
from datetime import timezone
from contextlib import contextmanager
import json


class Run(NamedTuple):
    run_id: int
    started_at: datetime
    finished_at: Optional[datetime]
    successful: Optional[bool]

class Metric(NamedTuple):
    run_id: int
    target_table: str
    target_column: Optional[str]
    metric_name: str
    metric_value: Any
    measured_at: datetime


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

    def query(self, query_string: str, data: List[any] = []) -> List[List[any]]:
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
                measured_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (run_id, target_table, target_column, metric_name)
            );
        """)

    @contextmanager
    def make_run(self):
        run_id = self._make_new_run()
        try:
            yield run_id
            self._end_run(run_id, True)
        except:
            self._end_run(run_id, False)

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

    def _end_run(self, run_id: int, successful: bool) -> int:
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
        self.query(
            f"""
                INSERT INTO {p}thorntale_metric (run_id, target_table, target_column, metric_name, metric_value)
                VALUES (?, ?, ?, ?, ?)
            """,
            [metric.run_id, metric.target_table, metric.target_column, metric.metric_name, json.dumps(metric.metric_value)],
        )

    def get_metrics(self, run_id: Optional[str] = None, target_table: Optional[str] = None, target_column: Optional[str] = None) -> List[Metric]:
        p = self.table_prefix
        where_clause = ""
        where_values = []

        if run_id is not None:
            where_clause += " AND run_id = ?"
            where_values += [run_id]

        if target_table is not None:
            where_clause += " AND target_table = ?"
            where_values += [target_table]

        if target_column is not None:
            where_clause += " AND target_column = ?"
            where_values += [target_column]

        return [
            Metric(
                row[0],
                row[1],
                row[2],
                row[3],
                json.loads(row[4]),
                datetime.fromisoformat(row[5]),
            )
            for row
            in self.query(f"""
                SELECT run_id, target_table, target_column, metric_name, metric_value, measured_at
                FROM {p}thorntale_runs
                WHERE 1 = 1 {where_clause}
            """, where_values)
        ]
