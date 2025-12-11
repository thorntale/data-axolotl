import sqlite3
from typing import Any, List, Dict
from rich.console import Console

from ..live_run_console import LiveConsole
from .base_connection import BaseConnection
from ..timeouts import Timeout


class SqliteConn(BaseConnection):
    @staticmethod
    def get_conn(console: Console | LiveConsole, params: Dict[str, Any]):
        return sqlite3.connect(params.get('path', "local.db"), isolation_level=None)

    def snapshot(self, run_timeout: Timeout):
        pass

    def list_only(self, run_timeout: Timeout):
        pass

    def state_query(self, query_string: str, data: List[Any] = []) -> List[List[Any]]:
        """ Runs a query against the DataAxolotl state tables """
        assert self.conn
        cur = self.conn.cursor()
        result = cur.execute(query_string, data)
        return result.fetchall()

    def escape_state_table(self, prefix: str, table: str) -> str:
        """ Makes a prefix + table name safe for insertion into a sql
        query. Prefix is a raw string taken from the config file. """
        if prefix:
            raise ValueError('prefix is not supported when using sqlite')
        return table
