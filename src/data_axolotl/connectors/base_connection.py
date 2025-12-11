from typing import TypeVar, Dict, Any, Optional, List

from rich.console import Console

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..config import DataAxolotlConfig
from ..live_run_console import LiveConsole
from ..timeouts import Timeout


ConfigT = TypeVar('ConfigT')
ConnT = TypeVar('ConnT')


class BaseConnection[ConfigT, ConnT]:
    conn: Optional[ConnT] = None

    def __init__(
        self,
        data_axolotl_config: DataAxolotlConfig,
        connection_config: ConfigT,
        run_id: int,
        console: Console | LiveConsole = Console(),
    ):
        self.data_axolotl_config = data_axolotl_config
        self.connection_config = connection_config
        self.run_id = run_id
        self.console = console

    def __enter__(self):
        self.conn = self.__class__.get_conn(self.console, self.connection_config.params)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.conn:
            self.conn.close()
            self.conn = None
        # Return None to propagate any exceptions
        return None

    @staticmethod
    def get_conn(console: Console | LiveConsole, params: Dict[str, Any]) -> ConnT:
        raise NotImplementedError('Must define get_conn in subclass')

    def snapshot(self, run_timeout: Timeout):
        raise NotImplementedError('Must define snapshot in subclass')

    def list_only(self, run_timeout: Timeout):
        raise NotImplementedError('Must define list_only in subclass')

    def state_query(self, query_string: str, data: List[Any] = []) -> List[List[Any]]:
        """ Runs a query against the DataAxolotl state tables """
        raise NotImplementedError('Must define state_query in subclass')

    def state_bulk_upload(self, table: str, cols: List[str], data: List[List[Any]]) -> bool:
        raise NotImplementedError('No state_bulk_upload provided')

    def escape_state_table(self, prefix: str, table: str) -> str:
        """ Makes a prefix + table name safe for insertion into a sql
        query. Prefix is a raw string taken from the config file. """
        return f"{prefix}.{table}"
