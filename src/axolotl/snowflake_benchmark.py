import time
import traceback
from typing import Dict, Any, List

from snowflake.connector import DictCursor

from .snowflake_connection import SnowflakeConn, ColumnInfo
from .state_dao import Metric


class SnowflakeBenchmarkConn(SnowflakeConn):
    """
    Extension of SnowflakeConn for benchmarking query performance.
    Includes methods for timing individual queries and packaging results.
    """

    def _package_metrics(
        self, column_info: ColumnInfo, query_results: Dict[str, Any]
    ) -> List[Metric]:
        """
        Takes the output of _query_values and packages them into metrics list
        """
        metrics: List[Metric] = []
        (fq_table_name, column_name, _, _, _) = column_info

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

    def _query_values_benchmark(
        self, query_columns: dict[str, str], column_info: ColumnInfo
    ) -> Dict[str, float]:
        """
        INTERNAL ONLY. Runs each query_column query separately, and instead of
        the result, returns a dict { metric_name : query duration}
        """

        (fq_table_name, _, _, _, _) = column_info
        res: Dict[str, float] = {}

        for metric_name, metric_query in query_columns.items():
            query = f"""
                SELECT {metric_query} AS "{metric_name}"
                FROM {fq_table_name} as c
            """
            with self.conn.cursor(DictCursor) as cur:
                try:
                    t0 = time.monotonic()
                    cur.execute(query)
                    results = cur.fetchone()
                    t1 = time.monotonic()
                    if not results:
                        raise ValueError("No results found")

                    res[metric_name] = t1 - t0

                except Exception:
                    print(f"Error executing query: {query}")
                    print(traceback.format_exc())
                    raise
        return res

    def _query_values(
        self, query_columns: dict[str, str], column_info: ColumnInfo
    ) -> Dict[str, Any]:
        """
        Use this instead of _query_metrics when you need to do something else to
        the results before packaging them.

        This packages the results into a dict[metric name, metric value] instaed
        of a list of metrics.
        """

        (fq_table_name, _, _, _, database) = column_info
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
                cur.execute(f"USE DATABASE {database.database};")
                cur.execute(query)
                results = cur.fetchone()
                if not results:
                    raise ValueError("No results found")

            except Exception:
                print(f"Error executing query: {query}")
                print(traceback.format_exc())
                raise
            res = results

        return res
