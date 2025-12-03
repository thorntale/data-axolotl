from collections import deque
from typing import List, Dict, Set, Iterator, Any, Optional, Annotated
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed, Future
from rich.console import Console
from ..live_run_console import LiveConsole
import time

from ..timeouts import Timeout
from ..metric_query import QueryStatus, MetricQuery
from ..connectors.state_dao import Metric


class ParallelQueryExecutor:
    max_workers: int = 10
    console: Console | LiveConsole = Console()

    def __init__(
        self,
        queries: List[MetricQuery],
    ):
        self.queries = queries

        self.num_queries = len(queries)
        self.num_successful = 0  # Count of successful queries
        self.num_failed = 0  # Count of failed queries
        self.running_queries: Dict[Annotated[str, "qid"], MetricQuery] = {}
        self.queue = deque(queries)

    def _update(self):
        if isinstance(self.console, LiveConsole):
            self.console.update(
                self.num_successful,
                self.num_failed,
                self.num_queries,
                list(self.running_queries.values()),
            )

    def after_start(self, metric_query: MetricQuery, qid: str):
        # self.console.print(f"Started:  {metric_query.name()} ({qid})")
        self._update()

    def after_end(self, metric_query: MetricQuery, exception: Optional[BaseException]):
        if exception:
            self.console.print(f"Failed:   {metric_query.name()} ({exception!s})")
        # else:
        #     self.console.print(f"Complete: {metric_query.name()}")
        self._update()

    @staticmethod
    def begin(metric_query: MetricQuery) -> Annotated[str, "qid"]:
        raise NotImplementedError('Need to implement begin on QueryPoolExecutor subclass.')

    @staticmethod
    def resolve(metric_query, qid: str, timeout: Timeout) -> List[Metric]:
        raise NotImplementedError('Need to implement resolve on QueryPoolExecutor subclass.')

    def run(
        self,
        total_timeout: Timeout,
        per_query_timeout_seconds: float,
    ) -> Iterator[Metric]:
        """ Runs queries in parallel """

        with KeyedExecutor(max_workers=self.max_workers) as executor:
            while True:
                # submit up to `max_workers`
                while (
                    (
                        self.queue
                        and not total_timeout.is_timed_out()
                        and not self.num_failed
                    )
                    and executor.num_pending() < self.max_workers
                ):
                    metric_query = self.queue.popleft()
                    future = executor.submit(
                        ('starting', metric_query, None),
                        self.begin,
                        metric_query,
                    )

                # If nothing is pending, we must be done
                if not executor.num_pending():
                    break

                # get the next completed metric_query
                key, future = executor.get_next_completed()
                step, metric_query, qid = key

                any_exception = future.exception()
                if any_exception:
                    any_failed = True
                    del self.running_queries[qid]
                    self.num_failed += 1
                    self.after_end(metric_query, exception=any_exception)

                elif step == 'starting':
                    qid = future.result()
                    self.running_queries[qid] = metric_query._replace(
                        query_id=qid,
                        timeout_at=time.monotonic() + per_query_timeout_seconds,
                        status=QueryStatus.STARTED,
                    )
                    metric_query = self.running_queries[qid]
                    self.after_start(metric_query, qid)
                    timeout = Timeout(min(per_query_timeout_seconds, total_timeout.time_remaining))
                    timeout.start()
                    executor.submit(
                        ('ending', metric_query, qid),
                        self.resolve,
                        metric_query,
                        qid,
                        timeout,
                    )

                elif step == 'ending':
                    metrics = future.result()
                    del self.running_queries[qid]
                    self.num_successful += 1
                    self.after_end(metric_query, exception=None)
                    yield from metrics

                else:
                    raise ValueError('step has invalid value')


class KeyedExecutor(ThreadPoolExecutor):
    result_key_by_future: Dict[Future[Any], Any] = {}

    def submit(self, result_key, fn, /, *args, **kwargs):
        future = super().submit(fn, *args, **kwargs)
        self.result_key_by_future[future] = result_key

    def num_pending(self) -> int:
        return len(self.result_key_by_future)

    def get_next_completed(self):
        if not self.result_key_by_future:
            raise ValueError('No pending items')
        fut = next(as_completed(self.result_key_by_future.keys()))
        key = self.result_key_by_future[fut]
        del self.result_key_by_future[fut]
        return (key, fut)
