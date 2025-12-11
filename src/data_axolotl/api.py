from pathlib import Path
from typing import Optional, List, Dict, Set, Any
from typing_extensions import Annotated
from rich.console import Console
from .live_run_console import live_run_console
import itertools
import traceback

from .metric_set import MetricSet
from .history_report import HistoryReport
from .alert_report import AlertReport
from .config import load_config, config_from_dict
from .connectors.identifiers import IncludeDirective
from .trackers import AlertSeverity
from .config import DataAxolotlConfig
from .connectors.state_dao import Run
from .trackers import MetricAlert
from .timeouts import Timeout


type RunId = Annotated[int, "run id"]


def _parse_any_config(
    config: Path | Dict[str, Any] | str | DataAxolotlConfig | None,
) -> DataAxolotlConfig:
    if isinstance(config, DataAxolotlConfig):
        return config

    if config is None or config == "":
        config = Path("./config.yaml")
    if isinstance(config, str):
        config = Path(config)

    if isinstance(config, Path):
        return load_config(config)
    elif isinstance(config, dict):
        return config_from_dict(config)
    else:
        raise TypeError(f"Unknown config: {config!r}. Please provide a config dict or path.")


def run(
    config: Path | Dict[str, Any] | str | DataAxolotlConfig | None,
    list_only: Optional[bool] = False,
):
    """ Run data-axolotl with the given config.
    Raises an error if anything goes wrong.
    """
    config = _parse_any_config(config)
    console = Console()
    with config.get_state_dao() as state:
        with live_run_console() as console:
            with state.make_run() as run_id:
                run_timeout = Timeout(timeout_seconds=config.run_timeout_seconds, detail=f"run_id: {run_id}")
                run_timeout.start()
                console.print(f"Starting run #{run_id}...")

                for conn_config in config.connections.values():
                    with conn_config.get_conn(run_id, console) as conn:
                        if list_only:
                            for m in conn.list_only(run_timeout):
                                console.print(m)
                        else:
                            metrics_it = conn.snapshot(run_timeout)
                            for ms in itertools.batched(metrics_it, 1_000):
                                try:
                                    state.record_metric(ms)
                                except Exception as e:
                                    print(f"Error recording metric: {e}")
                                    print(traceback.format_exc())
                                    raise

def list_runs(
    config: Path | Dict[str, Any] | str | DataAxolotlConfig | None,
) -> List[Run]:
    """ Returns a list of Run objects.
    Each Run has the following properties:
      - run_id: int
      - started_at: datetime
      - finished_at: Optional[datetime]
      - successful: Optional[bool]
    """
    config = _parse_any_config(config)
    with config.get_state_dao() as state:
        return sorted(state.get_all_runs(), key=lambda r: r.run_id)

def rm_run(
    config: Path | Dict[str, Any] | str | DataAxolotlConfig | None,
    run_id: RunId,
) -> bool:
    """ Remove a run with the given id. Returns true if successful, false if the
    run does not exist."""
    config = _parse_any_config(config)

    with config.get_state_dao() as state:
        runs = state.get_all_runs()
        if run_id in [run.run_id for run in runs]:
            state.delete_run(run_id)
            return True
        else:
            return False

def get_alerts(
    config: Path | Dict[str, Any] | str | DataAxolotlConfig | None,
    target: Optional[List[str]] = None,
    run_id: Optional[RunId] = None,
    level: Set[AlertSeverity] = { AlertSeverity.Major, AlertSeverity.Minor },
) -> List[MetricAlert]:
    """ Get a list of alerts from a run. If you don't provide a run_id, the most
    recent successful run will be used. You can also provide `target` and `level` to
    filter which alerts are returned. """
    metric_set = get_metrics(config=config, run_id=run_id, include=target)
    return AlertReport(metric_set).get_items(level)

def get_metrics(
    config: Path | Dict[str, Any] | str | DataAxolotlConfig | None,
    run_id: Optional[int] = None,
    include: Optional[List[str] | List[IncludeDirective]] = None,
) -> MetricSet:
    config = _parse_any_config(config)
    include = [
        t if isinstance(t, IncludeDirective)
        else IncludeDirective.from_string(t)
        for t in (include or [])
    ]

    with config.get_state_dao() as state:
        run_id = run_id or state.get_latest_successful_run_id()
        if run_id is None:
            raise ValueError("No runs found")

        runs = state.get_all_runs()
        if run_id not in [r.run_id for r in runs]:
            raise ValueError(f"Run {run_id} does not exist.")

        if run_id not in [r.run_id for r in runs if r.successful]:
            raise ValueError(f"Run {run_id} was not successful.")

        filtered_runs = [
            r for r in runs
            if r.successful and r.run_id <= run_id
        ]

        metrics = [
            m for m in state.get_metrics(
                run_id_lte=run_id,
                only_successful=True,
            )
            if not include or m.matches_includes(include)
        ]

        return MetricSet(filtered_runs, metrics)
