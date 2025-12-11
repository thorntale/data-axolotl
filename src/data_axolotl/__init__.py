from .api import run, get_alerts, list_runs, rm_run

from importlib.metadata import version as get_version
version = get_version('data-axolotl')
