# axolotl

[![PyPI - Version](https://img.shields.io/pypi/v/axolotl.svg)](https://pypi.org/project/axolotl)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/axolotl.svg)](https://pypi.org/project/axolotl)

-----

# Usage
The simplest way to try out Axolotl is from your local computer with a cli command. For production deployments, see the [Usage with Airflow](#usage-with-airflow) or [Usage via API](#usage-via-api) guides below.

## Usage Locally
First, install axolotl via pip. (You can also install Axolotl inside a [virtual environment](https://docs.python.org/3/tutorial/venv.html))
```sh
pip install axolotl
```
Verify it was installed successfully by running
```sh
axolotl --version
```

By default, axolotl stores its state in a local SQLite database, `./local.db`. State is used to track changes between runs and generate alerts. You can override where state is stored via the [config](#configuration).

A common pattern is to store state alongside the data you're monitoring, in a dedicated `axolotl` table. An example of this is shown in the [configuration section](#configuration). **TODO: write the example**


## Usage via API
**TODO**

## Usage with Airflow
**TODO**

# Configuration
When run as a cli command, Axolotl reads its configuration from a config file. The default config location is `./config.yaml` in the current working directory. You can also specify a config file via the cli `--config-path [path]` argument.

When running via the python api, such as with airflow, **TODO: how to pass config?**.

A description of config.yaml is as follows: **TODO**

```yaml
# config.yaml
---
TODO: yaml config definitions
```

# Development
Most users of axolotl won't need to follow the steps in this section. For only using Axolotl, just follow the guides above. However, if you're looking to make changes or contribute code, follow the guide below.

Axolotl manages its dev environment with [Hatch](https://hatch.pypa.io/). Follow their [installation guide](https://hatch.pypa.io/latest/install/) to get set up on your system.

Once hatch is installed, you can run axolotl as follows.

```sh
hatch run axolotl [args]
```

You can add dependencies in `pyproject.toml`, which will be auto updated whenever you do `hatch run`.

# License

`axolotl` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
