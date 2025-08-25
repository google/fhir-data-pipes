# The CLI tool for fhir-data-pipes

This is a simple command-line interface (CLI) tool that interacts with the
pipeline controller. It has most of the functionality available in the pipeline
Web Control Panel enabling one to run the pipelines, view configuration
settings, manage the data warehouse snaphots and other pipeline operations.

The CLI tool is packaged using setup tools and exposes the application via the
`controller` utility command.

## Features

You can get the list of available commands by running with the help flag `-h` or
`--help`. See [Usage](#usage) below.

## Installation

### Prerequisites

Python 3.9 or later

### Steps

```sh
cd pipelines/controller-cli
```

### Create and activate a virtual environment:

### On macOS/Linux

```sh
python3 -m venv .venv
source .venv/bin/activate
```

### On Windows

```bash
python -m venv .venv
.venv\Scripts\activate
```

## Development

Install the project in editable mode. This will install the required
dependencies and make the `controller` command available in your terminal. Any
updates to the code will reflect immediately.

```sh
pip install -e .
```

## Final Installation

For a final installation, use this command to install the package as a single
file.

```sh
pip install .
```

## Usage

After installation, you can now use the `controller` command from your terminal.
The first positional parameter is the _url of the pipeline controller's REST
API_. You then need to pass the commands as shown after running `controller`
with the help command.

```sh
usage: controller [-h] url {config,next,status,run,tables,logs,dwh} ...

The CLI tool for fhir-data-pipes

positional arguments:
  url                   url of the pipeline controller's REST API
  {config,next,status,run,tables,logs,dwh}
                        dwh, next, status, run, config, logs, tables are the available commands.
    config              show config values
    next                show the next scheduled run
    status              show the status of the pipeline
    run                 run the pipeline
    tables              create resource tables
    logs                show logs
    dwh                 show a list of dwh snapshots

optional arguments:
  -h, --help            show this help message and exit
```

### Show config values

```sh
controller <url> config
```

### Show specific config value

```sh
controller <url> config --config-name <config key>
```

### Show the next scheduled run

```sh
controller <url> next
```

### Show the status of the pipeline

```sh
controller <url> status
```

### Run the pipeline

```sh
controller <url> run --mode <run mode>
```

**Note:** To run a pipeline you must supply a run mode using the `-m` or
`--mode` flag. The value of mode can be either of `full`, `incremental` or
`views`.

### Create resource tables

```sh
controller <url> tables
```

### Download error logs

```sh
controller <url> logs --download
```

You can pass an optional file name for the downloaded file. The default is
`error.log`.

```sh
controller <url> logs --download --filename <filename>
```

### Show a list of dwh snapshots

```sh
controller <url> dwh
```

Delete a specific snapshot

```sh
controller <url> dwh delete --snapshot-id <snapshot id>
```

**Note:** You can get the snapshot id by running the `controller <url> dwh`
first. A valid snapshot-id is the full id as shown in the list e.g.
`dwh/controller_DEV_DWH_TIMESTAMP_2025_08_14T17_47_15_357080Z`

## Testing

### Running Tests

This project uses `unittest` for testing. To run the test suite, ensure your
virtual environment is active and you have installed the package as shown in the
[Development](#development) instructions above. Then you can execute the
following command from the project's root directory:

```sh
 python3 -m unittest tests/test_main.py
```

**Note:** update the TestControllerCLI.TEST_URL constant in tests/test_main.py
to point to your local server if necessary.

## Formatting and Linting

This project uses `black` for code formatting, `isort` for import sorting, and
`PyLint` for linting. To run these commands, you need to have installed the
requirements in the repo's root declared in the `requirements-dev.txt` file
_(i.e. using `pip install -r requirements-dev.txt` in the root folder's
`venv`)_.

Ensure you have activated your venv. You can then run the formatter and sorter
by changing directory to the `controller-cli` folder and running:

```sh
   black . && isort .
```

You can run the linter with:

```sh
   pylint .
```
