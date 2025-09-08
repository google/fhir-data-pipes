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

Python 3

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

For details on useage, please check out the documentation here, see the section
titled
[Controller CLI](https://google.github.io/fhir-data-pipes/additional#controller-cli)

## Testing

### Running Tests

This project uses `unittest` for testing. To run the test suite, ensure your
virtual environment is active and you have installed the package as shown below:

```sh
pip install .[dev]
```

Then you can execute the following command from the project's root directory:

```sh
 python3 -m unittest tests/test_main.py
```

## Formatting and Linting

This project uses `black` for code formatting, and `PyLint` for linting. To run
these commands, you need to have installed the requirements in the repo's root
declared in the `requirements-dev.txt` file _(i.e. using
`pip install -r requirements-dev.txt` in the root folder's `venv`)_.

Ensure you have activated your venv. You can then run the formatter and linter
by changing directory to the `controller-cli` folder and running:

```sh
   black .
```

You can run the linter with:

```sh
   pylint .
```
