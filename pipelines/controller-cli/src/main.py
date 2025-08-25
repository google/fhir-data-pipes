#!/usr/bin/env python3
# Copyright 2020-2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import json
import logging
import shutil
from typing import Any, Dict, Optional

import requests

HTTP_GET = "GET"
HTTP_POST = "POST"

RUN_MODES = ["INCREMENTAL", "FULL", "VIEWS"]

COMMAND_MAP = {
    "DWH": "dwh",
    "NEXT": "next",
    "STATUS": "status",
    "RUN": "run",
    "CONFIG": "config",
    "LOGS": "logs",
    "TABLES": "tables",
}


def process_response(response: str, args: Dict[str, Any]):
    print(
        f"Command: {args.command} {args.subcommand if hasattr(args, 'subcommand') else ''}"
    )
    print(f"Request url: {args.url}")
    print("Response:")
    try:
        print(json.dumps(response, indent=4))
    except json.JSONDecodeError as json_err:
        print(response)


def _make_api_request(
    verb: str, url: str, params: Optional[Dict[str, Any]] = None
) -> Optional[Dict[str, Any]]:
    try:
        if verb == HTTP_POST:
            response = requests.post(url, json={}, timeout=5)
        else:
            response = requests.get(url, params=params, timeout=5)

        response.raise_for_status()

        if response.headers.get("Content-Type", "").startswith("application/json"):
            data = response.json()
        else:
            data = response.text
        return data
    except requests.exceptions.RequestException as req_err:
        logging.error(f"An error occurred during the request: {req_err}")
    except json.JSONDecodeError as json_err:
        logging.error(f"Failed to decode JSON response: {json_err}")
        print(response)
        return None


def config(args) -> str:
    try:
        if args.config_name:
            response = _make_api_request(
                HTTP_GET, f"{args.url}/config/{args.config_name}"
            )
        else:
            response = _make_api_request(HTTP_GET, f"{args.url}/config")
        process_response(response, args)
    except requests.exceptions.RequestException as e:
        print(f"Error processing: {e}")


def next(args) -> str:
    try:
        response = _make_api_request(HTTP_GET, f"{args.url}/next")
        process_response(response, args)
    except requests.exceptions.RequestException as e:
        print(f"Error processing: {e}")


def status(args) -> str:
    try:
        response = _make_api_request(HTTP_GET, f"{args.url}/status")
        process_response(response, args)
    except requests.exceptions.RequestException as e:
        print(f"Error processing: {e}")


def run(args) -> str:
    try:
        response = _make_api_request(
            HTTP_POST, f"{args.url}/run?runMode={args.mode.upper()}"
        )
        process_response(response, args)
    except requests.exceptions.RequestException as e:
        print(f"Error processing: {e}")


def tables(args) -> str:
    try:
        response = _make_api_request(HTTP_POST, f"{args.url}/tables")
        process_response(response, args)
    except requests.exceptions.RequestException as e:
        print(f"Error processing: {e}")


def download_file(url: str, filename: str) -> str:
    try:
        with requests.get(url, stream=True) as r:
            with open(filename, "wb") as f:
                shutil.copyfileobj(r.raw, f)
        return f"File downloaded successfully to {filename}"
    except requests.exceptions.RequestException as e:
        return f"Error downloading file: {e}"


def logs(args) -> str:
    try:
        if args.download:
            filename = args.filename if args.filename else "error.log"
            response = download_file(f"{args.url}/download-error-log", filename)
            process_response(response, args)
        else:
            print(
                "You can use the tail command to watch the {DWH_ROOT}/error.log file."
            )
    except requests.exceptions.RequestException as e:
        print(f"Error processing: {e}")


def delete_snapshot(args) -> str:
    try:
        response = requests.delete(f"{args.url}/dwh?snapshotId={args.snapshot_id}")
        if response.status_code == 204:
            return f"Snapshot deleted successfully"
        else:
            return f"Error deleting snapshot: Status code {response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"Error deleting snapshot: {e}"


def dwh(args) -> str:
    try:
        if hasattr(args, "snapshot_id"):
            response = delete_snapshot(args)
        else:
            response = _make_api_request(HTTP_GET, f"{args.url}/dwh")
        process_response(response, args)
    except requests.exceptions.RequestException as e:
        print(f"Error processing: {e}")


def main():
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="The CLI tool for fhir-data-pipes",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "url",
        type=str,
        help="url of the pipeline controller's REST API",
    )
    subparsers = parser.add_subparsers(
        dest="command",
        help=f'{", ".join(str(value) for value in COMMAND_MAP.values())} '
        "are the available commands.",
        required=True,
    )

    config_parser = subparsers.add_parser("config", help="show config values")
    config_parser.add_argument(
        "--config-name", "-cn", required=False, help="name of the configuration key"
    )
    config_parser.set_defaults(func=config)

    next_parser = subparsers.add_parser("next", help="show the next scheduled run")
    next_parser.set_defaults(func=next)

    status_parser = subparsers.add_parser(
        "status", help="show the status of the pipeline"
    )
    status_parser.set_defaults(func=status)

    run_modes = ", ".join(mode.lower() for mode in RUN_MODES)
    run_parser = subparsers.add_parser("run", help="run the pipeline")
    run_parser.add_argument(
        "--mode",
        "-m",
        choices=([mode.lower() for mode in RUN_MODES]),
        required=True,
        help=f"the runType argument, options are {run_modes}",
    )
    run_parser.set_defaults(func=run)

    tables_parser = subparsers.add_parser("tables", help="create resource tables")
    tables_parser.set_defaults(func=tables)

    logs_parser = subparsers.add_parser("logs", help="show logs")
    logs_parser.add_argument("--download", action="store_true")
    logs_parser.add_argument(
        "--filename",
        "-f",
        required=False,
        help="name of the downloaded file, default error.log",
    )
    logs_parser.set_defaults(func=logs)

    dwh_parser = subparsers.add_parser("dwh", help="show a list of dwh snapshots")
    dwh_parser.set_defaults(func=dwh)
    dwh_sub_parsers = dwh_parser.add_subparsers(dest="subcommand")
    dwh_delete_parser = dwh_sub_parsers.add_parser(
        "delete",
        help="deletes a snapshot given an id. use 'controller {url} dwh' "
        "to get list of snapshot ids",
    )
    dwh_delete_parser.add_argument(
        "--snapshot-id",
        "-si",
        required=True,
        help=f"the id of the snapshot in the format "
        "dwh/controller_DEV_DWH_TIMESTAMP_2025_08_14T17_47_15_357080Z",
    )
    dwh_delete_parser.set_defaults(func=dwh)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
