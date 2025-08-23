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

API_URL = "http://localhost:9004"


def _make_api_request(
    verb: str, url: str, params: Optional[Dict[str, Any]] = None
) -> Optional[Dict[str, Any]]:
    try:
        if verb == "POST":
            response = requests.post(url, json={}, timeout=5)
        else:
            response = requests.get(url, params=params, timeout=5)

        response.raise_for_status()

        if response.headers.get("Content-Type", "").startswith("application/json"):
            data = response.json()
        else:
            print("Response is not JSON:")
            data = response.text
        return data
    except requests.exceptions.RequestException as req_err:
        logging.error(f"An error occurred during the request: {req_err}")
    except json.JSONDecodeError as json_err:
        logging.error(f"Failed to decode JSON response: {json_err}")
        print(response)
        return None


def download_file(url: str, filename: str) -> str:
    """
    Downloads a file from a URL to a local file.
    """
    try:
        with requests.get(url, stream=True) as r:
            with open(filename, "wb") as f:
                shutil.copyfileobj(r.raw, f)
        return f"File downloaded successfully to {filename}"
    except requests.exceptions.RequestException as e:
        return f"Error downloading file: {e}"


def delete_snapshot(url: str) -> str:
    try:
        requests.delete(url)
        return f"Snapshot deleted successfully"
    except requests.exceptions.RequestException as e:
        return f"Error deleting snapshot: {e}"


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="The CLI tool for fhir-data-pipes",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    parser.add_argument(
        "url",
        type=str,
        help="url of the REST API",
    )

    parser.add_argument(
        "command",
        type=str,
        help="pass the specific command, options are dwh, delete-snapshot, next,",
    )

    parser.add_argument(
        "--snapshot-id",
        "-sid",
        type=str,
        required=False,
        help="the id of the DWH snapshot to delete e.g."
        " dwh/controller_DEV_DWH_TIMESTAMP_2025_08_14T17_47_15_357080Z",
    )

    parser.add_argument(
        "--config-name",
        "-cn",
        type=str,
        required=False,
        help="name of the configuration key. Used with 'config' argument",
    )

    parser.add_argument(
        "--run-mode",
        "-rm",
        type=str,
        required=False,
        help="the runType argument, options are full, incremental, views. "
        "Used with 'run' argument",
    )

    args = parser.parse_args()
    logging.info(f"Running pipeline controller command: {args.command}")

    if args.command == "dwh":
        endpoint = "/dwh"
    elif args.command == "delete-snapshot":
        endpoint = "/dwh"
        if not args.snapshot_id:
            raise Exception(
                f"The snapshot id argument is required and should be "
                "supplied with '--snapshot-id "
                "dwh/controller_DEV_DWH_TIMESTAMP_2025_08_14 T12_26_31_956581Z'"
            )
    elif args.command == "config":
        endpoint = "/config"
    elif args.command == "next":
        endpoint = "/next"
    elif args.command == "status":
        endpoint = "/status"
    elif args.command == "download-error-log":
        endpoint = "/download-error-log"
    elif args.command == "tables":
        endpoint = "/tables"
    elif args.command == "run":
        endpoint = "/run"
        if not args.run_mode:
            raise Exception(
                f"The run mode argument is required and should be supplied "
                " with --run-mode, options are one of full, incremental, views"
            )
    else:
        logging.error(f"Invalid config: {args.command}")

        exit(1)

    params = {}
    active_base_url = args.url if args.url else API_URL

    if args.command == "config" and args.config_name:
        url = f"{active_base_url}{endpoint}/{args.config_name}"
    elif args.command == "delete-snapshot" and args.snapshot_id:
        url = f"{active_base_url}/dwh?snapshotId={args.snapshot_id}"
    elif args.command == "run" and args.run_mode:
        url = f"{active_base_url}/run?runMode={args.run_mode.upper()}"
    else:
        url = f"{active_base_url}{endpoint}"

    if args.command == "download-error-log":
        response = download_file(url, "download-error-log.log")
    elif args.command == "delete-snapshot":
        response = delete_snapshot(url)
    elif args.command == "tables" or args.command == "run":
        response = _make_api_request("POST", url, params)
    else:
        response = _make_api_request("GET", url, params)

    print(f"Command: {args.command}")
    print(f"Parameters: {params}")
    print(f"Request url: {url}")
    print("Response:")
    try:
        print(json.dumps(response, indent=4))
    except json.JSONDecodeError as json_err:
        print(response)


if __name__ == "__main__":
    main()
