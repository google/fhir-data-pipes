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

RUN_MODES = ["incremental", "full", "views"]

COMMAND_LIST = ["dwh", "next", "status", "run", "config", "logs", "tables"]


def process_response(response: str, args: argparse.Namespace):
    command_str = (
        f"{args.command} "
        f"{args.subcommand if hasattr(args, 'subcommand') else ''}".strip()
    )
    logging.info(f"Command: {command_str}")
    logging.info(f"Request url: {args.url}")
    logging.info("Response:")
    try:
        logging.info(json.dumps(response, indent=4))
    except json.JSONDecodeError:
        logging.info(response)


def _make_api_request(
    verb: str, url: str, params: Optional[Dict[str, Any]] = None
) -> Optional[Dict[str, Any]]:
    logging.debug(f"Making API request: {verb} {url}")
    logging.debug(f"Request parameters: {params}")
    try:
        headers = {"Content-Type": "application/json", "Accept": "*/*"}
        logging.debug(f"Request headers: {headers}")

        if verb == HTTP_POST:
            logging.debug("Executing POST request with empty JSON body")
            response = requests.post(
                url, json={}, params=params, headers=headers, timeout=5
            )
        else:
            logging.debug("Executing GET request")
            response = requests.get(url, params=params, headers=headers, timeout=5)

        logging.info(f"Response status code: {response.status_code}")
        logging.debug(f"Response headers: {dict(response.headers)}")

        if not response.ok:
            logging.error(f"HTTP error response received: {response.status_code}")
            logging.debug(f"Response headers: {dict(response.headers)}")

            try:
                error_content_type = response.headers.get("Content-Type", "")
                if error_content_type.startswith("application/json"):
                    error_data = response.json()
                    logging.error("Error response body (JSON): ")
                    logging.error(f"{json.dumps(error_data, indent=2)}")
                else:
                    error_text = response.text
                    logging.error(f"Error response body (text): {error_text}")
            except json.JSONDecodeError as parse_err:
                logging.error(f"Could not parse error response body: {parse_err}")
                logging.error(f"Raw error response: {response.content}")

            response.raise_for_status()

        logging.debug("Status check passed")

        content_type = response.headers.get("Content-Type", "")

        if content_type.startswith("application/json"):
            data = response.json()
            logging.debug("Parsed JSON response successfully")
        else:
            data = response.text
            logging.debug(f"Response is plain text, length: {len(data)}")

        logging.debug("API request completed successfully")
        return data
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"An error occurred during the request: {req_err}")
    except json.JSONDecodeError as json_err:
        logging.error(f"Failed to decode JSON response: {json_err}")
        logging.error(response)
    return None


def config(args: argparse.Namespace) -> str:
    try:
        if args.config_name:
            response = _make_api_request(
                HTTP_GET, f"{args.url}/config/{args.config_name}"
            )
        else:
            response = _make_api_request(HTTP_GET, f"{args.url}/config")
        process_response(response, args)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error processing: {e}")


def next_scheduled(args: argparse.Namespace) -> str:
    try:
        response = _make_api_request(HTTP_GET, f"{args.url}/next")
        process_response(response, args)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error processing: {e}")


def status(args: argparse.Namespace) -> str:
    try:
        response = _make_api_request(HTTP_GET, f"{args.url}/status")
        process_response(response, args)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error processing: {e}")


def run(args: argparse.Namespace) -> str:
    logging.info("=" * 50)
    logging.info("Executing 'run' command - starting pipeline run")
    logging.info(f"Run mode: {args.mode}")
    logging.info(f"Target URL: {args.url}")
    logging.info("=" * 50)

    try:
        params = {"runMode": args.mode.upper()}
        logging.debug(f"Request parameters: {params}")
        logging.debug(f"Initiating pipeline run with mode: {args.mode.upper()}")

        response = _make_api_request(HTTP_POST, f"{args.url}/run", params=params)

        if response:
            logging.info("Pipeline run request successful")
            logging.debug(f"Response data: {response}")
        else:
            logging.warning("Pipeline run request returned no response")

        process_response(response, args)
        logging.info("Run command completed successfully")
        logging.info("=" * 50)
    except requests.exceptions.RequestException as e:
        logging.error("=" * 50)
        logging.error(f"Error in run command: {e}")
        logging.error(f"Failed to execute pipeline run with mode: {args.mode}")
        logging.error("=" * 50)
        logging.error(f"Error processing: {e}")


def tables(args: argparse.Namespace) -> str:
    try:
        response = _make_api_request(HTTP_POST, f"{args.url}/tables")
        process_response(response, args)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error processing: {e}")


def download_file(url: str, filename: str) -> str:
    try:
        with requests.get(url, stream=True, timeout=10) as r:
            with open(filename, "wb") as f:
                shutil.copyfileobj(r.raw, f)
        return f"File downloaded successfully to {filename}"
    except requests.exceptions.RequestException as e:
        return f"Error downloading file: {e}"


def logs(args: argparse.Namespace) -> str:
    try:
        if args.download:
            filename = args.filename if args.filename else "error.log"
            response = download_file(f"{args.url}/download-error-log", filename)
            process_response(response, args)
        else:
            logging.info(
                "You can use the tail command to watch the {DWH_ROOT}/error.log file."
            )
    except requests.exceptions.RequestException as e:
        logging.error(f"Error processing: {e}")


def delete_snapshot(args: argparse.Namespace) -> str:
    try:
        response = requests.delete(
            f"{args.url}/dwh?snapshotId={args.snapshot_id}", timeout=10
        )
        if response.status_code == 204:
            return "Snapshot deleted successfully"
        return f"Error deleting snapshot: Status code {response.status_code}"
    except requests.exceptions.RequestException as e:
        return f"Error deleting snapshot: {e}"


def dwh(args: argparse.Namespace) -> str:
    try:
        if hasattr(args, "snapshot_id"):
            response = delete_snapshot(args)
        else:
            response = _make_api_request(HTTP_GET, f"{args.url}/dwh")
        process_response(response, args)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error processing: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="The CLI tool for fhir-data-pipes",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "url",
        type=str,
        help="url of the pipeline controller's REST API",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose output"
    )
    subparsers = parser.add_subparsers(
        dest="command",
        help=f'{", ".join(mode for mode in COMMAND_LIST)}'
        " are the available commands.",
        required=True,
    )

    config_parser = subparsers.add_parser("config", help="show config values")
    config_parser.add_argument(
        "--config-name", "-cn", required=False, help="name of the configuration key"
    )
    config_parser.set_defaults(func=config)

    next_parser = subparsers.add_parser("next", help="show the next scheduled run")
    next_parser.set_defaults(func=next_scheduled)

    status_parser = subparsers.add_parser(
        "status", help="show the status of the pipeline"
    )
    status_parser.set_defaults(func=status)

    run_modes = ", ".join(mode for mode in RUN_MODES)
    run_parser = subparsers.add_parser("run", help="run the pipeline")
    run_parser.add_argument(
        "--mode",
        "-m",
        type=str.lower,
        choices=(RUN_MODES),
        required=True,
        help=f"the type of run; options are {run_modes}",
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
        help="the id of the snapshot in the format "
        "<dwhRootPrefix><DwhFiles.TIMESTAMP_PREFIX><timestampSuffix> "
        "e.g. dwh/controller_DEV_DWH_TIMESTAMP_2025_08_14T17_47_15_357080Z",
    )
    dwh_delete_parser.set_defaults(func=dwh)

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(
            level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
        )
    else:
        logging.basicConfig(level=logging.INFO, format="%(message)s")

    args.func(args)


if __name__ == "__main__":
    main()
