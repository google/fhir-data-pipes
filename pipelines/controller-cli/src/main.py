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
import sys
from typing import Any, Dict, Optional

import requests

HTTP_GET = "GET"
HTTP_POST = "POST"

RUN_MODES = ["incremental", "full", "views"]

COMMAND_LIST = ["dwh", "next", "status", "run", "config", "logs", "tables"]

logger = logging.getLogger(__name__)


def process_response(response: str, args: argparse.Namespace):
    command_str = (
        f"{args.command} "
        f"""{args.subcommand if hasattr(args, "subcommand") else ""}""".strip()
    )
    logger.info("Command: %s", command_str)
    logger.info("Request url: %s", args.url)
    logger.info("Response:")
    try:
        if isinstance(response, dict):
            logger.info(json.dumps(response, indent=4))
        else:
            logger.info(response)
    except TypeError:
        logger.info(response)


def _make_api_request(
    verb: str, url: str, params: Optional[Dict[str, Any]] = None
) -> str:
    logger.debug("Making API request: %s %s", verb, url)
    logger.debug("Request parameters: %s", params)
    try:
        headers = {"Content-Type": "application/json", "Accept": "*/*"}
        logger.debug("Request headers: %s", headers)

        if verb == HTTP_POST:
            logger.debug("Executing POST request with empty JSON body")
            response = requests.post(
                url, json={}, params=params, headers=headers, timeout=5
            )
        else:
            logger.debug("Executing GET request")
            response = requests.get(url, params=params, headers=headers, timeout=5)

        logger.info("Response status code: %s", response.status_code)
        logger.debug("Response headers: %s", dict(response.headers))

        if not response.ok:
            logger.error("HTTP error response received: %s", response.status_code)
            logger.debug("Response headers: %s", dict(response.headers))

            try:
                error_content_type = response.headers.get("Content-Type", "")
                if error_content_type.startswith("application/json"):
                    error_data = response.json()
                    logger.error("Error response body (JSON): ")
                    logger.error("%s", json.dumps(error_data, indent=2))
                else:
                    error_text = response.text
                    logger.error("Error response body (text): %s", error_text)
            except json.JSONDecodeError as parse_err:
                logger.error("Could not parse error response body: %s", parse_err)
                logger.error("Raw error response: %s", response.content)

            response.raise_for_status()

        logger.debug("Status check passed")

        content_type = response.headers.get("Content-Type", "")

        if content_type.startswith("application/json"):
            data = response.json()
            logger.debug("Parsed JSON response successfully")
        else:
            data = response.text
            logger.debug("Response is plain text, length: %s", len(data))

        logger.debug("API request completed successfully")
        return data
    except requests.exceptions.HTTPError as http_err:
        logger.error("HTTP error occurred: %s", http_err)
    except requests.exceptions.RequestException as req_err:
        logger.error("An error occurred during the request: %s", req_err)
    except json.JSONDecodeError as json_err:
        logger.error("Failed to decode JSON response: %s", json_err)
    return "API REQUEST FAILED"


def config(args: argparse.Namespace) -> None:
    try:
        if args.config_name:
            response = _make_api_request(
                HTTP_GET, f"{args.url}/config/{args.config_name}"
            )
        else:
            response = _make_api_request(HTTP_GET, f"{args.url}/config")
        process_response(response, args)
    except requests.exceptions.RequestException as e:
        logger.error("Error processing: %s", e)


def next_scheduled(args: argparse.Namespace) -> None:
    try:
        response = _make_api_request(HTTP_GET, f"{args.url}/next")
        process_response(response, args)
    except requests.exceptions.RequestException as e:
        logger.error("Error processing: %s", e)


def status(args: argparse.Namespace) -> None:
    try:
        response = _make_api_request(HTTP_GET, f"{args.url}/status")
        process_response(response, args)
    except requests.exceptions.RequestException as e:
        logger.error("Error processing: %s", e)


def run(args: argparse.Namespace) -> None:
    logger.info("=" * 50)
    logger.info("Executing 'run' command - starting pipeline run")
    logger.info("Run mode: %s", args.mode)
    logger.info("Target URL: %s", args.url)
    logger.info("=" * 50)

    try:
        params = {"runMode": args.mode.upper()}
        logger.debug("Request parameters: %s", params)
        logger.debug("Initiating pipeline run with mode: %s", args.mode.upper())

        response = _make_api_request(HTTP_POST, f"{args.url}/run", params=params)

        if response:
            logger.info("Pipeline run request successful")
            logger.debug("Response data: %s", response)
        else:
            logger.warning("Pipeline run request returned no response")

        process_response(response, args)
        logger.info("Run command completed successfully")
        logger.info("=" * 50)
    except requests.exceptions.RequestException as e:
        logger.error("=" * 50)
        logger.error("Error in run command: %s", e)
        logger.error("Failed to execute pipeline run with mode: %s", args.mode)
        logger.error("=" * 50)


def tables(args: argparse.Namespace) -> None:
    try:
        response = _make_api_request(HTTP_POST, f"{args.url}/tables")
        process_response(response, args)
    except requests.exceptions.RequestException as e:
        logger.error("Error processing: %s", e)


def download_file(url: str, filename: str) -> str:
    try:
        with requests.get(url, stream=True, timeout=10) as r:
            with open(filename, "wb") as f:
                shutil.copyfileobj(r.raw, f)
        return f"File downloaded successfully to {filename}"
    except requests.exceptions.RequestException as e:
        return f"Error downloading file: {e}"


def logs(args: argparse.Namespace) -> None:
    try:
        if args.download:
            filename = args.filename if args.filename else "error.log"
            response: str = download_file(f"{args.url}/download-error-log", filename)
            process_response(response, args)
        else:
            logger.info(
                "You can use the tail command to watch the {DWH_ROOT}/error.log file."
            )
    except requests.exceptions.RequestException as e:
        logger.error("Error processing: %s", e)


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


def dwh(args: argparse.Namespace) -> None:
    try:
        if hasattr(args, "snapshot_id"):
            response = delete_snapshot(args)
        else:
            response = _make_api_request(HTTP_GET, f"{args.url}/dwh")
        process_response(response, args)
    except requests.exceptions.RequestException as e:
        logger.error("Error processing: %s", e)


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

    handler = logging.StreamHandler(sys.stdout)
    if args.verbose:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        logger.setLevel(logging.DEBUG)
    else:
        formatter = logging.Formatter("%(message)s")
        logger.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    args.func(args)


if __name__ == "__main__":
    main()
