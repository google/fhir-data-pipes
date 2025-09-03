import contextlib
import io
import sys
import unittest
from unittest import mock

import requests_mock

from src.main import main


class TestControllerCLI(unittest.TestCase):

    TEST_URL = "http://localhost:9004"

    @mock.patch.object(sys, "argv", ["controller", TEST_URL, "config"])
    @requests_mock.Mocker()
    def test_command_config(self, m):
        m.get(f"{self.TEST_URL}/config", json={"message": "Mocked response data"})
        with io.StringIO() as stdout, contextlib.redirect_stdout(stdout):
            main()
            output = stdout.getvalue().strip()
        self.assertIn("Command: config", output)
        self.assertIn(f"Request url: {TestControllerCLI.TEST_URL}", output)

    @mock.patch.object(sys, "argv", ["controller", TEST_URL, "next"])
    @requests_mock.Mocker()
    def test_command_next(self, m):
        m.get(f"{self.TEST_URL}/next", json={"message": "Mocked response data"})
        with io.StringIO() as stdout, contextlib.redirect_stdout(stdout):
            main()
            output = stdout.getvalue().strip()
        self.assertIn("Command: next", output)
        self.assertIn(f"Request url: {TestControllerCLI.TEST_URL}", output)

    @mock.patch.object(sys, "argv", ["controller", TEST_URL, "status"])
    @requests_mock.Mocker()
    def test_command_status(self, m):
        m.get(f"{self.TEST_URL}/status", json={"message": "Mocked response data"})
        with io.StringIO() as stdout, contextlib.redirect_stdout(stdout):
            main()
            output = stdout.getvalue().strip()
        self.assertIn("Command: status", output)
        self.assertIn(f"Request url: {TestControllerCLI.TEST_URL}", output)

    @mock.patch.object(
        sys, "argv", ["controller", TEST_URL, "run", "--mode", "incremental"]
    )
    @requests_mock.Mocker()
    def test_command_run(self, m):
        m.post(f"{self.TEST_URL}/run", json={"message": "Mocked response data"})
        with io.StringIO() as stdout, contextlib.redirect_stdout(stdout):
            main()
            output = stdout.getvalue().strip()
        self.assertIn("Command: run", output)
        self.assertIn(f"Request url: {TestControllerCLI.TEST_URL}", output)

    @mock.patch.object(sys, "argv", ["controller", TEST_URL, "tables"])
    @requests_mock.Mocker()
    def test_command_tables(self, m):
        m.post(f"{self.TEST_URL}/tables", json={"message": "Mocked response data"})
        with io.StringIO() as stdout, contextlib.redirect_stdout(stdout):
            main()
            output = stdout.getvalue().strip()
        self.assertIn("Command: tables", output)
        self.assertIn(f"Request url: {TestControllerCLI.TEST_URL}", output)

    @mock.patch.object(sys, "argv", ["controller", TEST_URL, "logs", "--download"])
    @requests_mock.Mocker()
    def test_command_logs(self, m):
        m.get(
            f"{self.TEST_URL}/download-error-log",
            json={"message": "Mocked response data"},
        )
        with io.StringIO() as stdout, contextlib.redirect_stdout(stdout):
            main()
            output = stdout.getvalue().strip()
        self.assertIn("Command: logs", output)
        self.assertIn(f"Request url: {TestControllerCLI.TEST_URL}", output)

    @mock.patch.object(sys, "argv", ["controller", TEST_URL, "dwh"])
    @requests_mock.Mocker()
    def test_command_dwh(self, m):
        m.get(f"{self.TEST_URL}/dwh", json={"message": "Mocked response data"})
        with io.StringIO() as stdout, contextlib.redirect_stdout(stdout):
            main()
            output = stdout.getvalue().strip()
        self.assertIn("Command: dwh", output)
        self.assertIn(f"Request url: {TestControllerCLI.TEST_URL}", output)

    @mock.patch.object(
        sys,
        "argv",
        [
            "controller",
            TEST_URL,
            "dwh",
            "delete",
            "-si",
            "dwh/controller_DEV_DWH_TIMESTAMP_2025_08_14T17_47_15_357080Z",
        ],
    )
    @requests_mock.Mocker()
    def test_command_dwh_delete(self, m):
        m.delete(f"{self.TEST_URL}/dwh", json={"message": "Mocked response data"})
        with io.StringIO() as stdout, contextlib.redirect_stdout(stdout):
            main()
            output = stdout.getvalue().strip()
        self.assertIn("Command: dwh delete", output)
        self.assertIn(f"Request url: {TestControllerCLI.TEST_URL}", output)
