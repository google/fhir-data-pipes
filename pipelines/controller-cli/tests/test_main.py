import contextlib
import io
import sys
import unittest
from unittest import mock

from src.main import main, status


class TestControllerCLI(unittest.TestCase):

    TEST_URL = "http://localhost:9004"

    @mock.patch.object(sys, "argv", ["controller", TEST_URL, "config"])
    def test_command_config(self):
        with io.StringIO() as stdout, contextlib.redirect_stdout(stdout):
            main()
            output = stdout.getvalue().strip()
        self.assertIn("Command: config", output)
        self.assertIn(f"Request url: {TestControllerCLI.TEST_URL}", output)

    @mock.patch.object(sys, "argv", ["controller", TEST_URL, "next"])
    def test_command_next(self):
        with io.StringIO() as stdout, contextlib.redirect_stdout(stdout):
            main()
            output = stdout.getvalue().strip()
        self.assertIn("Command: next", output)
        self.assertIn(f"Request url: {TestControllerCLI.TEST_URL}", output)

    @mock.patch.object(sys, "argv", ["controller", TEST_URL, "status"])
    def test_command_status(self):
        with io.StringIO() as stdout, contextlib.redirect_stdout(stdout):
            main()
            output = stdout.getvalue().strip()
        self.assertIn("Command: status", output)
        self.assertIn(f"Request url: {TestControllerCLI.TEST_URL}", output)

    @mock.patch.object(
        sys, "argv", ["controller", TEST_URL, "run", "--mode", "incremental"]
    )
    def test_command_run(self):
        with io.StringIO() as stdout, contextlib.redirect_stdout(stdout):
            main()
            output = stdout.getvalue().strip()
        self.assertIn("Command: run", output)
        self.assertIn(f"Request url: {TestControllerCLI.TEST_URL}", output)

    @mock.patch.object(sys, "argv", ["controller", TEST_URL, "tables"])
    def test_command_tables(self):
        with io.StringIO() as stdout, contextlib.redirect_stdout(stdout):
            main()
            output = stdout.getvalue().strip()
        self.assertIn("Command: tables", output)
        self.assertIn(f"Request url: {TestControllerCLI.TEST_URL}", output)

    @mock.patch.object(sys, "argv", ["controller", TEST_URL, "logs", "--download"])
    def test_command_logs(self):
        with io.StringIO() as stdout, contextlib.redirect_stdout(stdout):
            main()
            output = stdout.getvalue().strip()
        self.assertIn("Command: logs", output)
        self.assertIn(f"Request url: {TestControllerCLI.TEST_URL}", output)

    @mock.patch.object(sys, "argv", ["controller", TEST_URL, "dwh"])
    def test_command_dwh(self):
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
    def test_command_dwh_delete(self):
        with io.StringIO() as stdout, contextlib.redirect_stdout(stdout):
            main()
            output = stdout.getvalue().strip()
        self.assertIn("Command: dwh delete", output)
        self.assertIn(f"Request url: {TestControllerCLI.TEST_URL}", output)
