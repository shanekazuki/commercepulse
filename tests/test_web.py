import csv
import json
import tempfile
import threading
import unittest
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from commercepulse.web import PROJECT_ROOT, create_server


class DashboardApiTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_dir = Path(self.temp_dir.name) / "lakehouse"
        self.server = create_server(
            "127.0.0.1",
            0,
            project_root=PROJECT_ROOT,
            output_dir=self.output_dir,
            allow_mysql=False,
        )
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.temp_dir.cleanup()

    def get_json(self, path):
        with urlopen(self.base_url + path) as response:
            return response.status, json.loads(response.read())

    def test_dashboard_starts_with_empty_marts(self):
        status, payload = self.get_json("/api/dashboard")
        self.assertEqual(status, 200)
        self.assertEqual(payload["kpis"]["orders"], 0)
        self.assertIsNone(payload["quality"])
        self.assertFalse(payload["features"]["mysql"])

    def test_run_endpoint_builds_data_visible_to_dashboard(self):
        request = Request(
            self.base_url + "/api/run",
            data=b'{"source":"csv"}',
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urlopen(request) as response:
            result = json.loads(response.read())

        self.assertEqual(result["report"]["accepted_rows"], 15)
        self.assertEqual(result["dashboard"]["kpis"]["orders"], 14)
        with (self.output_dir / "gold/daily_sales.csv").open(
            encoding="utf-8", newline=""
        ) as stream:
            self.assertEqual(len(list(csv.DictReader(stream))), 13)

    def test_invalid_source_is_rejected(self):
        request = Request(
            self.base_url + "/api/run",
            data=b'{"source":"unknown"}',
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with self.assertRaises(HTTPError) as raised:
            urlopen(request)
        self.assertEqual(raised.exception.code, 400)
        payload = json.loads(raised.exception.read())
        self.assertIn("csv or mysql", payload["error"])

    def test_mysql_is_disabled_when_configured_for_public_demo(self):
        request = Request(
            self.base_url + "/api/run",
            data=b'{"source":"mysql"}',
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with self.assertRaises(HTTPError) as raised:
            urlopen(request)
        self.assertEqual(raised.exception.code, 403)

    def test_dashboard_page_is_served(self):
        with urlopen(self.base_url + "/") as response:
            page = response.read().decode("utf-8")
        self.assertEqual(response.status, 200)
        self.assertIn("Retail overview", page)


if __name__ == "__main__":
    unittest.main()
