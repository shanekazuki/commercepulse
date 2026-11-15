"""Local web dashboard and JSON API for CommercePulse."""

from __future__ import annotations

import csv
import json
import os
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from commercepulse.pipeline import run_pipeline

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE_CSV = PROJECT_ROOT / "data" / "sample" / "orders.csv"
OUTPUT_DIR = PROJECT_ROOT / "data" / "lakehouse"
WEB_DIR = PROJECT_ROOT / "web"
MART_FILES = {
    "daily_sales": "daily_sales.csv",
    "product_performance": "product_performance.csv",
    "customer_repeat_purchase": "customer_repeat_purchase.csv",
    "delivery_performance": "delivery_performance.csv",
}
MAX_REQUEST_BYTES = 4096


def _read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def _number(value: str | int | float) -> float:
    return float(value)


def load_dashboard_data(output_dir: Path) -> dict[str, Any]:
    """Read the generated Gold marts and quality report for the UI."""
    tables = {
        name: _read_csv(Path(output_dir) / "gold" / filename)
        for name, filename in MART_FILES.items()
    }
    daily = tables["daily_sales"]
    products = tables["product_performance"]
    customers = tables["customer_repeat_purchase"]
    deliveries = tables["delivery_performance"]

    revenue = sum(_number(row["net_revenue"]) for row in daily)
    order_count = sum(int(row["order_count"]) for row in daily)
    delivered = sum(int(row["delivered_order_items"]) for row in deliveries)
    on_time = sum(int(row["on_time_order_items"]) for row in deliveries)
    repeat_customers = sum(
        row["is_repeat_customer"].lower() == "true" for row in customers
    )
    report_path = Path(output_dir) / "quality" / "validation_report.json"
    quality = None
    if report_path.exists():
        quality = json.loads(report_path.read_text(encoding="utf-8"))

    return {
        "kpis": {
            "revenue": round(revenue, 2),
            "orders": order_count,
            "average_order_value": round(revenue / order_count, 2) if order_count else 0,
            "repeat_customer_rate": (
                round(100 * repeat_customers / len(customers), 1) if customers else 0
            ),
            "on_time_rate": round(100 * on_time / delivered, 1) if delivered else 0,
        },
        "tables": tables,
        "quality": quality,
    }


def create_server(
    host: str = "127.0.0.1",
    port: int = 8000,
    *,
    project_root: Path = PROJECT_ROOT,
    output_dir: Path = OUTPUT_DIR,
    allow_mysql: bool | None = None,
) -> ThreadingHTTPServer:
    project_root = Path(project_root)
    output_dir = Path(output_dir)
    web_dir = project_root / "web"
    run_lock = threading.Lock()
    if allow_mysql is None:
        allow_mysql = os.environ.get("COMMERCEPULSE_ALLOW_MYSQL", "true").lower() in {
            "1",
            "true",
            "yes",
        }

    class CommercePulseHandler(BaseHTTPRequestHandler):
        def _send_json(self, status: HTTPStatus, body: dict[str, Any]) -> None:
            encoded = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(encoded)

        def _send_file(self, file_path: Path, content_type: str) -> None:
            try:
                body = file_path.read_bytes()
            except OSError:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "Page asset not found"})
                return
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            path = urlparse(self.path).path
            if path == "/api/dashboard":
                try:
                    data = load_dashboard_data(output_dir)
                    data["features"] = {"mysql": allow_mysql}
                    self._send_json(HTTPStatus.OK, data)
                except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
                    self._send_json(
                        HTTPStatus.INTERNAL_SERVER_ERROR,
                        {"error": f"Could not load analytics: {exc}"},
                    )
                return

            assets = {
                "/": ("index.html", "text/html; charset=utf-8"),
                "/styles.css": ("styles.css", "text/css; charset=utf-8"),
                "/app.js": ("app.js", "text/javascript; charset=utf-8"),
            }
            asset = assets.get(path)
            if asset is None:
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})
                return
            self._send_file(web_dir / asset[0], asset[1])

        def do_POST(self) -> None:
            if urlparse(self.path).path != "/api/run":
                self._send_json(HTTPStatus.NOT_FOUND, {"error": "Not found"})
                return
            if not run_lock.acquire(blocking=False):
                self._send_json(
                    HTTPStatus.CONFLICT, {"error": "A pipeline run is already in progress"}
                )
                return

            try:
                try:
                    content_length = int(self.headers.get("Content-Length", "0"))
                except ValueError:
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Invalid request length"})
                    return
                if content_length < 0 or content_length > MAX_REQUEST_BYTES:
                    self._send_json(
                        HTTPStatus.BAD_REQUEST, {"error": "Request body is too large"}
                    )
                    return
                try:
                    request = json.loads(self.rfile.read(content_length) or b"{}")
                except (json.JSONDecodeError, UnicodeDecodeError):
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Invalid JSON body"})
                    return
                if not isinstance(request, dict):
                    self._send_json(HTTPStatus.BAD_REQUEST, {"error": "Expected a JSON object"})
                    return

                source = request.get("source", "csv")
                if source not in ("csv", "mysql"):
                    self._send_json(
                        HTTPStatus.BAD_REQUEST, {"error": "Source must be csv or mysql"}
                    )
                    return
                if source == "mysql" and not allow_mysql:
                    self._send_json(
                        HTTPStatus.FORBIDDEN,
                        {"error": "MySQL is disabled on this public deployment"},
                    )
                    return
                if source == "csv":
                    with SAMPLE_CSV.open("r", encoding="utf-8-sig", newline="") as stream:
                        records = list(csv.DictReader(stream))
                else:
                    from commercepulse.cli import _read_mysql

                    records = _read_mysql()

                report = run_pipeline(records, output_dir, source=source)
                dashboard = load_dashboard_data(output_dir)
                dashboard["features"] = {"mysql": allow_mysql}
                self._send_json(
                    HTTPStatus.OK,
                    {"report": report, "dashboard": dashboard},
                )
            except (OSError, RuntimeError, ValueError, KeyError) as exc:
                self._send_json(
                    HTTPStatus.INTERNAL_SERVER_ERROR,
                    {"error": f"Pipeline run failed: {exc}"},
                )
            finally:
                run_lock.release()

        def log_message(self, format: str, *args: Any) -> None:
            print(f"[CommercePulse] {self.address_string()} - {format % args}")

    server = ThreadingHTTPServer((host, port), CommercePulseHandler)
    server.daemon_threads = True
    return server


def serve(host: str = "127.0.0.1", port: int = 8000) -> None:
    report_path = OUTPUT_DIR / "quality" / "validation_report.json"
    if not report_path.exists():
        with SAMPLE_CSV.open("r", encoding="utf-8-sig", newline="") as stream:
            records = list(csv.DictReader(stream))
        report = run_pipeline(records, OUTPUT_DIR, source="csv")
        print(f"Loaded sample dataset: {report['accepted_rows']} rows accepted.")

    server = create_server(host, port)
    print(f"CommercePulse dashboard: http://{host}:{server.server_port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping CommercePulse dashboard.")
    finally:
        server.server_close()


def main() -> None:
    serve()


if __name__ == "__main__":
    main()
