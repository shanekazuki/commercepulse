"""Command-line entry point for the local CommercePulse ETL pipeline."""

from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path
from typing import Any

from commercepulse.pipeline import run_pipeline


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def _read_mysql() -> list[dict[str, Any]]:
    try:
        import pymysql
        from pymysql.cursors import DictCursor
    except ImportError as exc:
        raise RuntimeError(
            "MySQL source needs PyMySQL. Install it with: "
            "python -m pip install '.[mysql]'"
        ) from exc

    required = ("MYSQL_HOST", "MYSQL_DATABASE", "MYSQL_USER", "MYSQL_PASSWORD")
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        raise RuntimeError("Missing MySQL environment variables: " + ", ".join(missing))

    connection = pymysql.connect(
        host=os.environ["MYSQL_HOST"],
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        database=os.environ["MYSQL_DATABASE"],
        user=os.environ["MYSQL_USER"],
        password=os.environ["MYSQL_PASSWORD"],
        cursorclass=DictCursor,
        connect_timeout=10,
        read_timeout=30,
        write_timeout=30,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT order_item_id, order_id, order_date, customer_id, product_id,
                       product_name, category, quantity, unit_price, discount_amount,
                       promised_delivery_date, delivered_date, status
                FROM order_items
                ORDER BY order_item_id
                """
            )
            return list(cursor.fetchall())
    finally:
        connection.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="commercepulse",
        description="Run the retail analytics pipeline or local dashboard.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="Run the local Python ETL pipeline")
    run.add_argument("--source", choices=("csv", "mysql"), default="csv")
    run.add_argument("--input", type=Path, help="Input CSV path (required for CSV source)")
    run.add_argument("--output", type=Path, default=Path("data/lakehouse"))
    serve = subparsers.add_parser("serve", help="Start the local analytics dashboard")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8000")))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "serve":
        from commercepulse.web import serve

        serve(args.host, args.port)
        return 0

    try:
        if args.source == "csv":
            if args.input is None:
                raise RuntimeError("--input is required when --source=csv")
            records = _read_csv(args.input)
        else:
            records = _read_mysql()

        report = run_pipeline(records, args.output, source=args.source)
        print(
            "Pipeline complete: "
            f"{report['accepted_rows']} accepted, "
            f"{report['rejected_rows']} rejected, "
            f"{report['duplicate_rows']} duplicates."
        )
        print(f"Output: {args.output.resolve()}")
        return 0
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"commercepulse: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
