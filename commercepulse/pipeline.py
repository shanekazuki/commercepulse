"""Standard-library ETL implementation for the local portfolio demo."""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable

SILVER_FIELDS = (
    "order_item_id",
    "order_id",
    "order_date",
    "customer_id",
    "product_id",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "discount_amount",
    "revenue",
    "promised_delivery_date",
    "delivered_date",
    "status",
    "delivery_delay_days",
    "is_on_time",
)


def _money(value: Any, field: str) -> Decimal:
    try:
        result = Decimal(str(value).strip()).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(f"{field} must be a valid decimal")
    if not result.is_finite():
        raise ValueError(f"{field} must be finite")
    return result


def _iso_date(value: Any, field: str, *, optional: bool = False) -> str:
    text = "" if value is None else str(value).strip()
    if not text and optional:
        return ""
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        raise ValueError(f"{field} must be an ISO date (YYYY-MM-DD)")


def transform_record(record: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize one source order-item record."""
    for key in ("order_item_id", "order_id", "customer_id", "product_id"):
        if not str(record.get(key, "")).strip():
            raise ValueError(f"{key} is required")

    try:
        quantity = int(str(record.get("quantity", "")).strip())
    except (TypeError, ValueError):
        raise ValueError("quantity must be a whole number")
    if quantity <= 0:
        raise ValueError("quantity must be greater than zero")

    unit_price = _money(record.get("unit_price"), "unit_price")
    discount = _money(record.get("discount_amount", "0"), "discount_amount")
    gross = unit_price * quantity
    if unit_price < 0:
        raise ValueError("unit_price must not be negative")
    if discount < 0:
        raise ValueError("discount_amount must not be negative")
    if discount > gross:
        raise ValueError("discount_amount must not exceed line gross")

    order_date = _iso_date(record.get("order_date"), "order_date")
    promised = _iso_date(
        record.get("promised_delivery_date"), "promised_delivery_date", optional=True
    )
    delivered = _iso_date(record.get("delivered_date"), "delivered_date", optional=True)
    delay_days = ""
    on_time = ""
    if delivered and promised:
        delay_days = (date.fromisoformat(delivered) - date.fromisoformat(promised)).days
        on_time = str(delay_days <= 0).lower()

    return {
        "order_item_id": str(record["order_item_id"]).strip(),
        "order_id": str(record["order_id"]).strip(),
        "order_date": order_date,
        "customer_id": str(record["customer_id"]).strip(),
        "product_id": str(record["product_id"]).strip(),
        "product_name": str(record.get("product_name", "")).strip(),
        "category": str(record.get("category", "")).strip(),
        "quantity": quantity,
        "unit_price": f"{unit_price:.2f}",
        "discount_amount": f"{discount:.2f}",
        "revenue": f"{(gross - discount):.2f}",
        "promised_delivery_date": promised,
        "delivered_date": delivered,
        "status": str(record.get("status", "")).strip().lower(),
        "delivery_delay_days": delay_days,
        "is_on_time": on_time,
    }


def _write_csv(path: Path, fields: tuple[str, ...], rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def _gold_marts(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    daily: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"order_date": "", "revenue": Decimal("0"), "orders": set(), "units": 0}
    )
    products: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "product_id": "",
            "product_name": "",
            "category": "",
            "revenue": Decimal("0"),
            "units_sold": 0,
            "orders": set(),
        }
    )
    customers: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"customer_id": "", "orders": set(), "revenue": Decimal("0")}
    )
    deliveries: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "order_date": "",
            "delivered_order_items": 0,
            "on_time_order_items": 0,
            "late_order_items": 0,
            "total_delay_days": 0,
        }
    )

    for row in rows:
        amount = Decimal(row["revenue"])
        day = daily[row["order_date"]]
        day["order_date"] = row["order_date"]
        day["revenue"] += amount
        day["orders"].add(row["order_id"])
        day["units"] += row["quantity"]

        product = products[row["product_id"]]
        product["product_id"] = row["product_id"]
        product["product_name"] = row["product_name"]
        product["category"] = row["category"]
        product["revenue"] += amount
        product["units_sold"] += row["quantity"]
        product["orders"].add(row["order_id"])

        customer = customers[row["customer_id"]]
        customer["customer_id"] = row["customer_id"]
        customer["orders"].add(row["order_id"])
        customer["revenue"] += amount

        if row["delivery_delay_days"] != "":
            delivery = deliveries[row["order_date"]]
            delivery["order_date"] = row["order_date"]
            delivery["delivered_order_items"] += 1
            delivery["total_delay_days"] += int(row["delivery_delay_days"])
            if row["is_on_time"] == "true":
                delivery["on_time_order_items"] += 1
            else:
                delivery["late_order_items"] += 1

    daily_rows = []
    for day in sorted(daily):
        item = daily[day]
        count = len(item["orders"])
        daily_rows.append(
            {
                "order_date": day,
                "net_revenue": f"{item['revenue']:.2f}",
                "order_count": count,
                "average_order_value": f"{(item['revenue'] / count):.2f}",
                "units_sold": item["units"],
            }
        )

    product_rows = []
    for key in sorted(products):
        item = products[key]
        product_rows.append(
            {
                "product_id": item["product_id"],
                "product_name": item["product_name"],
                "category": item["category"],
                "net_revenue": f"{item['revenue']:.2f}",
                "units_sold": item["units_sold"],
                "order_count": len(item["orders"]),
            }
        )

    customer_rows = []
    for key in sorted(customers):
        item = customers[key]
        count = len(item["orders"])
        customer_rows.append(
            {
                "customer_id": item["customer_id"],
                "order_count": count,
                "is_repeat_customer": str(count > 1).lower(),
                "lifetime_revenue": f"{item['revenue']:.2f}",
            }
        )

    delivery_rows = []
    for key in sorted(deliveries):
        item = deliveries[key]
        count = item["delivered_order_items"]
        delivery_rows.append(
            {
                "order_date": key,
                "delivered_order_items": count,
                "on_time_order_items": item["on_time_order_items"],
                "late_order_items": item["late_order_items"],
                "on_time_rate_pct": f"{(100 * item['on_time_order_items'] / count):.2f}",
                "average_delay_days": f"{(item['total_delay_days'] / count):.2f}",
            }
        )

    return {
        "daily_sales": daily_rows,
        "product_performance": product_rows,
        "customer_repeat_purchase": customer_rows,
        "delivery_performance": delivery_rows,
    }


def run_pipeline(
    records: Iterable[dict[str, Any]], output_dir: Path, *, source: str = "csv"
) -> dict[str, Any]:
    """Run extract/land, validate/transform, and aggregate/load stages."""
    output_dir = Path(output_dir)
    bronze_dir = output_dir / "bronze"
    silver_dir = output_dir / "silver"
    gold_dir = output_dir / "gold"
    quality_dir = output_dir / "quality"
    for directory in (bronze_dir, silver_dir, gold_dir, quality_dir):
        directory.mkdir(parents=True, exist_ok=True)

    extracted_at = datetime.now(timezone.utc).isoformat()
    raw_rows = [
        {
            "_metadata": {"source": source, "extracted_at": extracted_at, "row_number": i},
            **record,
        }
        for i, record in enumerate(records, start=1)
    ]
    _write_jsonl(bronze_dir / "orders.jsonl", raw_rows)

    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    seen_item_ids: set[str] = set()
    duplicate_count = 0
    for raw in raw_rows:
        record = {key: value for key, value in raw.items() if key != "_metadata"}
        try:
            clean = transform_record(record)
            if clean["order_item_id"] in seen_item_ids:
                duplicate_count += 1
                continue
            seen_item_ids.add(clean["order_item_id"])
            accepted.append(clean)
        except ValueError as exc:
            rejected.append({"record": record, "reason": str(exc)})

    accepted.sort(key=lambda row: (row["order_date"], row["order_id"], row["order_item_id"]))
    _write_csv(silver_dir / "orders.csv", SILVER_FIELDS, accepted)
    _write_jsonl(quality_dir / "rejected_rows.jsonl", rejected)

    marts = _gold_marts(accepted)
    mart_fields = {
        "daily_sales": (
            "order_date", "net_revenue", "order_count", "average_order_value", "units_sold"
        ),
        "product_performance": (
            "product_id", "product_name", "category", "net_revenue", "units_sold", "order_count"
        ),
        "customer_repeat_purchase": (
            "customer_id", "order_count", "is_repeat_customer", "lifetime_revenue"
        ),
        "delivery_performance": (
            "order_date", "delivered_order_items", "on_time_order_items",
            "late_order_items", "on_time_rate_pct", "average_delay_days"
        ),
    }
    for name, rows in marts.items():
        _write_csv(gold_dir / f"{name}.csv", mart_fields[name], rows)

    report = {
        "source": source,
        "extracted_at": extracted_at,
        "input_rows": len(raw_rows),
        "accepted_rows": len(accepted),
        "rejected_rows": len(rejected),
        "duplicate_rows": duplicate_count,
        "silver_rows": len(accepted),
        "gold_marts": {name: len(rows) for name, rows in marts.items()},
    }
    (quality_dir / "validation_report.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    return report
