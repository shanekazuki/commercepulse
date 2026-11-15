import csv
import json
import tempfile
import unittest
from pathlib import Path

from commercepulse.pipeline import run_pipeline, transform_record


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SAMPLE = PROJECT_ROOT / "data" / "sample" / "orders.csv"


def sample_records():
    with SAMPLE.open(encoding="utf-8", newline="") as stream:
        return list(csv.DictReader(stream))


class TransformTests(unittest.TestCase):
    def test_revenue_and_delivery_are_calculated(self):
        row = transform_record(sample_records()[0])
        self.assertEqual(row["revenue"], "44.98")
        self.assertEqual(row["delivery_delay_days"], -1)
        self.assertEqual(row["is_on_time"], "true")

    def test_invalid_quantity_is_rejected(self):
        row = dict(sample_records()[0], quantity="0")
        with self.assertRaisesRegex(ValueError, "quantity"):
            transform_record(row)

    def test_discount_cannot_exceed_line_gross(self):
        row = dict(sample_records()[0], discount_amount="1000")
        with self.assertRaisesRegex(ValueError, "discount_amount"):
            transform_record(row)


class PipelineTests(unittest.TestCase):
    def test_end_to_end_metrics_and_quality_outputs(self):
        rows = sample_records()
        rows.append(dict(rows[0]))  # duplicate order-item ID
        rows.append(dict(rows[1], order_item_id="OI-BAD", quantity="-2"))

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            report = run_pipeline(rows, root, source="test")
            self.assertEqual(report["input_rows"], 17)
            self.assertEqual(report["accepted_rows"], 15)
            self.assertEqual(report["rejected_rows"], 1)
            self.assertEqual(report["duplicate_rows"], 1)

            with (root / "gold/daily_sales.csv").open(encoding="utf-8", newline="") as stream:
                daily = list(csv.DictReader(stream))
            self.assertEqual(len(daily), 13)
            self.assertEqual(daily[0]["order_date"], "2026-01-03")
            self.assertEqual(daily[0]["order_count"], "1")
            self.assertEqual(daily[0]["average_order_value"], "63.48")

            with (root / "gold/customer_repeat_purchase.csv").open(
                encoding="utf-8", newline=""
            ) as stream:
                customers = list(csv.DictReader(stream))
            repeat = next(row for row in customers if row["customer_id"] == "C-001")
            self.assertEqual(repeat["order_count"], "3")
            self.assertEqual(repeat["is_repeat_customer"], "true")

            saved_report = json.loads(
                (root / "quality/validation_report.json").read_text(encoding="utf-8")
            )
            self.assertEqual(saved_report["accepted_rows"], 15)
            self.assertTrue((root / "bronze/orders.jsonl").exists())
            self.assertTrue((root / "silver/orders.csv").exists())


if __name__ == "__main__":
    unittest.main()
