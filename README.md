# CommercePulse — Retail Sales Lakehouse

A portfolio-ready, local-first retail analytics pipeline that demonstrates **ETL** with Python and **ELT** with Databricks SQL/Delta. It can run from the included sample CSV without a database, cloud account, or credentials. A MySQL source and a Databricks workspace are optional integrations.

## What it demonstrates

- Extract order-line data from CSV or MySQL into a raw JSONL Bronze landing file.
- Validate, standardize, and deduplicate records into Silver; write a validation report and quarantined rows.
- Build Gold CSV marts for revenue/AOV, product performance, repeat customers, and delivery delays.
- Run the equivalent Bronze → Silver → Gold transformations as a Databricks notebook using Delta tables and SQL.

The sample data is synthetic and for demonstration only. The local pipeline uses Python's standard library; MySQL extraction requires the optional PyMySQL package. The Databricks notebook requires an actual Databricks cluster with PySpark.

## Run locally (no external services)

From the project directory:

```powershell
python -m commercepulse run --source csv --input data/sample/orders.csv --output data/lakehouse
python -m unittest discover -s tests -v
```

The run creates:

```text
data/lakehouse/
  bronze/orders.jsonl
  silver/orders.csv
  gold/daily_sales.csv
  gold/product_performance.csv
  gold/customer_repeat_purchase.csv
  gold/delivery_performance.csv
  quality/validation_report.json
  quality/rejected_rows.jsonl
```

Generated lakehouse files are ignored by Git; the checked-in sample CSV is not.

## Open the web dashboard

Start the local web app from the project directory:

```powershell
python -m commercepulse serve
```

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000). The dashboard reads the Gold marts, shows key retail metrics and tables, and can run the sample CSV pipeline using **Run pipeline**. Select **MySQL** to run against the optional MySQL source; the backend uses the same `MYSQL_*` environment variables described below. Keep the server running while using the page, and press `Ctrl+C` in its terminal to stop it.

The UI and API use only the Python standard library and local static assets; no frontend build step or external service is required.

## Publish on GitHub and Render

The included [`render.yaml`](render.yaml) describes one Python web service for both the dashboard and API. It binds to Render's `PORT`, exposes `/api/dashboard` as a health check, and disables MySQL on the public demo so visitors cannot use the endpoint to query a configured database.

1. Create a new GitHub repository named `commercepulse` and push this project to its root.
2. In Render, choose **New → Blueprint**, connect the GitHub repository, and deploy the `render.yaml` configuration. Render will provide a public `onrender.com` URL.
3. Use only synthetic/public sample data on the public service. Do not configure production MySQL credentials unless access controls are added first.

The Render free plan may sleep when idle and uses ephemeral local storage. The dashboard automatically regenerates the sample analytics at startup. The live demo is intended for portfolio use, not production workloads.

## Use MySQL as the source (optional)

1. Create a database and apply [`sql/mysql_schema.sql`](sql/mysql_schema.sql) to your own MySQL instance.
2. Load your order-line data into `order_items` (the included sample CSV uses the same columns).
3. Install the connector: `python -m pip install PyMySQL`.
4. Set `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_DATABASE`, `MYSQL_USER`, and `MYSQL_PASSWORD` in your local shell. Do not commit credentials.
5. Run `python -m commercepulse run --source mysql --output data/lakehouse`.

The query uses a read-only `SELECT`; it does not create or modify MySQL tables.

## Run the Databricks ELT demo (optional)

1. Upload `data/sample/orders.csv` to a Unity Catalog volume or DBFS location accessible to your cluster.
2. Open [`databricks/commercepulse_lakehouse.py`](databricks/commercepulse_lakehouse.py) as a Databricks notebook.
3. Set the `source_path` widget to the uploaded CSV path and choose a catalog/schema where you can create tables.
4. Run all cells. The notebook writes Delta `bronze_orders`, `silver_orders`, and four Gold tables, then displays the key marts.

This step needs a real Databricks workspace, cluster, and write permissions. No Azure resources are provisioned by this project. The SQL-only analyses are in [`databricks/sql/analytics.sql`](databricks/sql/analytics.sql).

## Metrics and assumptions

- Revenue = `quantity × unit_price − discount_amount`; discount is a line-level amount.
- AOV = net revenue divided by distinct orders for the reporting grain.
- Repeat customer = customer with more than one distinct order in the loaded dataset.
- Delivery delay = delivered date minus promised date; an order is on time when delivered on or before its promise date.
- The pipeline rejects missing IDs, invalid dates, non-positive quantities, negative prices/discounts, and discounts greater than line gross. Duplicate `order_item_id` values keep the first valid row.
- The sample is a small snapshot, not a time-aware incremental CDC implementation. Production use should add source watermarks, schema evolution, orchestration, secrets management, and monitoring.

## Project layout

```text
commercepulse/                 Local Python ETL package
data/sample/orders.csv         Synthetic, portable example input
databricks/                    Databricks Delta ELT notebook and SQL analyses
sql/mysql_schema.sql           Optional MySQL source schema
tests/                         Standard-library unit and pipeline tests
```
