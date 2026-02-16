"""
dlt pipeline: loads sales.csv into DuckDB (raw.sales table).
Run from Jupyter: from load_sales import run; run()
"""
import csv
from pathlib import Path
import dlt


@dlt.resource(name="sales", write_disposition="replace")
def sales_data(csv_path: str = "/workspace/data/sales.csv"):
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["quantity"] = int(row["quantity"])
            row["amount"]   = float(row["amount"])
            yield row


def run():
    pipeline = dlt.pipeline(
        pipeline_name="sales_pipeline",
        destination=dlt.destinations.duckdb("/workspace/db/lakehouse.duckdb"),
        dataset_name="raw",
    )
    info = pipeline.run(sales_data())
    print(info)
    return pipeline


if __name__ == "__main__":
    run()
