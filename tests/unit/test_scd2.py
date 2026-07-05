import pytest

pytest.importorskip("delta")

from datetime import datetime

from lakehouse.transforms.crimes import scd2_upsert_iucr


def _updates(spark, rows):
    return spark.createDataFrame(
        rows, "iucr string, primary_type string, description string, effective_from timestamp"
    )


def test_scd2_closes_old_version_and_inserts_new(spark, tmp_path):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS scdtest LOCATION '{tmp_path}'")
    table = "scdtest.dim_iucr"
    t0, t1 = datetime(2026, 1, 1), datetime(2026, 6, 1)

    scd2_upsert_iucr(spark, _updates(spark, [("0810", "THEFT", "OVER $500", t0)]), table)
    scd2_upsert_iucr(spark, _updates(spark, [("0810", "THEFT", "OVER $1000", t1)]), table)

    rows = {(r["description"], r["is_current"]) for r in spark.table(table).collect()}
    assert rows == {("OVER $500", False), ("OVER $1000", True)}
    closed = spark.table(table).filter("is_current = false").collect()[0]
    assert closed["effective_to"] == t1
    spark.sql("DROP DATABASE scdtest CASCADE")
