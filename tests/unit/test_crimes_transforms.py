from lakehouse.transforms.crimes import silver_crimes


def _bronze(spark, rows):
    return spark.createDataFrame(
        rows,
        "case_number string, date string, updated_on string, primary_type string, "
        "arrest string, domestic string, latitude string, longitude string, "
        "iucr string, description string",
    )


def test_silver_dedupes_on_case_number_keeping_latest_update(spark):
    rows = [
        ("hy100", "2026-05-01T10:00", "2026-05-02T00:00", "theft", "false", "false",
         "41.88", "-87.63", "0810", "over $500"),
        ("HY100", "2026-05-01T10:00", "2026-05-03T00:00", "THEFT", "true", "false",
         "41.88", "-87.63", "0810", "over $500"),
    ]
    out = silver_crimes(_bronze(spark, rows)).collect()
    assert len(out) == 1
    assert out[0]["arrest"] is True  # later updated_on wins
    assert out[0]["case_number"] == "HY100"  # normalized


def test_silver_nulls_impossible_coordinates_but_keeps_row(spark):
    rows = [("HY200", "2026-05-01T10:00", "2026-05-02T00:00", "BATTERY", "false",
             "true", "0.0", "0.0", "0460", "simple")]
    out = silver_crimes(_bronze(spark, rows)).collect()[0]
    assert out["latitude"] is None and out["longitude"] is None
    assert out["case_number"] == "HY200"


def test_silver_drops_rows_missing_required_keys(spark):
    rows = [(None, "2026-05-01T10:00", "2026-05-02T00:00", "THEFT", "false", "false",
             "41.88", "-87.63", "0810", "x")]
    assert silver_crimes(_bronze(spark, rows)).count() == 0
