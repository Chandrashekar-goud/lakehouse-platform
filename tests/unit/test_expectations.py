from lakehouse.quality.expectations import (
    assert_unique,
    expect_between,
    expect_not_null,
    run_suite,
)
import pytest


def test_run_suite_splits_good_and_quarantine_with_reasons(spark):
    df = spark.createDataFrame(
        [("a", 10.0), ("b", None), (None, 500.0)], "id string, temp double"
    )
    good, quarantine, result = run_suite(
        df, [expect_not_null("id"), expect_between("temp", -60, 60)]
    )
    assert good.count() == 2  # null temp is allowed by expect_between
    assert quarantine.count() == 1
    reasons = quarantine.collect()[0]["_dq_failures"]
    assert "not_null(id)" in reasons and "between(temp,-60,60)" in reasons
    assert result.pass_rate == pytest.approx(2 / 3)


def test_assert_unique_raises_on_duplicates(spark):
    df = spark.createDataFrame([("a",), ("a",)], "id string")
    with pytest.raises(ValueError, match="Uniqueness violated"):
        assert_unique(df, ["id"])
