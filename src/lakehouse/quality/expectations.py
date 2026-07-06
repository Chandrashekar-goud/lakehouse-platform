"""Lightweight data-quality framework with quarantine semantics.

Row-level checks tag failing rows; run_suite splits the input into (good,
quarantine) so bad records are preserved for triage instead of silently
dropped or fatally failing the job. Table-level checks (uniqueness) gate the
run as a whole. This is deliberately a ~100-line framework instead of Great
Expectations: fewer moving parts on a free tier, and the mechanics are
explainable line by line in an interview.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


@dataclass
class Check:
    name: str
    fail_condition: Callable[[], Column]  # true when a row FAILS


@dataclass
class SuiteResult:
    total_rows: int
    passed_rows: int
    quarantined_rows: int
    failures_by_check: dict[str, int]

    @property
    def pass_rate(self) -> float:
        return self.passed_rows / self.total_rows if self.total_rows else 1.0


def expect_not_null(*cols: str) -> Check:
    def cond() -> Column:
        conditions = [F.col(c).isNull() for c in cols]
        # F.greatest requires >= 2 columns; a single check is its own condition.
        return conditions[0] if len(conditions) == 1 else F.greatest(*conditions)

    return Check(name=f"not_null({','.join(cols)})", fail_condition=cond)


def expect_between(col: str, lo: float, hi: float, allow_null: bool = True) -> Check:
    def cond() -> Column:
        out_of_range = ~F.col(col).between(lo, hi)
        return out_of_range if not allow_null else (F.col(col).isNotNull() & out_of_range)

    return Check(name=f"between({col},{lo},{hi})", fail_condition=cond)


def expect_values_in(col: str, allowed: list[str]) -> Check:
    return Check(
        name=f"values_in({col})",
        fail_condition=lambda: F.col(col).isNotNull() & ~F.col(col).isin(allowed),
    )


def run_suite(df: DataFrame, checks: list[Check]) -> tuple[DataFrame, DataFrame, SuiteResult]:
    """Returns (good_rows, quarantined_rows_with_reasons, result)."""
    tagged = df
    flag_cols = []
    for i, check in enumerate(checks):
        flag = f"_dq_{i}"
        tagged = tagged.withColumn(flag, F.coalesce(check.fail_condition(), F.lit(False)))
        flag_cols.append((flag, check.name))

    reasons = F.array_compact(
        F.array(*[F.when(F.col(flag), F.lit(name)) for flag, name in flag_cols])
    )
    tagged = tagged.withColumn("_dq_failures", reasons)

    failures_by_check = {
        name: tagged.filter(F.col(flag)).count() for flag, name in flag_cols
    }
    total = tagged.count()
    good = tagged.filter(F.size("_dq_failures") == 0).drop(
        "_dq_failures", *[f for f, _ in flag_cols]
    )
    quarantine = tagged.filter(F.size("_dq_failures") > 0).drop(*[f for f, _ in flag_cols])
    passed = total - quarantine.count()

    return good, quarantine, SuiteResult(
        total_rows=total,
        passed_rows=passed,
        quarantined_rows=total - passed,
        failures_by_check=failures_by_check,
    )


def assert_unique(df: DataFrame, keys: list[str]) -> None:
    """Table-level gate: duplicate keys after dedup means the pipeline is
    broken, so fail loudly rather than quarantine."""
    dupes = df.groupBy(*keys).count().filter("count > 1").count()
    if dupes:
        raise ValueError(f"Uniqueness violated on {keys}: {dupes} duplicate key groups")
