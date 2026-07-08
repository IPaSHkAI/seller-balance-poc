"""
Реальные проверки качества данных для Seller Balance Pipeline.

В отличие от версии-скелета (dq_stub, который просто печатал строку),
этот модуль действительно проверяет датафрейм и:
  * возвращает структурированный отчёт (DQReport) по каждой проверке;
  * поднимает DQCheckFailed, если хотя бы одна проверка помечена как
    блокирующая (severity="error") и не прошла — чтобы Airflow-таск падал,
    а не "зелёнел" молча.

Проверки:
  1. Обязательные поля не должны быть null (event_id, event_ts, seller_id).
  2. event_id должен быть уникален (дубликаты событий).
  3. amount должен быть в разумном диапазоне (outlier detection).
  4. Ожидаемое множество event_type / direction (schema drift guard).
  5. Доля "грязных" строк не должна превышать порог (row-level error rate).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

Severity = Literal["error", "warning"]

REQUIRED_FIELDS = ["event_id", "event_ts", "seller_id"]
VALID_EVENT_TYPES = {"order", "payout", "refund", "fee"}
VALID_DIRECTIONS = {"in", "out"}
MAX_REASONABLE_AMOUNT = 10_000.0  # свыше — считаем аномалией для этого синтетического датасета


@dataclass
class CheckResult:
    name: str
    passed: bool
    severity: Severity
    details: str
    failed_rows: int = 0


@dataclass
class DQReport:
    total_rows: int
    results: list[CheckResult] = field(default_factory=list)

    @property
    def has_blocking_failures(self) -> bool:
        return any(not r.passed and r.severity == "error" for r in self.results)

    def summary(self) -> str:
        lines = [f"DQ report: {self.total_rows} rows checked"]
        for r in self.results:
            status = "OK" if r.passed else f"FAIL ({r.severity})"
            lines.append(f"  [{status}] {r.name}: {r.details}")
        return "\n".join(lines)

    def to_dict(self) -> dict:
        return {
            "total_rows": self.total_rows,
            "has_blocking_failures": self.has_blocking_failures,
            "checks": [r.__dict__ for r in self.results],
        }


class DQCheckFailed(Exception):
    """Поднимается, когда хотя бы одна блокирующая проверка не прошла."""


def check_required_fields_not_null(df: pd.DataFrame) -> CheckResult:
    null_mask = df[REQUIRED_FIELDS].isnull().any(axis=1) | (df[REQUIRED_FIELDS] == "").any(axis=1)
    failed = int(null_mask.sum())
    return CheckResult(
        name="required_fields_not_null",
        passed=failed == 0,
        severity="error",
        details=f"{failed} строк с null/пустыми значениями в {REQUIRED_FIELDS}",
        failed_rows=failed,
    )


def check_unique_event_id(df: pd.DataFrame) -> CheckResult:
    dup_mask = df.duplicated(subset=["event_id"], keep=False)
    failed = int(dup_mask.sum())
    return CheckResult(
        name="unique_event_id",
        passed=failed == 0,
        severity="error",
        details=f"{failed} строк с повторяющимся event_id",
        failed_rows=failed,
    )


def check_amount_within_range(df: pd.DataFrame, max_amount: float = MAX_REASONABLE_AMOUNT) -> CheckResult:
    amounts = pd.to_numeric(df["amount"], errors="coerce")
    outlier_mask = amounts.isnull() | (amounts.abs() > max_amount)
    failed = int(outlier_mask.sum())
    return CheckResult(
        name="amount_within_range",
        passed=failed == 0,
        severity="error",
        details=f"{failed} строк с amount вне диапазона (> {max_amount}) или нечисловым значением",
        failed_rows=failed,
    )


def check_categorical_domains(df: pd.DataFrame) -> CheckResult:
    bad_type = ~df["event_type"].isin(VALID_EVENT_TYPES)
    bad_dir = ~df["direction"].isin(VALID_DIRECTIONS)
    failed = int((bad_type | bad_dir).sum())
    return CheckResult(
        name="categorical_domains",
        passed=failed == 0,
        severity="warning",
        details=f"{failed} строк с неожиданным event_type/direction (возможен schema drift)",
        failed_rows=failed,
    )


def check_error_row_rate(df: pd.DataFrame, results_so_far: list[CheckResult], threshold: float = 0.02) -> CheckResult:
    total = len(df)
    error_rows = sum(r.failed_rows for r in results_so_far if r.severity == "error")
    rate = error_rows / total if total else 0.0
    passed = rate <= threshold
    return CheckResult(
        name="error_row_rate",
        passed=passed,
        severity="error",
        details=f"доля строк с ошибками {rate:.2%} (порог {threshold:.0%})",
    )


def run_dq_checks(df: pd.DataFrame, error_rate_threshold: float = 0.02) -> DQReport:
    report = DQReport(total_rows=len(df))
    report.results.append(check_required_fields_not_null(df))
    report.results.append(check_unique_event_id(df))
    report.results.append(check_amount_within_range(df))
    report.results.append(check_categorical_domains(df))
    report.results.append(
        check_error_row_rate(df, report.results, threshold=error_rate_threshold)
    )
    return report


def run_dq_checks_or_raise(df: pd.DataFrame, error_rate_threshold: float = 0.02) -> DQReport:
    report = run_dq_checks(df, error_rate_threshold=error_rate_threshold)
    if report.has_blocking_failures:
        raise DQCheckFailed(report.summary())
    return report


if __name__ == "__main__":
    import sys

    path = sys.argv[1] if len(sys.argv) > 1 else "data/events.csv"
    data = pd.read_csv(path, dtype=str)
    rep = run_dq_checks(data)
    print(rep.summary())
    if rep.has_blocking_failures:
        sys.exit(1)
