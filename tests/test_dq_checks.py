import pandas as pd
import pytest

from dq.checks import (
    DQCheckFailed,
    check_amount_within_range,
    check_categorical_domains,
    check_required_fields_not_null,
    check_unique_event_id,
    run_dq_checks,
    run_dq_checks_or_raise,
)


def _base_row(**overrides) -> dict:
    row = {
        "event_id": "e1",
        "event_ts": "2026-06-01 10:00:00",
        "seller_id": "S100",
        "event_type": "order",
        "currency": "USD",
        "amount": "10.00",
        "signed_amount": "10.00",
        "direction": "in",
        "source_system": "checkout",
    }
    row.update(overrides)
    return row


def make_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_clean_dataset_passes_all_checks():
    df = make_df([
        _base_row(event_id="e1"),
        _base_row(event_id="e2", seller_id="S101"),
        _base_row(event_id="e3", seller_id="S102", event_type="fee", direction="out", amount="1.00"),
    ])
    report = run_dq_checks(df)
    assert not report.has_blocking_failures
    assert all(r.passed for r in report.results if r.severity == "error")


def test_null_seller_id_is_caught():
    df = make_df([_base_row(event_id="e1"), _base_row(event_id="e2", seller_id="")])
    result = check_required_fields_not_null(df)
    assert not result.passed
    assert result.failed_rows == 1


def test_duplicate_event_id_is_caught():
    df = make_df([_base_row(event_id="dup"), _base_row(event_id="dup", seller_id="S999")])
    result = check_unique_event_id(df)
    assert not result.passed
    assert result.failed_rows == 2  # обе строки участвуют в дубле


def test_amount_outlier_is_caught():
    df = make_df([_base_row(event_id="e1", amount="999999.99")])
    result = check_amount_within_range(df)
    assert not result.passed
    assert result.failed_rows == 1


def test_unexpected_event_type_is_warning_not_blocking():
    df = make_df([_base_row(event_id="e1", event_type="chargeback")])
    result = check_categorical_domains(df)
    assert not result.passed
    assert result.severity == "warning"


def test_run_dq_checks_or_raise_raises_on_blocking_failure():
    df = make_df([_base_row(event_id="dup"), _base_row(event_id="dup")])
    with pytest.raises(DQCheckFailed):
        run_dq_checks_or_raise(df)


def test_run_dq_checks_or_raise_passes_clean_dataset():
    df = make_df([_base_row(event_id="e1"), _base_row(event_id="e2", seller_id="S101")])
    report = run_dq_checks_or_raise(df)
    assert report.total_rows == 2


def test_error_rate_threshold_trips_on_many_bad_rows():
    # 3 из 4 строк дублируются -> явно выше порога 2%
    rows = [_base_row(event_id="dup") for _ in range(4)]
    df = make_df(rows)
    report = run_dq_checks(df, error_rate_threshold=0.02)
    assert report.has_blocking_failures
