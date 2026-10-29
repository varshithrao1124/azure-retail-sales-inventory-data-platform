"""Config switching and retry behavior. No Spark session."""

import pytest

from northline.config import load_settings
from northline.errors import ConfigError
from northline.retry import call_with_retry
from northline.secrets import get_secret


def test_local_and_adls_paths(monkeypatch):
    local = load_settings("dev")
    assert local.storage_mode == "local"
    assert local.table_path("gold", "fact_sales").endswith("gold/fact_sales")
    assert local.table_id("silver", "customer_scd") == "northline_dev.silver.customer_scd"
    assert not local.table_path("bronze", "orders").startswith("abfss://")

    monkeypatch.setenv("NL_STORAGE_MODE", "adls")
    monkeypatch.setenv("NL_STORAGE_ACCOUNT", "stexample")
    monkeypatch.setenv("NL_CATALOG_MODE", "unity")
    monkeypatch.setenv("NL_CATALOG", "northline_dev")
    remote = load_settings("dev")
    assert remote.storage_mode == "adls"
    assert remote.use_unity_catalog
    assert remote.table_path("gold", "fact_sales") == (
        "abfss://gold@stexample.dfs.core.windows.net/fact_sales"
    )
    assert remote.landing_dir == "abfss://landing@stexample.dfs.core.windows.net/batches"


def test_unknown_environment():
    with pytest.raises(ConfigError):
        load_settings("missing")


def test_retry_on_delta_conflict_and_not_on_value_error():
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("ConcurrentAppendException: commit clash")
        return "done"

    assert call_with_retry(flaky, attempts=3, backoff_seconds=0, operation="test") == "done"
    assert calls["n"] == 3

    def bad():
        raise ValueError("bad row")

    with pytest.raises(ValueError):
        call_with_retry(bad, attempts=4, backoff_seconds=0)


def test_env_secret_missing(monkeypatch):
    monkeypatch.delenv("NL_DOES_NOT_EXIST", raising=False)
    with pytest.raises(ConfigError):
        get_secret("NL_DOES_NOT_EXIST", mode="env")
