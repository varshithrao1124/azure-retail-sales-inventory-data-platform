"""Secret lookup.

Local and CI runs stay on ``secret_mode=env`` and do not need credentials.
Databricks jobs use a Key Vault-backed secret scope. Values are never logged.
"""

from __future__ import annotations

import os

from northline.errors import ConfigError


def get_secret(name: str, *, mode: str = "env", scope: str = "", spark=None) -> str:
    if mode == "env":
        value = os.environ.get(name, "")
        if not value:
            raise ConfigError(
                f"Environment variable {name} is empty. "
                "See .env.example. Do not commit a filled-in .env."
            )
        return value
    if mode == "databricks":
        if spark is None or not scope:
            raise ConfigError("Databricks secret lookup needs a Spark session and secret scope")
        try:
            from pyspark.dbutils import DBUtils
        except ImportError as exc:
            raise ConfigError("pyspark.dbutils is available only on a Databricks cluster") from exc
        return DBUtils(spark).secrets.get(scope, name)
    raise ConfigError(f"Unknown secret mode {mode}")
