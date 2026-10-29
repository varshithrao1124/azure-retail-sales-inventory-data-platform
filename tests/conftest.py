"""Shared Spark session for tests that touch Delta."""

from __future__ import annotations

import os

import pytest

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")


@pytest.fixture(scope="session")
def spark():
    from northline.config import load_settings
    from northline.spark_session import build_spark

    settings = load_settings("test")
    session = build_spark(settings)
    yield session
    session.stop()
