import json
import logging
import os
from pathlib import Path

import pytest
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.declarative.yaml_declarative_source import (
    YamlDeclarativeSource,
)


CONNECTOR_DIR = Path(__file__).resolve().parents[1]
EXPECTED = json.loads(
    (CONNECTOR_DIR / "integration_tests" / "expected_streams.json").read_text()
)
TOKEN = os.getenv("ATTIO_API_TOKEN")

pytestmark = pytest.mark.skipif(
    not TOKEN,
    reason="ATTIO_API_TOKEN is required for read-only live contract checks",
)


def _source() -> tuple[YamlDeclarativeSource, dict[str, str]]:
    config = {"api_token": TOKEN}
    source = YamlDeclarativeSource(
        path_to_yaml=str(CONNECTOR_DIR / "manifest.yaml"),
        catalog=None,
        config=config,
        state={},
    )
    return source, config


def _rows(stream) -> list[dict]:
    return list(stream.read_records(sync_mode=SyncMode.full_refresh))


def test_live_discovery_and_workspace_topology() -> None:
    source, config = _source()
    assert source.check(logging.getLogger("source-attio-live"), config).status.value == (
        "SUCCEEDED"
    )

    catalog = source.discover(logging.getLogger("source-attio-discover"), config)
    assert sorted(stream.name for stream in catalog.streams) == EXPECTED["streams"]

    streams = {stream.name: stream for stream in source.streams(config)}
    assert sorted(streams) == EXPECTED["streams"]

    objects = _rows(streams["objects"])
    lists = _rows(streams["lists"])
    members = _rows(streams["workspace_members"])

    assert sorted(row["api_slug"] for row in objects) == EXPECTED["objects"]
    assert sorted(row["api_slug"] for row in lists) == EXPECTED["lists"]
    assert members
    assert all(isinstance(row.get("id"), dict) for row in objects + lists + members)


def test_live_companies_partition_has_unique_records() -> None:
    source, config = _source()
    records = next(stream for stream in source.streams(config) if stream.name == "records")
    company_slice = next(
        stream_slice
        for stream_slice in records.stream_slices(sync_mode=SyncMode.full_refresh)
        if stream_slice["object_slug"] == "companies"
    )

    rows = list(
        records.read_records(
            sync_mode=SyncMode.full_refresh,
            stream_slice=company_slice,
        )
    )
    keys = {
        (
            row["workspace_id"],
            row["object_id"],
            row["record_id"],
        )
        for row in rows
    }

    assert rows
    assert len(rows) == len(keys)
    assert all(None not in key for key in keys)
    assert all(row["_airbyte_attio_object_slug"] == "companies" for row in rows)
