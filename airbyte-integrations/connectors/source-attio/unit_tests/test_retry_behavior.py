from pathlib import Path
from unittest.mock import Mock, patch

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.declarative.yaml_declarative_source import (
    YamlDeclarativeSource,
)
from requests import Response


CONNECTOR_DIR = Path(__file__).resolve().parents[1]


def _response(status: int, body: bytes, headers: dict[str, str] | None = None) -> Response:
    response = Response()
    response.status_code = status
    response._content = body
    response.headers.update(headers or {})
    response.url = "https://api.attio.com/v2/objects"
    response.request = Mock(body=None, headers={})
    return response


def test_objects_retries_429_using_retry_after_without_leaking_auth() -> None:
    config = {"api_token": "unit-test-token-never-sent"}
    source = YamlDeclarativeSource(
        path_to_yaml=str(CONNECTOR_DIR / "manifest.yaml"),
        catalog=None,
        config=config,
        state={},
    )
    objects = next(stream for stream in source.streams(config) if stream.name == "objects")

    rate_limited = _response(429, b'{}', {"Retry-After": "0"})
    succeeded = _response(200, b'{"data": []}')

    with patch.object(
        objects.retriever.requester._http_client._session,
        "send",
        side_effect=[rate_limited, succeeded],
    ) as send:
        assert list(objects.read_records(sync_mode=SyncMode.full_refresh)) == []

    assert send.call_count == 2
