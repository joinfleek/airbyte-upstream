from pathlib import Path

import yaml


CONNECTOR_DIR = Path(__file__).resolve().parents[1]
MANIFEST = yaml.safe_load((CONNECTOR_DIR / "manifest.yaml").read_text())
METADATA = yaml.safe_load((CONNECTOR_DIR / "metadata.yaml").read_text())


def test_declares_expected_streams() -> None:
    assert {
        stream["$ref"].rsplit("/", 1)[-1] for stream in MANIFEST["streams"]
    } == {
        "objects",
        "object_attributes",
        "records",
        "lists",
        "list_attributes",
        "entries",
        "notes",
        "workspace_members",
    }


def test_token_is_secret_and_host_is_restricted() -> None:
    token = MANIFEST["spec"]["connection_specification"]["properties"]["api_token"]
    assert token["airbyte_secret"] is True
    assert METADATA["data"]["allowedHosts"]["hosts"] == ["api.attio.com"]


def test_post_streams_use_bounded_body_pagination() -> None:
    paginator = MANIFEST["definitions"]["offset_paginator_body"]
    assert paginator["page_token_option"]["inject_into"] == "body_json"
    assert paginator["page_token_option"]["field_name"] == "offset"
    assert paginator["page_size_option"]["field_name"] == "limit"
    assert paginator["pagination_strategy"]["page_size"] == 500

    streams = MANIFEST["definitions"]["streams"]
    assert streams["records"]["retriever"]["requester"]["http_method"] == "POST"
    assert streams["entries"]["retriever"]["requester"]["http_method"] == "POST"


def test_notes_respect_endpoint_page_limit() -> None:
    paginator = MANIFEST["definitions"]["offset_paginator_query"]
    assert paginator["page_size_option"]["field_name"] == "limit"
    assert paginator["pagination_strategy"]["page_size"] == 50


def test_child_paths_use_parent_stream_slices() -> None:
    streams = MANIFEST["definitions"]["streams"]
    assert streams["object_attributes"]["retriever"]["requester"]["path"] == (
        "objects/{{ stream_slice.object_slug }}/attributes"
    )
    assert streams["records"]["retriever"]["requester"]["path"] == (
        "objects/{{ stream_slice.object_slug }}/records/query"
    )
    assert streams["list_attributes"]["retriever"]["requester"]["path"] == (
        "lists/{{ stream_slice.list_slug }}/attributes"
    )
    assert streams["entries"]["retriever"]["requester"]["path"] == (
        "lists/{{ stream_slice.list_slug }}/entries/query"
    )


def test_retries_rate_limits_and_server_errors() -> None:
    handler = MANIFEST["definitions"]["error_handler"]
    filters = {
        tuple(response_filter["http_codes"]): response_filter["action"]
        for response_filter in handler["response_filters"]
    }
    assert filters[(429,)] == "RATE_LIMITED"
    assert filters[(500, 502, 503, 504)] == "RETRY"
    assert handler["backoff_strategies"][0] == {
        "type": "WaitTimeFromHeader",
        "header": "Retry-After",
    }
