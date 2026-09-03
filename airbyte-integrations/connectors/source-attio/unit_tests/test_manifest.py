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
        "object_attribute_options",
        "object_attribute_statuses",
        "records",
        "lists",
        "list_attributes",
        "list_attribute_options",
        "entries",
        "notes",
        "workspace_members",
    }


def test_token_is_secret_and_host_is_restricted() -> None:
    token = MANIFEST["spec"]["connection_specification"]["properties"]["api_token"]
    assert token["airbyte_secret"] is True
    assert METADATA["data"]["allowedHosts"]["hosts"] == ["api.attio.com"]
    assert MANIFEST["definitions"]["base_requester"]["use_cache"] is False


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
        "objects/{{ stream_partition.object_slug }}/attributes"
    )
    assert streams["records"]["retriever"]["requester"]["path"] == (
        "objects/{{ stream_partition.object_slug }}/records/query"
    )
    assert streams["list_attributes"]["retriever"]["requester"]["path"] == (
        "lists/{{ stream_partition.list_slug }}/attributes"
    )
    assert streams["entries"]["retriever"]["requester"]["path"] == (
        "lists/{{ stream_partition.list_slug }}/entries/query"
    )


def test_primary_keys_are_flattened_for_concurrent_discovery() -> None:
    streams = MANIFEST["definitions"]["streams"]
    expected = {
        "objects": ["workspace_id", "object_id"],
        "object_attributes": ["workspace_id", "target_id", "attribute_id"],
        "object_attribute_options": [
            "workspace_id",
            "target_id",
            "attribute_id",
            "option_id",
        ],
        "object_attribute_statuses": [
            "workspace_id",
            "target_id",
            "attribute_id",
            "status_id",
        ],
        "records": ["workspace_id", "object_id", "record_id"],
        "lists": ["workspace_id", "list_id"],
        "list_attributes": ["workspace_id", "target_id", "attribute_id"],
        "list_attribute_options": [
            "workspace_id",
            "target_id",
            "attribute_id",
            "option_id",
        ],
        "entries": ["workspace_id", "list_id", "entry_id"],
        "notes": ["workspace_id", "note_id"],
        "workspace_members": ["workspace_id", "workspace_member_id"],
    }

    for stream_name, primary_key in expected.items():
        stream = streams[stream_name]
        assert stream["primary_key"] == primary_key
        schema = MANIFEST["schemas"][stream["schema_loader"]["schema"]["$ref"].rsplit("/", 1)[-1]]
        assert set(primary_key) <= set(schema["properties"])


def test_typed_destination_schema_declares_the_api_contract() -> None:
    expected_properties = {
        "object": {"created_at", "plural_noun", "singular_noun"},
        "list": {
            "created_at",
            "created_by_actor",
            "name",
            "parent_object",
            "workspace_access",
            "workspace_member_access",
        },
        "workspace_member": {
            "access_level",
            "avatar_url",
            "created_at",
            "email_address",
            "first_name",
            "last_name",
        },
        "object_attribute": {
            "api_slug",
            "config",
            "created_at",
            "default_value",
            "description",
            "is_archived",
            "is_default_value_enabled",
            "is_multiselect",
            "is_required",
            "is_system_attribute",
            "is_unique",
            "is_writable",
            "relationship",
            "title",
            "type",
        },
        "record": {"created_at", "values", "web_url"},
        "entry": {"created_at", "entry_values", "parent_object", "parent_record_id"},
        "note": {
            "content_markdown",
            "content_plaintext",
            "created_at",
            "created_by_actor",
            "meeting_id",
            "parent_object",
            "parent_record_id",
            "tags",
            "title",
        },
        "attribute_option": {"is_archived", "title"},
        "attribute_status": {
            "celebration_enabled",
            "is_archived",
            "target_time_in_status",
            "title",
        },
    }

    for schema_name, properties in expected_properties.items():
        assert properties <= set(MANIFEST["schemas"][schema_name]["properties"])


def test_archived_configuration_and_child_values_are_included() -> None:
    streams = MANIFEST["definitions"]["streams"]
    for stream_name in (
        "object_attributes",
        "list_attributes",
        "object_select_attributes",
        "list_select_attributes",
        "object_status_attributes",
        "object_attribute_options",
        "list_attribute_options",
        "object_attribute_statuses",
    ):
        parameters = streams[stream_name]["retriever"]["requester"]["request_parameters"]
        assert parameters["show_archived"] == "true"

    assert MANIFEST["definitions"]["select_attribute_selector"]["record_filter"]
    assert MANIFEST["definitions"]["status_attribute_selector"]["record_filter"]


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
