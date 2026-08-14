from unittest.mock import Mock

import pendulum as pdm

from source_shopify.shopify_graphql.bulk.job import ShopifyBulkManager
from source_shopify.shopify_graphql.bulk.query import MetafieldCollection, MetafieldOrder, Product
from source_shopify.streams.streams import Collections, MetafieldCollections


def _product_manager() -> ShopifyBulkManager:
    return ShopifyBulkManager(
        http_client=Mock(),
        base_url="https://example.test/graphql.json",
        query=Product({"shop_id": 0}),
        job_termination_threshold=3600,
        job_size=30,
        job_checkpoint_interval=100_000,
    )


def test_grouped_product_query_does_not_support_threshold_checkpointing() -> None:
    manager = _product_manager()

    assert manager._supports_checkpointing is False


def test_new_record_only_composition_keeps_threshold_checkpointing() -> None:
    query = MetafieldOrder({"shop_id": 0})

    assert query.record_composition == {"new_record": "Metafield"}
    assert query.supports_checkpointing is True


def test_metafield_collections_groups_children_and_tracks_collection_state() -> None:
    query = MetafieldCollection({"shop_id": 0})

    assert query.record_composition == {
        "new_record": "Collection",
        "record_components": ["Metafield"],
    }
    assert query.supports_checkpointing is False
    assert MetafieldCollections.parent_stream_class is Collections


def test_metafield_collections_emits_one_stale_state_carrier(auth_config) -> None:
    stream = MetafieldCollections(auth_config)
    state = {"updated_at": "2026-03-01T00:00:00Z"}
    stale_records = [
        {"id": 1, "updated_at": "2025-01-01T00:00:00Z"},
        {"id": 2, "updated_at": "2025-02-01T00:00:00Z"},
    ]

    assert list(stream.filter_records_newer_than_state(state, stale_records)) == [stale_records[-1]]


def test_metafield_collections_does_not_add_carrier_when_new_record_exists(auth_config) -> None:
    stream = MetafieldCollections(auth_config)
    state = {"updated_at": "2026-03-01T00:00:00Z"}
    new_record = {"id": 2, "updated_at": "2026-04-01T00:00:00Z"}
    records = [
        {"id": 1, "updated_at": "2025-01-01T00:00:00Z"},
        new_record,
    ]

    assert list(stream.filter_records_newer_than_state(state, records)) == [new_record]


def test_self_canceled_non_checkpointable_job_does_not_consume_partial_result() -> None:
    manager = _product_manager()
    manager._job_self_canceled = True
    manager._job_last_rec_count = 409_587
    manager._job_created_at = pdm.now().subtract(hours=2).to_iso8601_string()
    manager._job_get_result = Mock(return_value="partial-products.jsonl")
    slice_start = pdm.datetime(2026, 8, 4, tz="UTC")
    slice_end = pdm.datetime(2026, 8, 5, tz="UTC")

    manager._on_canceled_job(Mock())
    adjusted_end = manager.get_adjusted_job_end(slice_start, slice_end)

    manager._job_get_result.assert_not_called()
    assert manager._job_result_filename is None
    assert manager._job_adjust_slice_from_checkpoint is False
    assert adjusted_end == slice_start
    assert manager._job_size == 15
