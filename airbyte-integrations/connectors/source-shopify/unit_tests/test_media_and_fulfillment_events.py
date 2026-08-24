#
# Copyright (c) 2026 Airbyte, Inc., all rights reserved.
#

"""
Fleek fork streams: `product_media` (GraphQL BULK over products.media) and
`fulfillment_events` (REST /orders/{id}/fulfillments/{id}/events.json).

Scenarios:
  - happy: a product with a MediaImage and an ExternalVideo produces one flat
    record each, carrying product_id, flattened preview/original image fields.
  - sad: a product with no media components produces no records.
  - fulfillment_events: slices are derived from the parent order payload and the
    path/order_id stamping is correct.
"""

from source_shopify.shopify_graphql.bulk.query import ProductMedia as ProductMediaQuery
from source_shopify.streams.streams import FulfillmentEvents


def test_product_media_processes_components():
    query = ProductMediaQuery({})
    record = {
        "__typename": "Product",
        "id": 111,
        "record_components": {
            "MediaImage": [
                {
                    "__typename": "MediaImage",
                    "__parentId": "gid://shopify/Product/111",
                    "id": "gid://shopify/MediaImage/222",
                    "alt": "",
                    "status": "READY",
                    "mediaContentType": "IMAGE",
                    "mimeType": "image/jpeg",
                    "createdAt": "2023-01-01T15:00:00Z",
                    "updatedAt": "2023-01-02T15:00:00Z",
                    "preview": {"image": {"url": "https://cdn/preview.jpg", "width": 100, "height": 50}},
                    "image": {"url": "https://cdn/original.jpg", "width": 1000, "height": 500},
                }
            ],
            "ExternalVideo": [
                {
                    "__typename": "ExternalVideo",
                    "__parentId": "gid://shopify/Product/111",
                    "id": "gid://shopify/ExternalVideo/333",
                    "alt": "a video",
                    "status": "READY",
                    "mediaContentType": "EXTERNAL_VIDEO",
                    "originUrl": "https://youtube.com/watch?v=1",
                    "preview": {"image": {"url": "https://cdn/video.jpg", "width": 10, "height": 5}},
                }
            ],
        },
    }

    image, video = list(query.record_process_components(record))

    assert image["id"] == 222
    assert image["admin_graphql_api_id"] == "gid://shopify/MediaImage/222"
    assert image["product_id"] == 111
    assert image["media_type"] == "MediaImage"
    assert image["alt"] is None
    assert image["preview_image_url"] == "https://cdn/preview.jpg"
    assert image["image_url"] == "https://cdn/original.jpg"
    assert image["image_width"] == 1000
    assert image["createdAt"] == "2023-01-01T15:00:00+00:00"
    assert "__parentId" not in image

    assert video["id"] == 333
    assert video["media_type"] == "ExternalVideo"
    assert video["originUrl"] == "https://youtube.com/watch?v=1"
    assert video["image_url"] is None
    assert video["updatedAt"] is None


def test_product_media_without_components_emits_nothing():
    query = ProductMediaQuery({})
    assert list(query.record_process_components({"id": 111, "record_components": {}})) == []


def test_fulfillment_events_slices_and_path(auth_config, mocker):
    stream = FulfillmentEvents(auth_config)
    parent_records = [
        {"id": 1, "updated_at": "2023-01-01T00:00:00+00:00", "fulfillments": [{"id": 10}, {"id": 11}]},
        {"id": 2, "updated_at": "2023-01-02T00:00:00+00:00", "fulfillments": []},
        {"id": 3, "deleted_at": "2023-01-03T00:00:00+00:00", "fulfillments": [{"id": 12}]},
    ]
    mocker.patch.object(type(stream.parent_stream), "read_records", return_value=parent_records)

    slices = list(stream.stream_slices())

    assert slices == [
        {"order_id": 1, "fulfillment_id": 10},
        {"order_id": 1, "fulfillment_id": 11},
    ]
    assert stream.path(stream_slice=slices[0]) == "orders/1/fulfillments/10/events.json"


def test_fulfillment_events_stamps_order_id(auth_config, mocker):
    stream = FulfillmentEvents(auth_config)
    mocker.patch.object(
        FulfillmentEvents.__mro__[1],
        "read_records",
        return_value=iter([{"id": 5, "fulfillment_id": 10}]),
    )

    records = list(stream.read_records(stream_slice={"order_id": 1, "fulfillment_id": 10}))

    assert records == [{"id": 5, "fulfillment_id": 10, "order_id": 1}]
