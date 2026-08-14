#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import time

import pytest
import requests
from source_shopify.auth import ClientCredentialsAuthenticator, ClientCredentialsTokenError, NotImplementedAuth, ShopifyAuthenticator
from source_shopify.source import ConnectionCheckTest


TEST_ACCESS_TOKEN = "test_access_token"
TEST_API_PASSWORD = "test_api_password"
TEST_CLIENT_ID = "test_client_id"
TEST_CLIENT_SECRET = "test_client_secret"
TEST_SHOP = "test-shop"
TOKEN_URL = f"https://{TEST_SHOP}.myshopify.com/admin/oauth/access_token"


@pytest.fixture
def config_access_token():
    return {"credentials": {"access_token": TEST_ACCESS_TOKEN, "auth_method": "access_token"}}


@pytest.fixture
def config_api_password():
    return {"credentials": {"api_password": TEST_API_PASSWORD, "auth_method": "api_password"}}


@pytest.fixture
def config_not_implemented_auth_method():
    return {"credentials": {"auth_method": "not_implemented_auth_method"}}


@pytest.fixture
def config_missing_access_token():
    return {"shop": "SHOP_NAME", "credentials": {"auth_method": "oauth2.0", "access_token": None}}


@pytest.fixture
def config_client_credentials():
    return {
        "shop": TEST_SHOP,
        "credentials": {
            "auth_method": "client_credentials",
            "client_id": TEST_CLIENT_ID,
            "client_secret": TEST_CLIENT_SECRET,
        },
    }


@pytest.fixture
def expected_auth_header_access_token():
    return {"X-Shopify-Access-Token": TEST_ACCESS_TOKEN}


@pytest.fixture
def expected_auth_header_api_password():
    return {"X-Shopify-Access-Token": TEST_API_PASSWORD}


def test_shopify_authenticator_access_token(config_access_token, expected_auth_header_access_token):
    authenticator = ShopifyAuthenticator(config=config_access_token)
    assert authenticator.get_auth_header() == expected_auth_header_access_token


def test_shopify_authenticator_api_password(config_api_password, expected_auth_header_api_password):
    authenticator = ShopifyAuthenticator(config=config_api_password)
    assert authenticator.get_auth_header() == expected_auth_header_api_password


def test_raises_notimplemented_auth(config_not_implemented_auth_method):
    authenticator = ShopifyAuthenticator(config=(config_not_implemented_auth_method))
    with pytest.raises(NotImplementedAuth):
        authenticator.get_auth_header()


def test_raises_missing_access_token(config_missing_access_token):
    config_missing_access_token["authenticator"] = ShopifyAuthenticator(config=(config_missing_access_token))
    failed_check = ConnectionCheckTest(config_missing_access_token).test_connection()
    assert failed_check == (
        False,
        "Authentication was unsuccessful. Please verify your authentication credentials or login is correct.",
    )


@pytest.mark.parametrize(
    "shop, expected_shop",
    [
        pytest.param("my-store", "my-store", id="bare_handle"),
        pytest.param("my-store.myshopify.com", "my-store", id="myshopify_domain"),
    ],
)
def test_client_credentials_get_shop_name(shop, expected_shop):
    authenticator = ClientCredentialsAuthenticator(config={"shop": shop})
    assert authenticator._get_shop_name() == expected_shop


def test_exchange_token_success(config_client_credentials, requests_mock):
    requests_mock.post(TOKEN_URL, json={"access_token": "new_token", "expires_in": 86400})
    authenticator = ClientCredentialsAuthenticator(config=config_client_credentials)

    access_token, expiry_time = authenticator._exchange_credentials_for_token()

    assert access_token == "new_token"
    assert expiry_time > time.time()
    assert requests_mock.last_request.text == "client_id=test_client_id&client_secret=test_client_secret&grant_type=client_credentials"


def test_exchange_token_uses_default_expiry(config_client_credentials, requests_mock):
    requests_mock.post(TOKEN_URL, json={"access_token": "new_token"})
    authenticator = ClientCredentialsAuthenticator(config=config_client_credentials)
    before = time.time()

    _, expiry_time = authenticator._exchange_credentials_for_token()

    assert expiry_time >= before + ClientCredentialsAuthenticator.DEFAULT_TOKEN_EXPIRY_SECONDS


@pytest.mark.parametrize(
    "credentials",
    [
        pytest.param({"client_secret": TEST_CLIENT_SECRET}, id="missing_client_id"),
        pytest.param({"client_id": TEST_CLIENT_ID}, id="missing_client_secret"),
    ],
)
def test_exchange_token_missing_credentials(credentials):
    authenticator = ClientCredentialsAuthenticator(config={"shop": TEST_SHOP, "credentials": credentials})
    with pytest.raises(ClientCredentialsTokenError, match="Missing client_id or client_secret"):
        authenticator._exchange_credentials_for_token()


@pytest.mark.parametrize(
    "status_code, expected_error",
    [
        pytest.param(401, "Invalid client credentials", id="invalid_credentials"),
        pytest.param(403, "Access denied", id="access_denied"),
        pytest.param(404, "not found", id="store_not_found"),
        pytest.param(500, "HTTP error 500", id="server_error"),
    ],
)
def test_exchange_token_http_errors(config_client_credentials, requests_mock, status_code, expected_error):
    requests_mock.post(TOKEN_URL, status_code=status_code)
    authenticator = ClientCredentialsAuthenticator(config=config_client_credentials)
    with pytest.raises(ClientCredentialsTokenError, match=expected_error):
        authenticator._exchange_credentials_for_token()


def test_exchange_token_network_error(config_client_credentials, requests_mock):
    requests_mock.post(TOKEN_URL, exc=requests.exceptions.ConnectionError)
    authenticator = ClientCredentialsAuthenticator(config=config_client_credentials)
    with pytest.raises(ClientCredentialsTokenError, match="Network error"):
        authenticator._exchange_credentials_for_token()


@pytest.mark.parametrize(
    "response",
    [
        pytest.param({"json": {"other": "value"}}, id="missing_token"),
        pytest.param({"text": "not-json"}, id="malformed_json"),
    ],
)
def test_exchange_token_malformed_response(config_client_credentials, requests_mock, response):
    requests_mock.post(TOKEN_URL, **response)
    authenticator = ClientCredentialsAuthenticator(config=config_client_credentials)
    with pytest.raises(ClientCredentialsTokenError, match="Missing or malformed access_token"):
        authenticator._exchange_credentials_for_token()


def test_get_access_token_caches_and_refreshes(config_client_credentials, requests_mock):
    requests_mock.post(
        TOKEN_URL,
        [
            {"json": {"access_token": "token_1", "expires_in": 86400}},
            {"json": {"access_token": "token_2", "expires_in": 86400}},
        ],
    )
    authenticator = ClientCredentialsAuthenticator(config=config_client_credentials)

    assert authenticator.get_access_token() == "token_1"
    assert authenticator.get_access_token() == "token_1"
    assert requests_mock.call_count == 1

    authenticator._token_expiry = time.time() + 100
    assert authenticator.get_access_token() == "token_2"
    assert requests_mock.call_count == 2


def test_shopify_authenticator_uses_client_credentials(config_client_credentials, requests_mock):
    requests_mock.post(TOKEN_URL, json={"access_token": "cc_token", "expires_in": 86400})
    authenticator = ShopifyAuthenticator(config=config_client_credentials)

    assert authenticator.get_auth_header() == {"X-Shopify-Access-Token": "cc_token"}
    assert authenticator.get_auth_header() == {"X-Shopify-Access-Token": "cc_token"}
    assert requests_mock.call_count == 1


def test_connection_check_surfaces_client_credentials_error(config_client_credentials, requests_mock):
    requests_mock.post(TOKEN_URL, status_code=401)
    config_client_credentials["authenticator"] = ShopifyAuthenticator(config=config_client_credentials)

    assert ConnectionCheckTest(config_client_credentials).test_connection() == (
        False,
        "Invalid client credentials. Please verify your Client ID and Client Secret.",
    )
