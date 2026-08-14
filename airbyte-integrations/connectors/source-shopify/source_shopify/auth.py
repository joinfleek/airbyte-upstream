#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
import time
from typing import Any, Dict, Mapping, Optional

import requests

from airbyte_cdk.sources.streams.http.requests_native_auth import TokenAuthenticator


logger = logging.getLogger("airbyte")


class MissingAccessTokenError(Exception):
    """
    Raised when the token is `None` instead of the real value
    """


class ClientCredentialsTokenError(Exception):
    """Raised when Shopify rejects a client-credentials token exchange."""


class NotImplementedAuth(Exception):
    """Not implemented Auth option error"""

    logger = logging.getLogger("airbyte")

    def __init__(self, auth_method: str = None):
        self.message = f"Not implemented Auth method = {auth_method}"
        super().__init__(self.logger.error(self.message))


class ClientCredentialsAuthenticator:
    """Exchange Shopify Dev Dashboard credentials for short-lived access tokens."""

    TOKEN_REFRESH_BUFFER_SECONDS = 300
    DEFAULT_TOKEN_EXPIRY_SECONDS = 86399

    def __init__(self, config: Mapping[str, Any]):
        self.config = config
        self._access_token: Optional[str] = None
        self._token_expiry: Optional[float] = None

    def _get_shop_name(self) -> str:
        """Return the normalized shop handle from connector configuration."""
        return self.config.get("shop", "").replace(".myshopify.com", "")

    def _exchange_credentials_for_token(self) -> tuple[str, float]:
        credentials = self.config.get("credentials", {})
        client_id = credentials.get("client_id")
        client_secret = credentials.get("client_secret")
        shop = self._get_shop_name()

        if not client_id or not client_secret:
            raise ClientCredentialsTokenError("Missing client_id or client_secret in credentials")

        try:
            response = requests.post(
                f"https://{shop}.myshopify.com/admin/oauth/access_token",
                data={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "grant_type": "client_credentials",
                },
                timeout=30,
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            status_code = error.response.status_code if error.response is not None else None
            if status_code == 401:
                message = "Invalid client credentials. Please verify your Client ID and Client Secret."
            elif status_code == 403:
                message = "Access denied. Please ensure your app is installed on the store and has the required scopes."
            elif status_code == 404:
                message = f"Store '{shop}' not found. Please verify your shop name."
            else:
                message = f"HTTP error {status_code} while exchanging client credentials."
            raise ClientCredentialsTokenError(message) from error
        except requests.exceptions.RequestException as error:
            raise ClientCredentialsTokenError(
                "Network error while exchanging client credentials. Please check your network connection."
            ) from error

        try:
            result = response.json()
            access_token = result["access_token"]
            expires_in = result.get("expires_in", self.DEFAULT_TOKEN_EXPIRY_SECONDS)
            return access_token, time.time() + expires_in
        except (KeyError, TypeError, ValueError) as error:
            raise ClientCredentialsTokenError("Invalid response from Shopify token endpoint. Missing or malformed access_token.") from error

    def get_access_token(self) -> str:
        should_refresh = (
            self._access_token is None or self._token_expiry is None or time.time() > self._token_expiry - self.TOKEN_REFRESH_BUFFER_SECONDS
        )
        if should_refresh:
            logger.info("Refreshing Shopify client-credentials access token")
            self._access_token, self._token_expiry = self._exchange_credentials_for_token()
        return self._access_token


class ShopifyAuthenticator(TokenAuthenticator):
    """
    Making Authenticator to be able to accept Header-Based authentication.
    """

    def __init__(self, config: Mapping[str, Any]):
        self.config = config
        self._client_credentials_authenticator: Optional[ClientCredentialsAuthenticator] = None

    def get_auth_header(self) -> Mapping[str, Any]:
        auth_header: str = "X-Shopify-Access-Token"
        credentials: Dict = self.config.get("credentials", self.config.get("auth_method"))
        auth_method: str = credentials.get("auth_method")

        if auth_method in ["oauth2.0", "access_token"]:
            access_token = credentials.get("access_token")
            if access_token:
                return {auth_header: access_token}
            else:
                raise MissingAccessTokenError
        elif auth_method == "api_password":
            return {auth_header: credentials.get("api_password")}
        elif auth_method == "client_credentials":
            if self._client_credentials_authenticator is None:
                self._client_credentials_authenticator = ClientCredentialsAuthenticator(self.config)
            return {auth_header: self._client_credentials_authenticator.get_access_token()}
        else:
            raise NotImplementedAuth(auth_method)
