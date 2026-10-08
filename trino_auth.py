"""Authentication for the Trino connection, configured through environment variables.

TRINO_AUTH selects the mode:

* ``none`` (default) - no authentication (local docker stack).
* ``jwt`` - send a ready-made access token: ``TRINO_JWT_TOKEN``.
* ``keycloak`` - fetch tokens from Keycloak with the client-credentials grant (service
  account, no browser) and refresh them before they expire:
  ``KEYCLOAK_TOKEN_URL`` (``https://<host>/realms/<realm>/protocol/openid-connect/token``),
  ``KEYCLOAK_CLIENT_ID``, ``KEYCLOAK_CLIENT_SECRET``, optional ``KEYCLOAK_SCOPE``.
* ``oauth2`` - interactive login: Trino redirects to Keycloak and the client opens a browser.

Transport: ``TRINO_HTTP_SCHEME`` (``https`` when authenticating; the client refuses to send
credentials over plain http) and ``TRINO_VERIFY`` (``true`` | ``false`` | path to a CA bundle).
"""

from __future__ import annotations

import logging
import os
import threading
import time

import requests
from requests import PreparedRequest, Session
from requests.auth import AuthBase
from trino.auth import Authentication, JWTAuthentication, OAuth2Authentication

log = logging.getLogger(__name__)

# Refresh this many seconds before the token's expiry so in-flight requests never carry
# an expired token (Keycloak access tokens live 5 minutes by default).
_REFRESH_MARGIN_S = 30


class _KeycloakBearer(AuthBase):
    """requests auth hook that attaches a Keycloak access token, refreshing it when due."""

    def __init__(self, token_url: str, client_id: str, client_secret: str, scope: str | None, verify) -> None:
        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.verify = verify
        self._token: str | None = None
        self._expires_at = 0.0
        self._lock = threading.Lock()  # the trino client may send heartbeats from another thread

    def _fetch(self) -> None:
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        if self.scope:
            data["scope"] = self.scope
        resp = requests.post(self.token_url, data=data, timeout=30, verify=self.verify)
        if resp.status_code != 200:
            # Keycloak returns {"error": ..., "error_description": ...}; never log the secret.
            raise RuntimeError(f"Keycloak token request failed ({resp.status_code}): {resp.text[:300]}")
        body = resp.json()
        self._token = body["access_token"]
        self._expires_at = time.monotonic() + int(body.get("expires_in", 300))
        log.info("Obtained Keycloak access token (expires in %ss)", body.get("expires_in"))

    def token(self) -> str:
        with self._lock:
            if self._token is None or time.monotonic() >= self._expires_at - _REFRESH_MARGIN_S:
                self._fetch()
            return self._token

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        r.headers["Authorization"] = "Bearer " + self.token()
        return r


class KeycloakClientCredentialsAuthentication(Authentication):
    """trino.auth.Authentication that keeps a fresh Keycloak service-account token."""

    def __init__(self, token_url: str, client_id: str, client_secret: str, scope: str | None = None, verify=True):
        self._bearer = _KeycloakBearer(token_url, client_id, client_secret, scope, verify)

    def set_http_session(self, http_session: Session) -> Session:
        http_session.auth = self._bearer
        return http_session


def _verify_from_env():
    value = os.getenv("TRINO_VERIFY", "true")
    if value.lower() in ("true", "1", "yes"):
        return True
    if value.lower() in ("false", "0", "no"):
        return False
    return value  # path to a CA bundle


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Environment variable {name} is required for TRINO_AUTH={os.getenv('TRINO_AUTH')}")
    return value


def trino_connection_kwargs() -> dict:
    """Keyword arguments for ``trino.dbapi.connect`` derived from the environment."""
    mode = os.getenv("TRINO_AUTH", "none").lower()
    verify = _verify_from_env()
    kwargs: dict = {"http_scheme": os.getenv("TRINO_HTTP_SCHEME", "http"), "verify": verify}

    if mode == "none":
        return kwargs
    if mode == "jwt":
        kwargs["auth"] = JWTAuthentication(_require("TRINO_JWT_TOKEN"))
    elif mode == "keycloak":
        kwargs["auth"] = KeycloakClientCredentialsAuthentication(
            token_url=_require("KEYCLOAK_TOKEN_URL"),
            client_id=_require("KEYCLOAK_CLIENT_ID"),
            client_secret=_require("KEYCLOAK_CLIENT_SECRET"),
            scope=os.getenv("KEYCLOAK_SCOPE"),
            verify=verify,
        )
    elif mode == "oauth2":
        kwargs["auth"] = OAuth2Authentication()
    else:
        raise SystemExit(f"Unknown TRINO_AUTH={mode!r} (expected none, jwt, keycloak or oauth2)")

    if kwargs["http_scheme"] != "https":
        log.warning("Sending credentials over plain http (TRINO_HTTP_SCHEME=http) - only for testing")
        kwargs["allow_insecure_auth"] = True
    return kwargs
