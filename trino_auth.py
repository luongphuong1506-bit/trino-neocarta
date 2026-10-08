"""Authentication for the Trino connection, configured through environment variables.

TRINO_AUTH selects the mode:

* ``none`` (default) - no authentication (local docker test stack).
* ``oauth2`` - Trino's OAuth2 login flow: the client prints/opens the IdP login URL in a
  browser, then polls Trino for the token. Works with any Trino configured with
  ``http-server.authentication.type=oauth2``; you log in once per run.
* ``client_credentials`` - non-interactive: fetch an access token from the IdP token
  endpoint with a service-account client (OAuth2 client-credentials grant) and refresh it
  before it expires. ``OAUTH2_TOKEN_URL``, ``OAUTH2_CLIENT_ID``, ``OAUTH2_CLIENT_SECRET``,
  optional ``OAUTH2_SCOPE``. (``keycloak`` and the ``KEYCLOAK_*`` names are accepted aliases.)
* ``jwt`` - send a ready-made access token: ``TRINO_JWT_TOKEN``.

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


class _ClientCredentialsBearer(AuthBase):
    """requests auth hook that attaches an OAuth2 access token, refreshing it when due."""

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
            # IdPs return {"error": ..., "error_description": ...}; never log the secret.
            raise RuntimeError(f"OAuth2 token request failed ({resp.status_code}): {resp.text[:300]}")
        body = resp.json()
        self._token = body["access_token"]
        self._expires_at = time.monotonic() + int(body.get("expires_in", 300))
        log.info("Obtained OAuth2 access token (expires in %ss)", body.get("expires_in"))

    def token(self) -> str:
        with self._lock:
            if self._token is None or time.monotonic() >= self._expires_at - _REFRESH_MARGIN_S:
                self._fetch()
            return self._token

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        r.headers["Authorization"] = "Bearer " + self.token()
        return r


class ClientCredentialsAuthentication(Authentication):
    """trino.auth.Authentication that keeps a fresh client-credentials access token."""

    def __init__(self, token_url: str, client_id: str, client_secret: str, scope: str | None = None, verify=True):
        self._bearer = _ClientCredentialsBearer(token_url, client_id, client_secret, scope, verify)

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


def _env(*names: str) -> str | None:
    """First non-empty value among ``names`` (new name first, legacy aliases after)."""
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def _require(*names: str) -> str:
    value = _env(*names)
    if not value:
        raise SystemExit(f"{names[0]} is required for TRINO_AUTH={os.getenv('TRINO_AUTH')}")
    return value


def trino_connection_kwargs() -> dict:
    """Keyword arguments for ``trino.dbapi.connect`` derived from the environment."""
    mode = (os.getenv("TRINO_AUTH") or "none").lower()
    verify = _verify_from_env()
    kwargs: dict = {"http_scheme": os.getenv("TRINO_HTTP_SCHEME") or "http", "verify": verify}

    if mode == "none":
        return kwargs
    if mode == "oauth2":
        # Login URL is printed and opened in the default browser; the token is cached for
        # the lifetime of the connection (or in the OS keyring if `keyring` is installed).
        kwargs["auth"] = OAuth2Authentication()
    elif mode in ("client_credentials", "keycloak"):
        kwargs["auth"] = ClientCredentialsAuthentication(
            token_url=_require("OAUTH2_TOKEN_URL", "KEYCLOAK_TOKEN_URL"),
            client_id=_require("OAUTH2_CLIENT_ID", "KEYCLOAK_CLIENT_ID"),
            client_secret=_require("OAUTH2_CLIENT_SECRET", "KEYCLOAK_CLIENT_SECRET"),
            scope=_env("OAUTH2_SCOPE", "KEYCLOAK_SCOPE"),
            verify=verify,
        )
    elif mode == "jwt":
        kwargs["auth"] = JWTAuthentication(_require("TRINO_JWT_TOKEN"))
    else:
        raise SystemExit(f"Unknown TRINO_AUTH={mode!r} (expected none, oauth2, client_credentials or jwt)")

    if kwargs["http_scheme"] != "https":
        log.warning("Sending credentials over plain http (TRINO_HTTP_SCHEME=http) - only for testing")
        kwargs["allow_insecure_auth"] = True
    return kwargs
