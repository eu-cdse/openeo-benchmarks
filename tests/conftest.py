import os

import openeo
import pytest


@pytest.fixture
def auth_connection(capfd) -> openeo.Connection:
    """
    Fixture to create an authenticate the connection to the backend under test.

    Authentication is based on `authenticate_oidc`, which works by default for local development (e.g. device flow or refresh tokens)
    but also supports (since python client version 0.18.0) client credentials auth
    if the appropriate env vars are set (OPENEO_AUTH_METHOD, OPENEO_AUTH_CLIENT_ID, OPENEO_AUTH_CLIENT_SECRET,
    and OPENEO_AUTH_PROVIDER_ID)
    """
    openeo_backend_url = os.environ.get(
        "OPENEO_BACKEND_URL",
        "https://openeo.dataspace.copernicus.eu/"
    )

    connection = openeo.connect(openeo_backend_url)

    # Temporarily disable output capturing, to make sure that the OIDC device code instructions are shown.
    with capfd.disabled():
        # Use a shorter max poll time by default
        # to alleviate the default impression that the test seem to hang
        # because of the OIDC device code poll loop.
        max_poll_time = int(os.environ.get("OPENEO_OIDC_DEVICE_CODE_MAX_POLL_TIME") or 30)
        connection.authenticate_oidc(max_poll_time=max_poll_time)
    return connection
