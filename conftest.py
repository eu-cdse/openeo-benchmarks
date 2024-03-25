import os
import pytest
import requests
import openeo

def get_openeo_base_url():
    try:
        backend_url = os.environ["OPENEO_BACKEND_URL"].rstrip("/")
    except Exception:
        raise RuntimeError("Environment variable 'ENDPOINT' should be set"
                           " with URL pointing to OpenEO backend to test against"
                           " (e.g. 'http://localhost:8080/' or 'http://openeo-dev.vgt.vito.be/')")
    return backend_url.rstrip("/")


@pytest.fixture
def requests_session(request) -> requests.Session:
    """
    Fixture to create a `requests.Session` that automatically injects a query parameter in API URLs
    referencing the currently running test.
    Simplifies cross-referencing between integration tests and flask/YARN logs
    """
    session = requests.Session()
    session.params["_origin"] = f"{request.session.name}/{request.node.name}"
    return session


@pytest.fixture
def connection(get_openeo_base_url) -> openeo.Connection:
    return openeo.connect(session=get_openeo_base_url)


@pytest.fixture
def auth_connection(connection, capfd) -> openeo.Connection:
    """
    Fixture to authenticate the connection,
    uses `authenticate_oidc`, which works by default for local development (e.g. device flow or refresh tokens)
    but also supports (since python client version 0.18.0) client credentials auth
    if the appropriate env vars are set (OPENEO_AUTH_METHOD, OPENEO_AUTH_CLIENT_ID, OPENEO_AUTH_CLIENT_SECRET,
    and OPENEO_AUTH_PROVIDER_ID)
    """
    # Temporarily disable output capturing, to make sure that the OIDC device code instructions are shown.
    with capfd.disabled():
        # Use a shorter max poll time by default
        # to alleviate the default impression that the test seem to hang
        # because of the OIDC device code poll loop.
        max_poll_time = int(os.environ.get("OPENEO_OIDC_DEVICE_CODE_MAX_POLL_TIME") or 30)
        connection.authenticate_oidc(max_poll_time=max_poll_time)
    return connection

