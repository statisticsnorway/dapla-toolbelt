from datetime import timedelta
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from gcsfs.retry import HttpError
from google.auth._helpers import utcnow

from dapla import pandas as dp
from dapla.gcs import GCSFileSystem


def test_instance() -> None:
    # Chack that instantiation works with the current version of pyarrow
    client = GCSFileSystem()
    assert client is not None


@pytest.mark.timeout(
    30
)  # Times the test out after 30 sec, this is will happen if a deadlock happens
@patch.dict("dapla.auth.os.environ", {"OIDC_TOKEN": "fake-token"}, clear=True)
@patch.dict(
    "dapla.auth.os.environ", {"DAPLA_TOOLBELT_FORCE_TOKEN_EXCHANGE": "1"}, clear=True
)
@patch("dapla.auth.AuthClient.fetch_google_token")
def test_gcs_deadlock(mock_fetch_google_token: Mock) -> None:
    # When overriding the refresh method we experienced a deadlock, resulting in the credentials never being refreshed
    # This test checks that the credentials object is updated on refresh
    # and that it proceeds to the next step when a valid token is provided.

    mock_fetch_google_token.side_effect = [
        ("FakeToken1", utcnow()),
        ("FakeToken2", utcnow()),
        ("FakeToken3", utcnow()),
        ("FakeToken4", utcnow()),
        ("FakeToken5Valid", utcnow() + timedelta(seconds=30)),
    ]

    gcs_path = "gs://ssb-dapla-pseudo-data-produkt-test/integration_tests_data/personer.parquet"
    with pytest.raises(
        HttpError
    ) as exc_info:  # Since we supply invalid credentials an error should be raised
        dp.read_pandas(gcs_path)
    assert "Invalid Credentials" in str(exc_info.value)
    assert (
        mock_fetch_google_token.call_count == 5
    )  # mock_fetch_google_token is called as part of refresh
    # until a token that has not expired is returned
