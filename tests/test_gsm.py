from unittest.mock import Mock

from pytest_mock import MockerFixture

from dapla.gsm import get_secret_version

PKG = "dapla.gsm"


def test_get_secret_version(mocker: MockerFixture) -> None:
    mock_smclient = mocker.patch(f"{PKG}.SecretManagerServiceClient")
    mock_authclient = mocker.patch(f"{PKG}.AuthClient")

    fake_creds = Mock()
    mock_authclient.fetch_google_credentials.return_value = fake_creds

    project_id = "tester-a92f"
    shortname = "supersecret"

    get_secret_version(project_id=project_id, shortname=shortname)
    mock_smclient.assert_called_once_with(credentials=fake_creds)
