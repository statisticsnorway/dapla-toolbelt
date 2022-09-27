from dapla.statbank import StatbankTransfer, StatbankUttrekksBeskrivelse
import pytest
from unittest import mock


@pytest.fixture
def fake_key():
    return "16-Chars_LongKey"
@pytest.fixture
def fake_user():
    return "SSB-person-456"
@pytest.fixture
def fake_pass():
    return "coConU7s6"
@pytest.fixture
def fake_auth():
    return "Basic "

@pytest.fixture
def beskrivelse_error_response():
    return ""
@pytest.fixture
def beskrivelse_correct_response():
    return ""

@pytest.fixture
def transfer_error_response():
    return ""
@pytest.fixture
def transfer_correct_response():
    return ""

# Our only get-request is for the "uttrekksbeskrivelse"
@mock.patch('requests.get')
def test_uttrekksbeskrivelse_init():
    ...
    # last thing to get filled during __init__ is .kodelister, check that dict has length

# Our only post-request is for actually transferring the data to StatBanken
@mock.patch('requests.post')
def test_transfer_correct_entry():
    ...
def test_transfer_validation_error():
    ...
