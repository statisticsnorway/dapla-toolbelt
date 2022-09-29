#!/usr/bin/env python3

from dapla.statbank import StatbankTransfer, StatbankUttrekksBeskrivelse
import pandas as pd
import pytest
from unittest import mock

# Fake Auth
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
    return bytes("Basic U1NCLXBlcnNvbi00NTY6b0R3WHRhM0RYZEVHWVovRllJUm92dz09", "utf8")

# Fake data
@pytest.fixture
def fake_data():
    return pd.DataFrame({})

@pytest.fixture
def fake_body():
    return """
    """


# Our only get-request is for the "uttrekksbeskrivelse"
@mock.patch('requests.get')
def test_uttrekksbeskrivelse_init():
    uttrekk = StatbankUttrekksBeskrivelse()
    # last thing to get filled during __init__ is .kodelister, check that dict has length
    assert len(uttrekk.kodelister)
    
def test_uttrekksbeskrivelse_validate_data_wrong_deltabell_count():
    ...
    
def test_uttrekksbeskrivelse_validate_data_wrong_col_count():
    ...
    
def test_uttrekksbeskrivelse_validate_data_codes_outside_beskrivelse():
    ...
    
    
    

    
# Post requests both to dapla-key service and to the transfer API...
@mock.patch('requests.post')
def test_transfer_correct_entry():
    transfer = StatbankTransfer()
    
    # "Lastenummer" is one of the last things set by __init__ and signifies a correctly loaded data-transfer.
    # Is also used to build urls to webpages showing the ingestion status
    assert transfer.lastenummer.isdigit()
    
def test_transfer_no_auth_residuals():
    # Do a search for the key, password, and ciphered auth in the returned object.
    # Important to remove any traces of these before object is handed to user
    
    # Username should be in object (checks integrity of object, and validity of search) 
    assert len(search__dict__(transfer, fake_user))
    
    # Make sure none of these are in the object for security
    assert 0 == len(search__dict__(transfer, fake_key))
    assert 0 == len(search__dict__(transfer, fake_pass))
    assert 0 == len(search__dict__(transfer, fake_auth[:15]))
    
def search__dict__(obj, searchterm: str, path = "root", keep = {}):
    """ Recursive search through all nested objects having a __dict__-attribute"""
    if hasattr(obj, "__dict__"):
        for key, elem in obj.__dict__.items():
            if hasattr(elem, "__dict__"):
                path = path + "/" + key
                keep = search__dict__(elem, searchterm, path=path, keep=keep)
            if searchterm.lower() in str(elem).lower() or searchterm.lower() in str(key).lower():
                keep[path + "/" + key] = elem
    return keep


def test_transfer_validation_error():
    # Alter in-data to introduce error which causes the validation-process to error-out
    ...

def test_transfer_auth_error():
    # Sending in the wrong password, make sure its handled elegantly
    with pytest.raises(Exception):
        ...
        
        
