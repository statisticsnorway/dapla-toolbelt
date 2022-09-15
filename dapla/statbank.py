#!/usr/bin/env python3

# Local imports
from AESECBPKCS5Padding import AESECBPKCS5Padding

# Standard library
import os
import getpass
import urllib.parse
import base64
from datetime import datetime as dt
from datetime import timedelta as td
from pathlib import Path
import json
# External packages
import requests as r
from requests.exceptions import ConnectionError
import pandas as pd
from dotenv import load_dotenv
load_dotenv()


def dfs_to_statbank(df: pd.DataFrame,
                    lastebruker: str,
                    tabellid: str = None,
                    filnavn: str = None,
                    database: str = 'PROD',
                    bruker_trebokstaver: str = getpass.getuser(), 
                    publisering: dt = (dt.now() + td(days=1)).strftime('%Y-%m-%d'),
                    fagansvarlig1: str = getpass.getuser(),
                    fagansvarlig2: str = getpass.getuser(),
                    auto_overskriv_data: str = '1',
                    auto_godkjenn_data: str = '2'
                    ):

    # Validate dataframe-specifics, df should be a single dataframe, or list of dataframes
    if not isinstance(df, pd.DataFrame):
        try:
            for elem in df:
                if not isinstance(elem, df):
                    raise TypeError("Expecting DataFrame or iterable of dataframes, what is first parameter to function set as?")
        except Exception as e:
            print("Having trouble iterating over non-dataframe object? Sent in as first parameter to function dfs_to_statbank")
            raise e

    # Validate other parameters generally
    validate_original_parameters(locals())

    # Requirements for both requests
    urls = build_urls(database)
    headers = build_headers(lastebruker, database)

    # Try-block is to ensure that as much auth as possible is deleted right after it is not needed anymore
    try:
        # Get-request uttaksbeskrivelse (entry-request metadata)
        filbeskrivelse = get_filbeskrivelse(tabellid, urls, headers)

        # Build main body of post-request from specific input
        body = build_body_from_dataframes(df, filbeskrivelse)


        validate_body_filbeskrivelse(df, filbeskrivelse)
        # Build rest of post-request  
        params = build_params_filbeskrivelse(filbeskrivelse['Huvudtabell'], publisering, bruker_trebokstaver, 
                                            fagansvarlig1, fagansvarlig2, auto_overskriv_data, auto_godkjenn_data)
        url_load_params = urls['loader'] + urllib.parse.urlencode(params)
        response = r.post(url_load_params, headers = headers, data = body)
    except Exception as e:
        raise e
    finally:
        # Cleanup leftovers that may contain auth or password
        del headers
        del response.request.headers

    response = handle_response(response, urls)
    return response


# VALIDATION

def validate_original_parameters(params):

    if not params['tabellid'].isdigit() or len(params['tabellid']) != 5:
        raise ValueError("Tabellid må være tall, som en streng, og 5 tegn lang.")

    if not isinstance(params['lastebruker'], str) or not params['lastebruker']:
        raise ValueError("Du må sette en lastebruker korrekt")

    databases = ['PROD', 'TEST', 'QA', 'UTV']
    database = params['database'].upper()
    if database not in databases:
        raise ValueError(f"{database} not among {*databases,}")

    for tbf in [params['bruker_trebokstaver'], params['fagansvarlig1'], params['fagansvarlig2']]:
        if len(tbf) != 3 or not isinstance(tbf, str):
            raise ValueError(f'Brukeren {tbf} - "trebokstavsforkortelse" - må være tre bokstaver...')

    if not isinstance(params['publisering'], dt):
        if not valid_date_form(params['publisering']):
            raise ValueError("Skriv inn datoformen for publisering som 1900-01-01")

    if not params['auto_overskriv_data'] in ['0', '1']:
        raise ValueError("(Strengverdi) Sett auto_overskriv_data til enten '0' = ingen overskriving (dubletter gir feil), eller  '1' = automatisk overskriving")

    if not params['auto_godkjenn_data'] in ['0', '1', '2']:
        raise ValueError("(Strengverdi) Sett auto_godkjenn_data til enten '0' = manuell, '1' = automatisk (umiddelbart), eller '2' = JIT-automatisk (just-in-time)")


def valid_date_form(date: str) -> bool:
    if (date[:4] + date[5:7] + date[8:]).isdigit() and (date[4]+date[7]) == "--":
        return True
    return False


def validate_df_filbeskrivelse(df: pd.DataFrame, filbeskrivelse:Path) -> bool:
    print("Validating df against filbeskrivelse TODO")


def validate_body_filbeskrivelse(body, filbeskrivelse):
    pass


# BUILDING

def build_body_from_dataframes(dfs, filbeskrivelse: dict) -> str:
    pass


def build_params_filbeskrivelse(
                                hovedtabell: str, 
                                publisering: dt,
                                bruker_trebokstaver: str,
                                fagansvarlig1: str,
                                fagansvarlig2: str,
                                auto_overskriv_data: str,
                                auto_godkjenn_data: str) -> dict:

    if isinstance(publisering, dt):
        publisering = publisering.strftime('%Y-%m-%d')

    return {
        'initialier': bruker_trebokstaver,
        'hovedtabell': hovedtabell,
        'publiseringsdato': publisering,
        'fagansvarlig1': fagansvarlig1,
        'fagansvarlig2': fagansvarlig2,
        'auto_overskriv_data': auto_overskriv_data,
        'auto_godkjenn_data': auto_godkjenn_data,
    }


def build_df_body_filbeskrivelse(df: pd.DataFrame, filbeskrivelse: dict) -> str:
    body = body.replace("\n", "\r\n")
    return body


def build_urls(database: str) -> dict:
    BASE_URLS = {
        'PROD': "https://i.ssb.no/",
        'TEST': "https://i.test.ssb.no/",
        'QA': "https://i.qa.ssb.no/", # Fins denne?
        'UTV': "https://i.utv.ssb.no/",
    }
    base_url = BASE_URLS[database]
    END_URLS = {
        'loader': 'statbank/sos/v1/DataLoader?',
        'uttak': 'statbank/sos/v1/uttaksbeskrivelse?',
        'gui': 'lastelogg/gui/',
        'api': 'lastelogg/api/',
    }
    return {k:base_url+v for k,v in END_URLS.items()}


def build_headers(lastebruker: str, database: str) -> dict:
    return {
        'Authorization': build_auth(lastebruker, database),
        'Content-Type': 'multipart/form-data; boundary=12345',
        'Connection': 'keep-alive',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept': r'*/*',
    }


def build_auth(lastebruker: str, database: str):
    # Get keys from environment?
    KEYS = {
        'PROD': "",
        'TEST': "",
        'QA': '',
        'UTV': '',
    }
    if not KEYS['TEST']:
        KEYS['TEST'] = os.environ["keys_test"]
    key = KEYS[database]
    # 
    encrypted_password = encrypt_password(key)
    username_encryptedpassword = bytes(lastebruker, 'UTF-8') + bytes(':', 'UTF-8') + bytes(encrypted_password, 'UTF-8')
    del KEYS, key  # Remove from memory at earliest convenience
    return bytes('Basic ', 'UTF-8') + base64.b64encode(username_encryptedpassword)


def encrypt_password(key: str) -> str:
    if len(key) != 16:
        raise ValueError('Key must be of length 16')
    cipher = AESECBPKCS5Padding(key, "b64")
    if 'test_pass' in os.environ.keys():
        return cipher.encrypt(os.environ['test_pass'])
    return cipher.encrypt(getpass.getpass("Lastpassord:"))


# REQUEST HANDLING

def get_filbeskrivelse(tabellid: str, urls: dict, headers: str) -> dict:
    filbeskrivelse_url = urls['uttak']+"tableId="+tabellid
    filbeskrivelse = r.get(filbeskrivelse_url, headers=headers)
    if filbeskrivelse.status_code != 200:
        raise ConnectionError(filbeskrivelse)
    filbeskrivelse = json.loads(filbeskrivelse.text)
    print(f"Hentet uttaksbeskrivelsen for {filbeskrivelse['Huvudtabell']}, med tabellid: {tabellid} den {filbeskrivelse['Uttaksbeskrivelse_lagd']}")
    return filbeskrivelse


def handle_response(response, URLS):

    # Parse result
    response_json = json.loads(response.text)
    response_message = response_json['TotalResult']['Message']

    if response.status_code == 200:
        oppdragsnummer = response_message.split("lasteoppdragsnummer:")[1].split(" =")[0]
        if not oppdragsnummer.isdigit():
            raise ValueError(f"Lasteoppdragsnummer: {oppdragsnummer} er ikke ett rent nummer.")

        publiseringdato = dt.strptime(response_message.split("Publiseringsdato '")[1].split("',")[0], "%d.%m.%Y %H:%M:%S")
        publiseringstime = int(response_message.split("Publiseringstid '")[1].split(":")[0])
        publiseringsminutt = int(response_message.split("Publiseringstid '")[1].split(":")[1].split("'")[0])
        publisering = publiseringdato + td(0, (publiseringstime*3600+publiseringsminutt*60))
        print(f"Publisering satt til: {publisering.strftime('%Y-%m-%d %H:%M')}")
        print(f"Følg med på lasteloggen (tar noen minutter): {URLS['gui'] + oppdragsnummer}")
        print(f"Og evt APIen?: {URLS['api'] + oppdragsnummer}")
        return response_json
    else:
        #print(response_json)
        raise ConnectionError(response_message)


if __name__ == '__main__':
    print("Only for importing?")