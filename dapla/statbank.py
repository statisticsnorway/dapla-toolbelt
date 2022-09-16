#!/usr/bin/env python3

# Todo
# - Which classes do we want? Hierarchy?
#   - StatbankTable (some shared properties with "getting from external API", tabellid, number of classification and value-columns etc.)
#   - UttaksBeskrivelse (without need to enter password twice?)
#   - StatbankRequest (valid header for both, but needs to swap to post/get and params)
# - Testing
# - Docstrings / Documentation

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
import pyarrow as pa

from dapla import AuthClient

# Can be removed when finished developing
from dotenv import load_dotenv
load_dotenv()




class StatbankTransfer(StatbankAuth):
    def __init__(self,
                data: pd.DataFrame,
                    lastebruker: str,
                    tabellid: str = None,
                    database: str = 'PROD',
                    bruker_trebokstaver: str = os.environ['JUPYTERHUB_USER'].split("@")[0], 
                    publisering: dt = (dt.now() + td(days=366)).strftime('%Y-%m-%d'),
                    fagansvarlig1: str = os.environ['JUPYTERHUB_USER'].split("@")[0],
                    fagansvarlig2: str = os.environ['JUPYTERHUB_USER'].split("@")[0],
                    auto_overskriv_data: str = '1',
                    auto_godkjenn_data: str = '2',
                    validation: bool = True):
        self.data = data
        self.lastebruker = lastebruker
        self.tabellid = tabellid
        self.hovedtabell = None
        self.database = database
        self.tbf = bruker_trebokstaver
        self.publisering = publisering
        self.fagansvarlig1 = fagansvarlig1
        self.fagansvarlig2 = fagansvarlig2
        self.overskriv_data = auto_overskriv_data
        self.godkjenn_data = auto_godkjenn_data
        self.validation = validation
        self.boundary = "12345"
        if validation: self._validate_original_parameters()
        
        self.urls = self._build_urls()
        self.params = self._build_params()
        self.headers = self._build_headers()
        try:
            self.filbeskrivelse = self._get_filbeskrivelse()  # Could be its own class?


            self.data_type, self.data_iter = self._identify_data_type()
            self.body = self._body_from_data()

            if validation: self._validate_body()

            self.response = self._handle_response()
        finally:
            del self.headers

        
    def _identify_data_type(self):
        try:
            iter(self.data)
            datatype = None
            for elem in self.data:
                if not datatype: 
                    datatype=type(elem)
                else:
                    if not datatype == type(elem):
                        raise TypeError("Data is iterable with non-uniform file types, needs to be uniform")
        except:
            return type(self.data), False
        else:
            return datatype, True

        
    def _body_from_data(self):
        # If data is string, check if valid body, or path(s)
            ## Read from paths into pa.Tables or pd.DataFrame? Stress-test which?
            ## Reset self.data_type to new datatype
        
        
        # If data is single pd.DataFrame or pa.Table, put into iterable, so code under works
        if not self.data_iter:
            self.data = [self.data]
        # We need the filenames in the body, and they must match up with amount of data-elements we have
        deltabeller_filnavn = [x['Filnavn'] for x in self.filbeskrivelse['DeltabellTitler']]
        if len(deltabeller_filnavn) != len(self.data):
            raise ValueError("Length mismatch between data-iterable and number of Uttaksbeskrivelse deltabellers filnavn.")
        # Data should be a iterable of pd.DataFrames or pa.Tables at this point, reshape to body
        for elem, filename in zip(self.data, deltabeller_filnavn):
            body += f"--{self.boundary}"
            body += f"\nContent-Disposition:form-data; filename={filename}"
            body += "\nContent-type:text/plain\n\n"
            if self.data_type == pd.DataFrame:
                body += elem.to_csv(sep=";", index = False, header = False)
            elif self.data_type == pa.Table:
                for line in elem.to_pylist():
                    body += ";".join(line.values()) + "\n"
            else:
                raise TypeError("Expecting Dataframe or Table at this point in code")
        body += f"\n--{self.boundary}--"
        body = body.replace("\n", "\r\n")  # Statbank likes this?
        return body
        
        
    def _validate_original_parameters(self):
        #if not self.tabellid.isdigit() or len(self.tabellid) != 5:
        #    raise ValueError("Tabellid må være tall, som en streng, og 5 tegn lang.")

        if not isinstance(self.lastebruker, str) or not self.lastebruker:
            raise ValueError("Du må sette en lastebruker korrekt")

        databases = ['PROD', 'TEST', 'QA', 'UTV']
        database = self.database.upper()
        if database not in databases:
            raise ValueError(f"{database} not among {*databases,}")

        for tbf in [self.tbf, self.fagansvarlig1, self.fagansvarlig2]:
            if len(tbf) != 3 or not isinstance(tbf, str):
                raise ValueError(f'Brukeren {tbf} - "trebokstavsforkortelse" - må være tre bokstaver...')

        if not isinstance(self.publisering, dt):
            if not self._valid_date_form(self.publisering):
                raise ValueError("Skriv inn datoformen for publisering som 1900-01-01")

        if not self.overskriv_data in ['0', '1']:
            raise ValueError("(Strengverdi) Sett overskriv_data til enten '0' = ingen overskriving (dubletter gir feil), eller  '1' = automatisk overskriving")

        if not self.godkjenn_data in ['0', '1', '2']:
            raise ValueError("(Strengverdi) Sett godkjenn_data til enten '0' = manuell, '1' = automatisk (umiddelbart), eller '2' = JIT-automatisk (just-in-time)")




    def _validate_body_filbeskrivelse(self):
        ...
        
    @staticmethod
    def _valid_date_form(date)
        if (date[:4] + date[5:7] + date[8:]).isdigit() and (date[4]+date[7]) == "--":
            return True
        return False
    
    
    def _build_params(self):
        if isinstance(self.publisering, dt):
            self.publisering = self.publisering.strftime('%Y-%m-%d')
        return {
            'initialier' : self.tbf,
            'hovedtabell': self.hovedtabell,
            'publiseringsdato': self.publisering,
            'fagansvarlig1': self.fagansvarlig1,
            'fagansvarlig2': self.fagansvarlig2,
            'auto_overskriv_data': self.overskriv_data,
            'auto_godkjenn_data': self.godkjenn_data,
        }
    
    
    def _build_urls(database: str) -> dict:
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

    
    def _get_filbeskrivelse(self):
        filbesk = StatbankUttrekksBeskrivelse(self.urls['uttak'], self.tabellid, self.headers)
        
        return filbesk


    
class StatbankUttrekksBeskrivelse(StatbankAuth):
    def __init__(self, url, tabellid, headers=None):
        self.url = url
        self.base = ""
        self.lagd = ""
        self.tabellid = tabellid
        self.hovedtabell = ""
        self.deltabelltitler = dict()
        self.variabler = dict()
        self.kodelister = dict()
        self.prikking = None
        if headers:
            self.headers = headers
        else:
            self.headers = self._build_headers()
        try:
            self._get_uttrekksbeskrivelse()
        finally:
            del self.headers#, headers
        self._split_attributes()
        
        
    def _get_uttrekksbeskrivelse(self):
        filbeskrivelse_url = self.url+"tableId="+self.tabellid
        filbeskrivelse = r.get(filbeskrivelse_url, headers=self.headers)
        if filbeskrivelse.status_code != 200:
            del self.headers
            raise ConnectionError(filbeskrivelse)
        # Also deletes / overwrites returned Auth-header from get-request
        filbeskrivelse = json.loads(filbeskrivelse.text)
        print(f"Hentet uttaksbeskrivelsen for {filbeskrivelse['Huvudtabell']}, med tabellid: {tabellid} den {filbeskrivelse['Uttaksbeskrivelse_lagd']}")        
        # reset tabellid and hovedkode after content of request
        self.filbeskrivelse = filbeskrivelse
        
    def _split_attributes(self):
        # Tabellid might have been "hovedkode" up to this point, as both are valid in the URI
        self.lagd = self.filbeskrivelse['Uttaksbeskrivelse_lagd']
        self.base = self.filbeskrivelse['base']
        self.tabellid = self.filbeskrivelse['TabellId']
        self.hovedtabell = self.filbeskrivelse['Huvudtabell']
        self.deltabelltitler = self.filbeskrivelse['DeltabellTitler']
        self.variabler = self.filbeskrivelse['deltabller']
        self.kodelister = self.filbeskrivelse['kodelister']
        if 'null_prikk_missing_kodeliste' in filbeskrivelse.keys():
            self.prikking = self.filbeskrivelse['null_prikk_missing_kodeliste']

        
    
class StatbankAuth:

    def _build_headers(self):
        return {
            'Authorization': self._build_auth(),
            'Content-Type': 'multipart/form-data; boundary=12345',
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept' : r'*/*',
            }
        
        
    def _build_auth(self):
        # Hør med Bjørn om hvordan dette skal implementeres for å sende passordet
        response = requests.post('http://dapla-statbank-authenticator.dapla.svc.cluster.local/encrypt',
              headers={
                  'Authorization': 'Bearer %s' % AuthClient.fetch_personal_token(),
                  'Content-type': 'application/json'
              }, json={"message" : self.database)})
        # Get key from response
        try:
            key = response.text['key']
        except Exception as e:
            raise e
        finally:
            del response
        # Encrypt password with key
        try:
            encrypted_password = self._encrypt_password(key)
        except Exception as e:
            raise e
        finally :
            del key
        # Combine with username
        username_encryptedpassword = bytes(lastebruker, 'UTF-8') + bytes(':', 'UTF-8') + bytes(encrypted_password, 'UTF-8')
        return bytes('Basic ', 'UTF-8') + base64.b64encode(username_encryptedpassword)

    @staticmethod
    def _encrypt_password(key):
        if len(key) != 16:
            raise ValueError('Key must be of length 16')
        try:
            cipher = AESECBPKCS5Padding(key, "b64")
        finally:
            del key
        return cipher.encrypt(getpass.getpass(f"Lastepassord:"))
    

if __name__ == '__main__':
    print("Only for importing?")