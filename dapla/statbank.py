#!/usr/bin/env python3

# Todo
# - Which classes do we want?

 Standard library
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
from pyarrow import csv as pa_csv

from dotenv import load_dotenv
load_dotenv()


class StatbankTransfer:
    def __init__(self,
                data: pd.DataFrame,
                    lastebruker: str,
                    tabellid: str = None,
                    database: str = 'PROD',
                    bruker_trebokstaver: str = os.environ['JUPYTERHUB_USER'].split("@")[0], 
                    publisering: dt = (dt.now() + td(days=1)).strftime('%Y-%m-%d'),
                    fagansvarlig1: str = os.environ['JUPYTERHUB_USER'].split("@")[0],
                    fagansvarlig2: str = os.environ['JUPYTERHUB_USER'].split("@")[0],
                    auto_overskriv_data: str = '1',
                    auto_godkjenn_data: str = '2',
                    validation: bool = True):
        self.data = data
        self.lastebruker = lastebruker
        self.tabellid = tabellid
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
        self.filbeskrivelse = self._get_filbeskrivelse()  # Could be its own class?
        
        
        self.data_type, self.data_iter = self._identify_data_type()
        self.body = self._body_from_data()

        if validation: self._validate_body()

        self.response = self._handle_response()

        
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
        ...


    def _validate_body_filbeskrivelse(self):
        ...
        
    @staticmethod
    def _valid_date_form(date)
        if (date[:4] + date[5:7] + date[8:]).isdigit() and (date[4]+date[7]) == "--":
            return True
        return False
    

    def _build_headers(self):
        ...
    def _build_auth(self):
        ...
    def _build_params(self):
        ...
    
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
        filbeskrivelse_url = self.urls['uttak']+"tableId="+self.tabellid
        filbeskrivelse = r.get(filbeskrivelse_url, headers=self.headers)
        if filbeskrivelse.status_code != 200:
            del self.headers
            raise ConnectionError(filbeskrivelse)
        filbeskrivelse = json.loads(filbeskrivelse.text)
        print(f"Hentet uttaksbeskrivelsen for {filbeskrivelse['Huvudtabell']}, med tabellid: {tabellid} den {filbeskrivelse['Uttaksbeskrivelse_lagd']}")        
        # reset tabellid and hovedkode after content of request
        # Tabellid might have been "hovedkode" up to this point, as both are valid in the URI
        self.tabellid = filbeskrivelse['TabellId']
        self.hovedtabell = filbeskrivelse['Huvudtabell']
        return filbeskrivelse


    def _post_ciphered_pass_key(self):        
        ...
    def 


if __name__ == '__main__':
    print("Only for importing?")