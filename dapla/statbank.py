#!/usr/bin/env python3

# Todo
# - Ability to send a constructed Uttrekksbeskrivelse into Transfer?
# - Validation on
#   - Time formats? ååååMmm = 2022M01
#   - Rounding of floats? And correct decimal-signifier in data?
#   - "Prikking"-columns contain only allowed codes
# - More Testing (Pytest + mocking requests)
# - Docstrings / Documentation

# Standard library
import os
import getpass
import base64
from datetime import datetime as dt
from datetime import timedelta as td
import json
import urllib.parse
# External packages
import requests as r
from requests.exceptions import ConnectionError
import pandas as pd
from pyjstat import pyjstat

# SSB-packages / local
from .auth import AuthClient

############################################
# Transferring data to Statbank from Dapla #
############################################

class StatbankAuth:
    """
    Parent class for shared behavior between Statbankens "Transfer-API" and "Uttaksbeskrivelse-API"
    ...

    Methods
    -------
    _decide_dapla_environ() -> str:
        If in Dapla-staging, should return "TEST", otherwise "PROD". 
    _build_headers() -> dict:
        Creates dict of headers needed in request to talk to Statbank-API
    _build_auth() -> str:
        Gets key from environment and encrypts password with key, combines it with username into expected Authentication header.
    _encrypt_request() -> str:
        Encrypts password with key from local service, url for service should be environment variables. Password is not possible to send into function. Because safety.
    _build_urls() -> dict:
        Urls will differ based environment variables, returns a dict of urls.
    __init__():
        is not implemented, as Transfer and UttrekksBeskrivelse both add their own.
    
    """
    @staticmethod
    def _decide_dapla_environ() -> str:
        if "staging" in os.environ["CLUSTER_ID"].lower():
            return "TEST"
        else:
            return "PROD"
        
    def _build_headers(self) -> dict:
        return {
            'Authorization': self._build_auth(),
            'Content-Type': 'multipart/form-data; boundary=12345',
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept' : r'*/*',
            }


    def _build_auth(self):
        response = self._encrypt_request()
        try:
            username_encryptedpassword = bytes(self.lastebruker, 'UTF-8') + bytes(':', 'UTF-8') + bytes(json.loads(response.text)['message'], 'UTF-8')
        finally:
            del response
        return bytes('Basic ', 'UTF-8') + base64.b64encode(username_encryptedpassword)

    @staticmethod
    def _encrypt_request():
        return r.post(os.environ['STATBANK_ENCRYPT_URL'],
                      headers={
                              'Authorization': f'Bearer {AuthClient.fetch_personal_token()}',
                              'Content-type': 'application/json'}, 
                      json={"message": getpass.getpass(f"Lastepassord:")}
                     )

    @staticmethod
    def _build_urls() -> dict:
        base_url = os.environ['STATBANK_BASE_URL']
        END_URLS = {
            'loader': 'statbank/sos/v1/DataLoader?',
            'uttak': 'statbank/sos/v1/uttaksbeskrivelse?',
            'gui': 'lastelogg/gui/',
            'api': 'lastelogg/api/',
        }
        return {k: base_url+v for k, v in END_URLS.items()}

    
class StatbankUttrekksBeskrivelse(StatbankAuth):
    """
    Class for talking with the "uttrekksbeskrivelses-API", which describes metadata about shape of data to be transferred.
    And metadata about the table itself in Statbankens system, like ID, name of codelists etc.
    ...

    Attributes
    ----------
    lastebruker : str
        Username for Statbanken, not the same as "tbf" or "common personal username" in other SSB-systems
    url : str
        Main url for transfer
    lagd : str
        Time of getting the Uttrekksbeskrivelse
    tabellid: str
        Originally the ID of the main table, which to get the Uttrekksbeskrivelse on, 
        but is reset based on the info in the Uttrekksbeskrivelse. 
        To compansate for the possibility of the user sending in "Hovedtabell"-name as tabellid.
        That should work also, probably...
    hovedtabell : str
        The name of the main table in Statbanken, not numbers, like the ID is.
    deltabelltitler : dict
        Names and descriptions of the individual "table-parts" that need to be sent in as different DataFrames.
    variabler : dict
        Metadata about the columns in the different table-parts.
    kodelister : dict
        Metadata about column-contents, like formatting on time, or possible values ("codes").
    prikking : dict
        Details around extra columns which describe main column's "prikking", meaning their suppression-type. 
    headers : dict
        The headers for the request, might be sent in from a StatbankTransfer-object.
    filbeskrivelse : dict
        The "raw" json returned from the API-get-request, loaded into a dict.
    
    Methods
    -------
    validate_dfs(data=pd.DataFrame, raise_errors=bool):
        Checks sent data against UttrekksBeskrivelse, raises errors at end of checking, if raise_errors not set to False.
    _get_uttrekksbeskrivelse():
        Handles the response from the API, prints some status.
    _make_request():
        Makes the actual get-request, split out for easier mocking
    _split_attributes():
        After a successful response, split recieved data into attributes for easier access
    __init__():
    
    """
    def __init__(self, tabellid, lastebruker, raise_errors = True, headers=None):
        self.lastebruker = lastebruker
        self.url = self._build_urls()['uttak']
        self.lagd = ""
        self.tabellid = tabellid
        self.raise_errors = raise_errors
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
            if hasattr(self, "headers"):
                del self.headers
        self._split_attributes()

    def __str__(self):
        deltabellnavn = ",\n".join([f"{i+1}: {x['Filnavn']}" for i, x in enumerate(self.deltabelltitler)])
        return f'Uttrekksbeskrivelse for statbanktabell {self.tabellid}. \nLastebruker: {self.lastebruker}. \nOversikt deltabeller:\n{deltabellnavn}'
        
    def __repr__(self):
        return f'StatbankUttrekksBeskrivelse(tabellid="{self.tabellid}", lastebruker="{self.lastebruker}")'
    
    def validate_dfs(self, data, raise_errors: bool = False) -> dict:
        validation_errors = {}
        print("validating...")
        ### Number deltabelltitler should match length of data-iterable
        if len(self.deltabelltitler) > 1:
            if not isinstance(data, list) or not isinstance(data, tuple):
                raise TypeError(f"""Please put multiple pandas Dataframes in a list as your data.
                These are your 'deltabeller', and the number & order of DataFrames should match this: 
                {self.deltabelltitler}
                """)
        elif len(self.deltabelltitler) == 1:
            if not isinstance(data, pd.DataFrame):
                raise TypeError("Only one deltabell, expecting one pandas Dataframe in as your data.")
            # For the code below to access the data correctly
            to_validate = [data]
        else:
            validation_errors["deltabell_num"] = ValueError("Deltabeller is shorter than one, weird. Make sure the uttaksbeskrivelse is ok.")

        ### Number of columns in data must match beskrivelse
        for deltabell_num, deltabell in enumerate(self.variabler):
            deltabell_navn = deltabell['deltabell']
            col_num = len(deltabell['variabler']) + len(deltabell['statistikkvariabler'])
            if "null_prikk_missing" in deltabell.keys():
                col_num += len(deltabell["null_prikk_missing"])
            if len(to_validate[deltabell_num].columns) != col_num:
                validation_errors[f"col_count_data_{deltabell_num}"] = ValueError(f"Expecting {col_num} columns in datapart {deltabell_num}: {deltabell_navn}")
        
        
        ### No values outside, warn of missing from codelists on categorical columns
        categorycode_outside = []
        categorycode_missing = []
        for kodeliste in self.kodelister:
            kodeliste_id = kodeliste['kodeliste']
            for deltabell in self.variabler:
                for i, deltabell2 in enumerate(self.deltabelltitler):
                    if deltabell2["Filnavn"] == deltabell["deltabell"]:
                        deltabell_nr = i + 1
                for variabel in deltabell["variabler"]:
                    if variabel["Kodeliste_id"] == kodeliste_id:
                        break
                else:
                    raise KeyError(f"Can't find {kodeliste_id} among deltabells variables.")
            #if 'SumIALtTotalKode' in kodeliste.keys():
                #print(kodeliste["SumIALtTotalKode"])
            col_unique = to_validate[deltabell_nr-1].iloc[:,int(variabel["kolonnenummer"])-1].unique()
            kod_unique = [i["kode"] for i in kodeliste['koder']]
            for kod in col_unique:
                if kod not in kod_unique:
                    categorycode_outside += [f"Code {kod} in data, but not in uttrekksbeskrivelse, add to statbank admin? From column number {variabel['kolonnenummer']}, in deltabell number {deltabell_nr}, ({deltabell['deltabell']})"]
            for kod in kod_unique:
                if kod not in col_unique:
                    categorycode_missing += [f"Code {kod} missing from column number {variabel['kolonnenummer']}, in deltabell number {deltabell_nr}, ({deltabell['deltabell']})"]
        ### Check rounding on floats?
        ### Check formatting on time?
        if categorycode_outside:
            print("Codes in data, outside codelist:")
            print("\n".join(categorycode_outside))
            print()
            validation_errors["categorycode_outside"] = ValueError(categorycode_outside)
        if categorycode_missing:
            print("Category codes missing from data (This is ok, just make sure missing data is intentional):")
            print("\n".join(categorycode_missing))
            print()
        
        if raise_errors and validation_errors:
            raise Exception(validation_errors)
        
        return validation_errors
    

        
    def _get_uttrekksbeskrivelse(self) -> dict:
        filbeskrivelse_url = self.url+"tableId="+self.tabellid
        try:
            filbeskrivelse = self._make_request(filbeskrivelse_url, self.headers)
        finally:
            if hasattr(self, "headers"):
                del self.headers
        #print(filbeskrivelse.text)
        if filbeskrivelse.status_code != 200:
            raise ConnectionError(filbeskrivelse, filbeskrivelse.text)
        # Also deletes / overwrites returned Auth-header from get-request
        filbeskrivelse = json.loads(filbeskrivelse.text)
        print(f"""Hentet uttaksbeskrivelsen for {filbeskrivelse['Huvudtabell']}, 
        med tabellid: {self.tabellid}
        den {filbeskrivelse['Uttaksbeskrivelse_lagd']}""")
        
        # reset tabellid and hovedkode after content of request
        self.filbeskrivelse = filbeskrivelse
        
    def _make_request(self, url: str, header: dict):
        return r.get(url, headers=self.headers)
    
    def _split_attributes(self) -> None:
        # Tabellid might have been "hovedkode" up to this point, as both are valid in the URI
        self.lagd = self.filbeskrivelse['Uttaksbeskrivelse_lagd']
        self.tabellid = self.filbeskrivelse['TabellId']
        self.hovedtabell = self.filbeskrivelse['Huvudtabell']
        self.deltabelltitler = self.filbeskrivelse['DeltabellTitler']
        self.variabler = self.filbeskrivelse['deltabller']
        self.kodelister = self.filbeskrivelse['kodelister']
        if 'null_prikk_missing_kodeliste' in self.filbeskrivelse.keys():
            self.prikking = self.filbeskrivelse['null_prikk_missing_kodeliste']


class StatbankTransfer(StatbankAuth):
    """
    Class for talking with the "transfer-API", which actually recieves the data from the user.
    Create an instance of a StatbankUttrekksbeskrivelse.
    ...
    
    Attributes
    ----------
    data : pd.DataFrame or list of pd.DataFrames
        Number of DataFrames needs to match the number of "deltabeller" in the uttakksbeskrivelse.
        Data-shape can be validated before transfer with the Uttakksbeskrivelses-class.
    lastebruker : str
        Username for Statbanken, not the same as "tbf" or "common personal username" in other SSB-systems
    tabellid: str
        The numeric id of the table, matching the one found on the website. Should be a 5-length string.
    hovedtabell : str
        The "name" of the table, not known to most, so this is set by the Uttakksbeskrivelse, when we get it
    tbf : str
        The abbrivation of username at ssb. Three letters, like "cfc"
    publisering : str
        Date for publishing the transfer. Shape should be "yyyy-mm-dd", like "2022-01-01". 
        Statbanken only allows publishing four months into the future?
    fagansvarlig1 : str
        First person to be notified by email of transfer. Defaults to the same as "tbf"
    fagansvarlig2 : str
        Second person to be notified by email of transfer. Defaults to the same as "tbf"
    overskriv_data : str
        "0" = no overwrite
        "1" = overwrite
    godkjenn_data : str
        "0" = manual approval
        "1" = automatic approval at transfer-time (immediately)
        "2" = JIT (Just In Time), approval right before publishing time
    validation : bool
        Set to True, if you want the python-validation code to run user-side.
        Set to False, if its slow and unnecessary.
    boundary : str
        String that defines the splitting of the body in the transfer-post-request. 
        Kept here for uniform choice through the class.
    urls : dict
        Urls for transfer, observing the result etc., 
        built from environment variables in Dapla-environment
    headers: dict
        Might be deleted without warning.
        Temporarily holds the Authentication for the request.
    filbeskrivelse: StatbankUttrekksBeskrivelse
        Transfer creates its own StatbankUttrekksBeskrivelse, to validate its data against.
        And also guarantee that "tabellid" and "hovedtabell" are set correctly.
    params: dict
        This dict will be built into the url in the post request. 
        Keep it in this nice shape for later introspection.
    data_iter: bool
        A record of if the data was sent into the class as an iterable or not.
    data_type: type
        The datatype of the data sent in, either sent standalone, or in the sent list.
        Currently the class prefers pd.DataFrame.
    body: str
        The data parsed into the body-shape the Statbank-API expects in the transfer-post-request.
    response: requests.response
        The resulting response from the transfer-request. Headers might be deleted without warning.
    delay: 
        Not editable, please dont try. Indicates if the Transfer has been sent yet, or not.
    
    Methods
    -------
    transfer():
        If Transfer was delayed, you can make the transfer by calling this method.
    _validate_original_parameters():
        Validating "pure" parameters on the way into the class.
    _build_urls():
        INHERITED - See description under StatbankAuth
    _build_headers():
        INHERITED - See description under StatbankAuth
    _get_filbeskrivelse():
        Initializes a StatbankUttrekksbeskrivelses-object under .filbeskrivelse
    _build_params():
        Builds the params to be attached to the url
    _identify_data_type():
        Sets data_iter and data_type dependant on data sent in to "data"
    _body_from_data():
        Converts data to .body for the transfer request to add to json/data/body.
    _handle_response():
        Handles the response back from the transfer post-request
    __init__():
    """
    def __init__(self,
                data: pd.DataFrame,
                    tabellid: str = None,
                    lastebruker: str = "",
                    bruker_trebokstaver: str = os.environ['JUPYTERHUB_USER'].split("@")[0], 
                    publisering: dt = (dt.now() + td(days=100)).strftime('%Y-%m-%d'),
                    fagansvarlig1: str = os.environ['JUPYTERHUB_USER'].split("@")[0],
                    fagansvarlig2: str = os.environ['JUPYTERHUB_USER'].split("@")[0],
                    auto_overskriv_data: str = '1',
                    auto_godkjenn_data: str = '2',
                    validation: bool = True,
                    delay: bool = False,
                    ):
        self.data = data
        self.tabellid = tabellid
        if lastebruker:
            self.lastebruker = lastebruker
        else:
            raise ValueError("You must set lastebruker as a parameter")
        self.hovedtabell = None
        self.tbf = bruker_trebokstaver
        self.publisering = publisering
        self.fagansvarlig1 = fagansvarlig1
        self.fagansvarlig2 = fagansvarlig2
        self.overskriv_data = auto_overskriv_data
        self.godkjenn_data = auto_godkjenn_data
        self.validation = validation
        self.__delay = delay
        
        
        self.boundary = "12345"
        if validation: self._validate_original_parameters()

        self.urls = self._build_urls()
        if not self.delay:
            self.transfer()
            
    def __str__(self):
        if self.delay:
            return f'Overføring for statbanktabell {self.tabellid}. \nLastebruker: {self.lastebruker}.\nIkke overført enda.'
        else:
            return f'''Overføring for statbanktabell {self.tabellid}. 
    Lastebruker: {self.lastebruker}.
    Publisering: {self.publisering}.
    Lastelogg: {self.urls['gui'] + self.oppdragsnummer}'''
        
    def __repr__(self):
        return f'StatbankTransfer([data], tabellid="{self.tabellid}", lastebruker="{self.lastebruker}")'
    
    @property
    def delay(self):
        return self.__delay
            
    def _validate_original_parameters(self) -> None:
        # if not self.tabellid.isdigit() or len(self.tabellid) != 5:
        #    raise ValueError("Tabellid må være tall, som en streng, og 5 tegn lang.")

        if not isinstance(self.lastebruker, str) or not self.lastebruker:
            raise ValueError("Du må sette en lastebruker korrekt")

        for tbf in [self.tbf, self.fagansvarlig1, self.fagansvarlig2]:
            if len(tbf) != 3 or not isinstance(tbf, str):
                raise ValueError(f'Brukeren {tbf} - "trebokstavsforkortelse" - må være tre bokstaver...')

        if not isinstance(self.publisering, dt):
            if not self._valid_date_form(self.publisering):
                raise ValueError("Skriv inn datoformen for publisering som 1900-01-01")

        if self.overskriv_data not in ['0', '1']:
            raise ValueError("(Strengverdi) Sett overskriv_data til enten '0' = ingen overskriving (dubletter gir feil), eller  '1' = automatisk overskriving")

        if self.godkjenn_data not in ['0', '1', '2']:
            raise ValueError("(Strengverdi) Sett godkjenn_data til enten '0' = manuell, '1' = automatisk (umiddelbart), eller '2' = JIT-automatisk (just-in-time)")

    
    def _identify_data_type(self) -> tuple[type, bool]:
        if isinstance(self.data, pd.DataFrame):
            data_type = pd.DataFrame
            data_iter = False
        elif isinstance(self.data, list) or isinstance(self.data, tuple):
            for i, d in enumerate(self.data):
                if not isinstance(d, pd.DataFrame):
                      raise TypeError(f"Element {i} in data, is not a DataFrame :(")
            data_type = pd.DataFrame
            data_iter = True
        else:
            raise TypeError("Expecting data to be either a single DataFrame, or a list/tuple of DataFrames.")
        return data_type, data_iter


    def _body_from_data(self) -> str:
        # If data is single pd.DataFrame, put into iterable, so code under works
        if not self.data_iter:
            self.data = [self.data]

        if not self.data_type == pd.DataFrame:
            raise TypeError("Only programmed for Pandas DataFrames as data at this point.")

        # We need the filenames in the body, and they must match up with amount of data-elements we have
        deltabeller_filnavn = [x['Filnavn'] for x in self.filbeskrivelse.deltabelltitler]
        if len(deltabeller_filnavn) != len(self.data):
            raise ValueError("Length mismatch between data-iterable and number of Uttaksbeskrivelse deltabellers filnavn.")
        # Data should be a iterable of pd.DataFrames at this point, reshape to body
        for elem, filename in zip(self.data, deltabeller_filnavn):
            # Replace all nans in data
            elem = elem.fillna("")
            body = f"--{self.boundary}"
            body += f"\nContent-Disposition:form-data; filename={filename}"
            body += "\nContent-type:text/plain\n\n"
            if self.data_type == pd.DataFrame:
                body += elem.to_csv(sep=";", index = False, header = False)
            else:
                raise TypeError("Expecting Dataframe or Table at this point in code")
        body += f"\n--{self.boundary}--"
        body = body.replace("\n", "\r\n")  # Statbank likes this?
        #print(repr(body))
        return body


    @staticmethod
    def _valid_date_form(date) -> bool:
        if (date[:4] + date[5:7] + date[8:]).isdigit() and (date[4]+date[7]) == "--":
            return True
        return False

    def _build_params(self) -> dict:
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

    def transfer(self, headers: dict = {}):
        """The headers-parameter is for a future implemention of a possible BatchTransfer, dont use it please."""
        # In case transfer has already happened, dont transfer again
        if hasattr(self, "oppdragsnummer"):
            raise ValueError(f"Already transferred?\n{self.urls['gui'] + self.oppdragsnummer} \nRemake the StatbankTransfer-object if intentional. ")
        if not headers:
            self.headers = self._build_headers()
        else:
            self.headers = headers
        try:
            self.filbeskrivelse = self._get_filbeskrivelse()
            self.hovedtabell = self.filbeskrivelse.hovedtabell
            # Reset taballid, as sending in "hovedkode" as tabellid is possible up to this point
            self.tabellid = self.filbeskrivelse.tabellid
            self.params = self._build_params()
            
            self.data_type, self.data_iter = self._identify_data_type()
            if self.data_type != pd.DataFrame: 
                raise ValueError(f"Data must be loaded into one or more pandas DataFrames. Type looks like {self.data_type}")
            if self.validation: 
                self.validation_errors = self.filbeskrivelse.validate_dfs(self.data, raise_errors = True)
                
            self.body = self._body_from_data()
            
            url_load_params = self.urls['loader'] + urllib.parse.urlencode(self.params)
            #print(url_load_params, self.headers, self.body)
            self.response = self._make_transfer_request(url_load_params)
            print(self.response)
            if self.response.status_code == 200:
                del self.response.request.headers  # Auth is stored here also, for some reason
        finally:
            del self.headers  # Cleaning up auth-storing
            self.__delay = False
        self._handle_response()

    def _get_filbeskrivelse(self) -> StatbankUttrekksBeskrivelse:
        return StatbankUttrekksBeskrivelse(tabellid=self.tabellid, 
                                           lastebruker=self.lastebruker, 
                                           headers=self.headers)
    
    def _make_transfer_request(self, url_params: str,):
        return r.post(url_params, headers = self.headers, data = self.body)
    
    def _handle_response(self) -> None:
        response_json = json.loads(self.response.text)
        if self.response.status_code == 200:
            response_message = response_json['TotalResult']['Message']
            try:
                self.oppdragsnummer = response_message.split("lasteoppdragsnummer:")[1].split(" =")[0]
            except:
                raise ValueError(response_json)
            if not self.oppdragsnummer.isdigit():
                raise ValueError(f"Lasteoppdragsnummer: {oppdragsnummer} er ikke ett rent nummer.")

            publiseringdato = dt.strptime(response_message.split("Publiseringsdato '")[1].split("',")[0], "%d.%m.%Y %H:%M:%S")
            publiseringstime = int(response_message.split("Publiseringstid '")[1].split(":")[0])
            publiseringsminutt = int(response_message.split("Publiseringstid '")[1].split(":")[1].split("'")[0])
            publisering = publiseringdato + td(0, (publiseringstime*3600+publiseringsminutt*60))
            print(f"Publisering satt til: {publisering.strftime('%Y-%m-%d %H:%M')}")
            print(f"Følg med på lasteloggen (tar noen minutter): {self.urls['gui'] + self.oppdragsnummer}")
            print(f"Og evt APIen?: {self.urls['api'] + self.oppdragsnummer}")
            self.response_json = response_json
        else:
            print("Take a closer look at StatbankTransfer.response.text for more info about connection issues.")
            raise ConnectionError(response_json)

class StatbankBatchTransfer(StatbankAuth):
    """
    Takes a bunch of delayed Transfer jobs in a list, so they can all be dispatched at the same time, with one password request.
        ...
    
    Attributes
    ----------    
    jobs: 
        list of delayed StatbankTransfers.
    lastebruker:
        extracts the "lastebruker" from the first transfer-job. 
        All jobs in the batch-transfer must use the same user and password.
    headers:
        Deleted without warning. Temporarily holds the headers to Authenticate the transfers.
    
    Methods
    -------
    transfer():
        Builds headers (asks for password), calls the transfer function of each StatbankTransfer-job. Deletes headers.
    
    """    
    def __init__(self,
                jobs: list = []):
        if not jobs:
            raise ValueError("Can't batch-transfer, no transfers, give me a list of delayed StatbankTransfers.")
        self.jobs = jobs
        # Make sure all jobs are delayed StatbankTransfer-objects
        for i, job in enumerate(self.jobs):
            if not isinstance(job, StatbankTransfer):
                raise TypeError(f"Transfer-job {i} is not a StatbankTransfer-object.")
            if not job.delay:
                raise ValueError(f"Transfer-job {i} was not delayed?")
        self.lastebruker = self.jobs[0].lastebruker
        self.transfer()
        
    def transfer(self):
        self.headers = self._build_headers()
        try:
            for job in self.jobs:
                job.transfer(self.headers)
        finally: 
            del self.headers
        
        
##############################
# Getting data from Statbank #
##############################

def apidata(id_or_url: str = "",
            payload: dict = {"query": [], "response": {"format": "json-stat2"}},
            include_id: bool = False) -> pd.DataFrame:
    """
    Parameter1 - id_or_url: The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".
    Parameter2: Payload, the query to include with the request.
    Parameter3: If you want to include "codes" in the dataframe, set this to True
    Returns: a pandas dataframe with the table
    """
    if len(id_or_url)==5 and id_or_url.isdigit():
        url = f"https://data.ssb.no/api/v0/no/table/{id_or_url}/"
    else:
        try:
            urllib.parse.urlparse(id_or_url)
            url = id_or_url
        except:
            raise ValueError("First parameter not recognized as a statbank ID or a direct url")
    repr(url)
    print(url)
    # Spør APIet om å få resultatet med requests-biblioteket
    resultat = r.post(url, json=payload)
    if resultat.status_code == 200:
        # Putt teksten i resultatet inn i ett pyjstat-datasett-objekt
        dataset = pyjstat.Dataset.read(resultat.text)
        # Skriv pyjstat-objektet ut som en pandas dataframe
        df = dataset.write('dataframe')
        # Om man ønsker IDen påført dataframen, så er vi fancy
        if include_id:
            df2 = dataset.write('dataframe', naming='id')
            skip = 0
            for i, col in enumerate(df2.columns):
                insert_at = (i+1)*2-1-skip
                df_col_tocompare = df.iloc[:, insert_at-1]
                # Sett inn kolonne på rett sted, avhengig av at navnet ikke er brukt
                # og at nabokolonnen ikke har samme verdier.
                if col not in df.columns and not df2[col].equals(df_col_tocompare):
                    df.insert(insert_at, col, df2[col])
                # Indexen må justeres, om vi lar være å skrive inn kolonnen
                else:
                    skip += 1
        df = df.convert_dtypes()
        return df
    elif resultat.status_code == 403:
        raise r.ConnectionError(f"Too big dataset? Try specifying a query into the function apidata (not apidata_all) to limit the returned data size. Status code {resultat.status_code}: {resultat.text}")
    elif resultat.status_code == 400:
        raise r.ConnectionError(f"Bad Request, something might be wrong with your query... Status code {resultat.status_code}: {resultat.text}")
    else:
        raise r.ConnectionError(f"Status code {resultat.status_code}: {resultat.text}")

def apidata_all(id_or_url: str = "",
                include_id: bool = False) -> pd.DataFrame:
    """
    Parameter1 - id_or_url: The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".
    Returns: a pandas dataframe with the table
    """
    return apidata(id_or_url, apidata_query_all(id_or_url), 
                       include_id=include_id)
        
def apidata_query_all(id_or_url: str = "") -> dict:
    """
    Parameter1 - id_or_url: The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".
    Returns: A dict of the prepared query based on all the codes in the table.
    """
    if len(id_or_url)==5 and id_or_url.isdigit():
        url = f"https://data.ssb.no/api/v0/no/table/{id_or_url}/"
    else:
        try:
            urllib.parse.urlparse(id_or_url)
            url = id_or_url
        except:
            raise ValueError("First parameter not recognized as a statbank ID or a direct url")
    res = r.get(url)
    if res.status_code == 200:
        meta = json.loads(res.text)['variables']
        code_list = []
        for code in meta:
            tmp = {}
            for k, v in code.items():
                if k == 'code':
                    tmp[k] = v
                if k == 'values':
                    tmp['selection'] = {'filter':'item', k : v}
            code_list += [tmp]
        code_list
        query = {'query': code_list,
                 "response": {"format": "json-stat2"}}
        return query
    else:
        raise r.ConnectionError(f"Can't get query metadata in first of two requests. Status code {res.status_code}: {res.text}")

# Credit: https://github.com/sehyoun/SSB_API_helper/blob/master/src/ssb_api_helper.py    
def apidata_rotate(df, ind='year', val='value'):
    """Rotate the dataframe so that years are used as the index
    Args:
    df (pandas.dataframe): dataframe (from <get_from_ssb> function
    ind (str): string of column name denoting time
    ind (str): string of column name denoting values
    Returns:
    dataframe: pivotted dataframe
    """
    return df.pivot_table(index=ind,
                        values=val,
                        columns=[iter for iter in df.columns \
                                      if iter != ind and iter != val])

if __name__ == '__main__':
    print("Only for importing?")