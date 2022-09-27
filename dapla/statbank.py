#!/usr/bin/env python3

# Todo
# - Ability to send a constructed Uttrekksbeskrivelse into Transfer?
# - Validation on
#   - Column count, accounts for "prikking-columns"?
#   - Time?
#   - Rounding of floats? And correct decimal-signifier
# - Testing
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

# SSB-packages / local
from dapla import AuthClient

# Ciphering requirements
from abc import ABCMeta, abstractmethod
from base64 import b64decode, b64encode
from binascii import unhexlify
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import ECB


class StatbankAuth:
    """
    Parent class for shared behavior between Statbankens "Transfer-API" and "Uttaksbeskrivelse-API"
    ...

    Methods
    -------
    _build_headers() -> dict:
        Creates dict of headers needed in request to talk to Statbank-API
        
    _build_auth() -> str:
        Gets key from environment and encrypts password with key, combines it with username into expected Authentication header.
        
    _encrypt_password(key) -> str:
        Encrypts password with key. Password is not possible to send into function. Because safety.
        
    _build_urls(database) -> dict:
        Urls will differ based on database to connect to, returns a dict of urls depending on database choice.
        
    __init__():
        is not implemented, as Transfer and UttrekksBeskrivelse both add their own.
    
    """
    def _build_headers(self) -> dict:
        return {
            'Authorization': self._build_auth(),
            'Content-Type': 'multipart/form-data; boundary=12345',
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept' : r'*/*',
            }

    def _build_auth(self):
        # Hør med Bjørn om hvordan dette skal implementeres for å sende passordet
        #response = r.post('http://dapla-statbank-authenticator.dapla.svc.cluster.local/encrypt',
        #                         headers={
        #                              'Authorization': 'Bearer %s' % AuthClient.fetch_personal_token(),
        #                              'Content-type': 'application/json'
        #                         }, json={"message": self.database})
        # Get key from response
        #try:
        #    key = response.text['key']
        try:
            key = os.environ["STATBANK_TEST_KEY"]
        except Exception as e:
            raise e
        #finally:
        #    del response
        # Encrypt password with key
        try:
            encrypted_password = self._encrypt_password(key)
        except Exception as e:
            raise e
        finally:
            del key
            
        try:
            # Combine with username
            username_encryptedpassword = bytes(self.lastebruker, 'UTF-8') + bytes(':', 'UTF-8') + bytes(encrypted_password, 'UTF-8')
        finally:
            del encrypted_password
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
    
    @staticmethod
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
        return {k: base_url+v for k, v in END_URLS.items()}

    
class StatbankUttrekksBeskrivelse(StatbankAuth):
    """
    Class for talking with the "uttrekksbeskrivelses-API", which describes metadata about shape of data to be transferred.
    And metadata about the table itself in Statbankens system, like ID, name of codelists etc.
    ...

    Attributes
    ----------
    database : str
        Which of statbankens databases to connect to. PROD, TEST, QA or UTV
    lastebruker : str
        Username for Statbanken, not the same as "tbf" or "common personal username" in other SSB-systems
    url : str
        Main url for transfer, built from database choice
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
    _split_attributes():
    __init__():
    
    """
    def __init__(self, tabellid, lastebruker, database="PROD", headers=None):
        self.database = database
        self.lastebruker = lastebruker
        self.url = self._build_urls(self.database)['uttak']
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
            del self.headers
        self._split_attributes()

    def validate_dfs(self, data, raise_errors: bool = False) -> dict:
        validation_errors = {}
        
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
            col_num = len(deltabell['variabler'])
            if len(to_validate[deltabell_num].columns) != col_num:
                validation_errors["col_count_data" + deltabell_num] = ValueError(f"Expecting {col_num} columns in datapart {deltabell_num}: {deltabell_navn}")
        
        
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
        
        if raise_errors:
            raise Exception(validation_errors)
        
        return validation_errors
        
    def _get_uttrekksbeskrivelse(self) -> dict:
        filbeskrivelse_url = self.url+"tableId="+self.tabellid
        filbeskrivelse = r.get(filbeskrivelse_url, headers=self.headers)
        if filbeskrivelse.status_code != 200:
            del self.headers
            raise ConnectionError(filbeskrivelse)
        # Also deletes / overwrites returned Auth-header from get-request
        filbeskrivelse = json.loads(filbeskrivelse.text)
        print(f"""Hentet uttaksbeskrivelsen for {filbeskrivelse['Huvudtabell']}, 
        med tabellid: {self.tabellid}
        den {filbeskrivelse['Uttaksbeskrivelse_lagd']}""")
        
        # reset tabellid and hovedkode after content of request
        self.filbeskrivelse = filbeskrivelse

    def _split_attributes(self) -> None:
        # Tabellid might have been "hovedkode" up to this point, as both are valid in the URI
        self.lagd = self.filbeskrivelse['Uttaksbeskrivelse_lagd']
        #self.database = self.filbeskrivelse['base']
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
        f
    lastebruker : str
        f
    tabellid: str
        The 
    hovedtabell : str
        a
    tbf : str
        a    
    publisering : str
        a
    fagansvarlig1 : str
        a
    fagansvarlig2 : str
        a
    overskriv_data : str
        a
    godkjenn_data : str
        a
    validation : bool
        a
    boundary : str
        a
    urls : dict
        a
    headers: dict
        a
    filbeskrivelse: dict
        a
    params: dict
        a
    data_iter: bool
        a
    data_type: type
        a
    body: str
        a
    response: requests.response
        a
    
    Methods
    -------
    _validate_original_parameters():
    _build_urls():
        INHERITED - See description under StatbankAuth
    _build_headers():
        INHERITED - See description under StatbankAuth
    _get_filbeskrivelse():
        Gets a StatbankUttrekksbeskrivelses-object
    _build_params():
    _identify_data_type():
    _body_from_data():
    _handle_response():
    __init__():
    """
    def __init__(self,
                data: pd.DataFrame,
                    tabellid: str = None,
                    lastebruker: str = "",
                    database: str = 'PROD',
                    bruker_trebokstaver: str = os.environ['JUPYTERHUB_USER'].split("@")[0], 
                    publisering: dt = (dt.now() + td(days=366)).strftime('%Y-%m-%d'),
                    fagansvarlig1: str = os.environ['JUPYTERHUB_USER'].split("@")[0],
                    fagansvarlig2: str = os.environ['JUPYTERHUB_USER'].split("@")[0],
                    auto_overskriv_data: str = '1',
                    auto_godkjenn_data: str = '2',
                    validation: bool = True):
        self.data = data
        self.tabellid = tabellid
        if lastebruker:
            self.lastebruker = lastebruker
        else:
            raise ValueError("You must set lastebruker as a parameter")
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

        self.urls = self._build_urls(self.database)
        self.headers = self._build_headers()
        try:
            self.filbeskrivelse = self._get_filbeskrivelse()
            self.hovedtabell = self.filbeskrivelse.hovedtabell
            # Reset taballid, as sending in "hovedkode" as tabellid is possible up to this point
            self.tabellid = self.filbeskrivelse.tabellid
            self.params = self._build_params()
            
            self.data_type, self.data_iter = self._identify_data_type()
            if self.data_type != pd.DataFrame: 
                raise ValueError(f"Data must be loaded into one or more pandas DataFrames. Type looks like {self.data_type}")
            if validation: 
                validation_errors = self.filbeskrivelse.validate_dfs(self.data, raise_errors = True)
                
            self.body = self._body_from_data()
            
            url_load_params = self.urls['loader'] + urllib.parse.urlencode(self.params)
            #print(url_load_params, self.headers, self.body)
            self.response = r.post(url_load_params, headers = self.headers, data = self.body)
            print(self.response)
            if self.response.status_code == 200:
                del self.response.request.headers  # Auth is stored here also, for some reason
        finally:
            del self.headers  # Cleaning up auth-storing
            
        self._handle_response()
        
    def _validate_original_parameters(self) -> None:
        # if not self.tabellid.isdigit() or len(self.tabellid) != 5:
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



    def _get_filbeskrivelse(self) -> StatbankUttrekksBeskrivelse:
        return StatbankUttrekksBeskrivelse(tabellid=self.tabellid, 
                                           lastebruker=self.lastebruker, 
                                           database=self.database, 
                                           headers=self.headers)

    def _handle_response(self) -> None:
        response_json = json.loads(self.response.text)
        if self.response.status_code == 200:
            response_message = response_json['TotalResult']['Message']
            self.oppdragsnummer = response_message.split("lasteoppdragsnummer:")[1].split(" =")[0]
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
            raise ConnectionError(response_json['ItemResults'][0]['Exception'])
        
    


# credit: https://github.com/Laerte/aes_pkcs5/blob/main/aes_pkcs5/algorithms/__init__.py
OUTPUT_FORMATS = ("b64", "hex")

class AESCommon(metaclass=ABCMeta):
    """Common AES interface"""
    def __init__(self, key: str, output_format: str) -> None:
        self._key = key.encode()
        if output_format not in OUTPUT_FORMATS:
            raise NotImplementedError(f"Support for output format: {output_format} is not implemented")
        self._output_format = output_format

    def encrypt(self, message: str) -> str:
        """Return encrypted message
        :param message: message to be encrypted
        :type message: str
        """
        cipher_instance = self._get_cipher()
        message = message.encode()
        offset = 16 - len(message) % 16
        message = message + (offset * chr(offset)).encode()
        encryptor = cipher_instance.encryptor()
        result = encryptor.update(message)
        return (
            b64encode(result).decode() if self._output_format == "b64" else result.hex()
        )

    def decrypt(self, message: str) -> str:
        """Return decrypted message
        :param message: encrypted message
        :type message: str
        """
        cipher_instance = self._get_cipher()
        decryptor = cipher_instance.decryptor()
        result = decryptor.update(
            b64decode(message) if self._output_format == "b64" else unhexlify(message)
        )
        pad_num = result[-1]
        result = result[:-pad_num]
        return result.decode()

    @abstractmethod
    def _get_cipher(self) -> Cipher:
        """
        Return the Cipher that will be used
        """

class AESECBPKCS5Padding(AESCommon):
    """Implements AES algorithm with ECB mode of operation and padding scheme PKCS5."""
    def __init__(self, key: str, output_format: str):
        super(AESECBPKCS5Padding, self).__init__(key=key, output_format=output_format)

    def _get_cipher(self):
        """Return AES/CBC/PKCS5Padding Cipher"""
        return Cipher(AES(self._key), mode=ECB(), backend=default_backend())
    
    
if __name__ == '__main__':
    print("Only for importing?")