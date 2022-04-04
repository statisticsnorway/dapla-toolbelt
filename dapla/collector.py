import json

import requests

from .auth import AuthClient


class CollectorClient:

    def __init__(self, collector_url):
        self.collector_url = collector_url

    def initiate(self, specification):
        keycloak_token = AuthClient.fetch_personal_token()
        collector_response = requests.put(
            self.collector_url,
            headers={'Authorization': 'Bearer %s' % keycloak_token, 'Content-type': 'application/json'},
            data=json.dumps(specification)
        )

        if collector_response.status_code == 201:
            print("Collector initiated successfully! Check running tasks with the list_tasks() method.")
        else:
            raise RuntimeError(f'Something went wrong. Response from {self.collector_url}:'
                               f' {collector_response.status_code}'
                               f' - {collector_response.text or collector_response.reason}')

    def list_tasks(self):
        keycloak_token = AuthClient.fetch_personal_token()
        collector_response = requests.get(self.collector_url, headers={'Authorization': 'Bearer %s' % keycloak_token})

        if collector_response.status_code == 200:
            print(json.dumps(collector_response.json(), indent=2))
        else:
            raise RuntimeError(f'Something went wrong. Response from {self.collector_url}:'
                               f' {collector_response.status_code}'
                               f' - {collector_response.text or collector_response.reason}')
