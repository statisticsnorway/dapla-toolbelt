import json
import requests

from .auth import AuthClient


class CollectorClient:
    @staticmethod
    def initiate_collector(collector_name, environment, specification):
        collector_url = f"https://{collector_name}.{environment}-bip-app.ssb.no/tasks"
        keycloak_token = AuthClient.fetch_personal_token()

        collector_response = requests.put(
            collector_url,
            headers={'Authorization': 'Bearer %s' % keycloak_token, 'Content-type': 'application/json'},
            data=json.dumps(specification)
        )

        if collector_response.status_code == 201:
            print("Collector initiated successfully! Check running tasks with the collector_tasks() method.")
        else:
            raise RuntimeError(f'Something went wrong. Response from {collector_url}: {collector_response.status_code}'
                               f' - {collector_response.text or collector_response.reason}')

    @staticmethod
    def collector_tasks(collector_name, environment):
        collector_url = f"https://{collector_name}.{environment}-bip-app.ssb.no/tasks"
        keycloak_token = AuthClient.fetch_personal_token()

        collector_response = requests.get(collector_url, headers={'Authorization': 'Bearer %s' % keycloak_token})

        if collector_response.status_code == 200:
            print(json.dumps(collector_response.json(), indent=2))
        else:
            raise RuntimeError(f'Something went wrong. Response from {collector_url}: {collector_response.status_code}'
                               f' - {collector_response.text or collector_response.reason}')
