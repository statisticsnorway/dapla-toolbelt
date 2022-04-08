import json

import requests

from .auth import AuthClient


class CollectorClient:

    def __init__(self, collector_url):
        self.collector_url = collector_url

    def start(self, specification):
        keycloak_token = AuthClient.fetch_personal_token()
        collector_response = requests.put(
            self.collector_url,
            headers={'Authorization': 'Bearer %s' % keycloak_token, 'Content-type': 'application/json'},
            data=json.dumps(specification)
        )
        collector_response.raise_for_status()
        print("Collector task with id: " + collector_response.json()['workerId'] +
              " initiated successfully! Check running tasks with the running_tasks() method.")
        return collector_response

    def running_tasks(self):
        keycloak_token = AuthClient.fetch_personal_token()
        collector_response = requests.get(self.collector_url, headers={'Authorization': 'Bearer %s' % keycloak_token})
        collector_response.raise_for_status()
        return collector_response

    def stop(self, task_id):
        keycloak_token = AuthClient.fetch_personal_token()
        collector_response = requests.delete(f'{self.collector_url}/{task_id}',
                                             headers={'Authorization': 'Bearer %s' % keycloak_token})
        collector_response.raise_for_status()
        print("Collector task with id: "+ task_id +" stopped successfully")
        return collector_response
