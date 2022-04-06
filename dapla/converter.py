import requests
import json

from .auth import AuthClient


class ConverterClient:

    def __init__(self, converter_url):
        self.converter_url = converter_url

    def start(self, job_config):
        """
        :param job_config: json object of job configuration
        :return: job_id
        """
        keycloak_token = AuthClient.fetch_personal_token()
        converter_response = requests.post(
            f'{self.converter_url}/jobs',
            headers={'Authorization': 'Bearer %s' % keycloak_token, 'Content-type': 'application/json'},
            data=json.dumps(job_config)
        )
        converter_response.raise_for_status()
        return converter_response

    def get_job_summary(self, job_id):
        """
        :param job_id:
        :return:
        """
        keycloak_token = AuthClient.fetch_personal_token()

        job_summary = requests.get(
            f'{self.converter_url}/jobs/{job_id}/execution-summary',
            headers={'Authorization': 'Bearer %s' % keycloak_token, 'Content-type': 'application/json'}
        )
        job_summary.raise_for_status()
        return job_summary
