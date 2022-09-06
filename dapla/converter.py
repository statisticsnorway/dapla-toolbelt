import requests
import json

from .auth import AuthClient


class ConverterClient:

    def __init__(self, converter_url):
        self.converter_url = converter_url

    def start(self, job_config):
        """Schedule a new converter job

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


    def start_simulation(self, job_config):
        """Start a simulated converter job

        Useful for testing job configurations or diagnosing pseudonymization issues.

        :param job_config: json object of job configuration
        :return: job_id
        """
        keycloak_token = AuthClient.fetch_personal_token()
        converter_response = requests.post(
            f'{self.converter_url}/jobs/simulation',
            headers={'Authorization': 'Bearer %s' % keycloak_token, 'Content-type': 'application/json'},
            data=json.dumps(job_config)
        )
        converter_response.raise_for_status()
        return converter_response


    def get_job_summary(self, job_id):
        """Retrieve the execution summary for a specific converter job

        :param job_id: job id returned after starting converter job using start()
        :return: job summary
        """
        keycloak_token = AuthClient.fetch_personal_token()

        job_summary = requests.get(
            f'{self.converter_url}/jobs/{job_id}/execution-summary',
            headers={'Authorization': 'Bearer %s' % keycloak_token, 'Content-type': 'application/json'}
        )
        job_summary.raise_for_status()
        return job_summary


    def stop_job(self, job_id):
        """Stop a specific converter job

        :param job_id:
        :return:
        """
        keycloak_token = AuthClient.fetch_personal_token()

        job_status = requests.post(
            f'{self.converter_url}/jobs/{job_id}/stop',
            headers={'Authorization': 'Bearer %s' % keycloak_token, 'Content-type': 'application/json'}
        )
        job_status.raise_for_status()
        return job_status


    def get_pseudo_report(self, job_id):
        """Get a report with details about how pseudonymization is being applied for a specific job

        The report includes:
        * mathced pseudo rules
        * unmatched pseudo rules
        * pseudo rule to field match map - rule names and their corresponding matches
        * field matches to pseudo rule map - field names and the corresponding pseudo rule that covers this field
        * metrics
        * textual schema hierarchy that illustrates how the pseudo rules are being applied

        :param job_id: job id returned after starting converter job using start()
        :return: job summary
        """
        keycloak_token = AuthClient.fetch_personal_token()

        pseudo_report = requests.get(
            f'{self.converter_url}/jobs/{job_id}/reports/pseudo',
            headers={'Authorization': 'Bearer %s' % keycloak_token, 'Content-type': 'application/json'}
        )
        pseudo_report.raise_for_status()
        return pseudo_report


    def get_pseudo_schema(self, job_id):
        """Get hierarchical schema representation that details how pseudo rules are being applied

        This is a smaller version get_pseudo_report

        :param job_id: job id returned after starting converter job using start()
        :return: job summary
        """
        keycloak_token = AuthClient.fetch_personal_token()

        pseudo_report = requests.get(
            f'{self.converter_url}/jobs/{job_id}/reports/pseudo-schema-hierarchy',
            headers={'Authorization': 'Bearer %s' % keycloak_token, 'Content-type': 'application/json'}
        )
        pseudo_report.raise_for_status()
        return pseudo_report
