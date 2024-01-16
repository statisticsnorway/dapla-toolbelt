import json
from typing import Any

import requests
from requests import Response

from .auth import AuthClient


class CollectorClient:
    """Client for working with DataCollector."""

    def __init__(self, collector_url: str):
        """Initialize CollectorClient."""
        self.collector_url = collector_url

    def start(self, specification: dict[str, Any]) -> Response:
        """Start a new collector task.

        Args:
            specification: The JSON object of the collector specification

        Returns:
            Response: The "requests.Response" object from the API call
        """
        keycloak_token = AuthClient.fetch_personal_token()
        collector_response = requests.put(
            self.collector_url,
            headers={
                "Authorization": "Bearer %s" % keycloak_token,
                "Content-type": "application/json",
            },
            data=json.dumps(specification),
        )
        collector_response.raise_for_status()
        print(
            "Task initiated successfully! Check running tasks with the running_tasks() method."
        )
        return collector_response

    def running_tasks(self) -> Response:
        """Get all running collector tasks."""
        keycloak_token = AuthClient.fetch_personal_token()
        collector_response = requests.get(
            self.collector_url, headers={"Authorization": "Bearer %s" % keycloak_token}
        )
        collector_response.raise_for_status()
        return collector_response

    def stop(self, task_id: int) -> Response:
        """Stop a running collector task.

        Args:
            task_id: The id of the task to stop.

        Returns:
            Response: The "requests.Response" object from the API call
        """
        keycloak_token = AuthClient.fetch_personal_token()
        collector_response = requests.delete(
            f"{self.collector_url}/{task_id}",
            headers={"Authorization": "Bearer %s" % keycloak_token},
        )
        if collector_response.status_code == 400:
            print(
                "Collector task with id: "
                + str(task_id)
                + " cannot be stopped! Maybe it was already stopped or completed"
            )
        else:
            print("Collector task with id: " + str(task_id) + " stopped successfully")

        return collector_response
