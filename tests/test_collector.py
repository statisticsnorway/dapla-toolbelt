import json
from unittest import mock
from unittest.mock import Mock

import responses

from dapla.collector import CollectorClient

collector_test_url = "https://mock-collector.no/tasks"
collector_test_url_stop = "https://mock-collector.no/tasks/1234"
fake_token = "123456789"


@mock.patch("dapla.collector.AuthClient")
@responses.activate
def test_initiate_201_response(auth_client_mock: Mock) -> None:
    auth_client_mock.fetch_personal_token.return_value = fake_token
    specification: dict[str, str] = {}
    worker_id = "abcd"
    collector_response = f"""{{
        "workerId": "{worker_id}"
    }}"""
    responses.add(
        responses.PUT, collector_test_url, json=collector_response, status=201
    )
    client = CollectorClient(collector_test_url)
    response = client.start(specification)

    assert json.loads(response.json())["workerId"] == worker_id
    assert len(responses.calls) == 1


@mock.patch("dapla.collector.AuthClient")
@responses.activate
def test_list_tasks_200_response(auth_client_mock: Mock) -> None:
    auth_client_mock.fetch_personal_token.return_value = fake_token
    collector_response: dict[str, str] = {}
    responses.add(
        responses.GET, collector_test_url, json=collector_response, status=200
    )
    client = CollectorClient(collector_test_url)
    client.running_tasks()

    assert len(responses.calls) == 1


@mock.patch("dapla.collector.AuthClient")
@responses.activate
def test_stop_task_200_response(auth_client_mock: Mock) -> None:
    auth_client_mock.fetch_personal_token.return_value = fake_token
    collector_response: dict[str, str] = {}
    responses.add(
        responses.DELETE, collector_test_url_stop, json=collector_response, status=200
    )
    client = CollectorClient(collector_test_url)
    client.stop(1234)

    assert len(responses.calls) == 1
