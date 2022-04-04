import mock
import responses

from dapla.collector import CollectorClient

collector_test_url = 'https://mock-collector.no/tasks'
fake_token = '123456789'


@mock.patch('dapla.auth.AuthClient')
@responses.activate
def test_initiate_201_response(auth_client_mock):
    auth_client_mock.fetch_personal_token.return_value = fake_token
    specification = {}
    collector_response = {}
    responses.add(responses.PUT, collector_test_url, json=collector_response, status=201)
    client = CollectorClient(collector_test_url)
    client.initiate(specification)

    assert len(responses.calls) == 1


@mock.patch('dapla.auth.AuthClient')
@responses.activate
def test_list_tasks_200_response(auth_client_mock):
    auth_client_mock.fetch_personal_token.return_value = fake_token
    collector_response = {}
    responses.add(responses.GET, collector_test_url, json=collector_response, status=200)
    client = CollectorClient(collector_test_url)
    client.list_tasks()

    assert len(responses.calls) == 1
