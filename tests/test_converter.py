import json
from unittest import mock

import responses

from dapla.converter import ConverterClient

auth_endpoint_url = "https://mock-auth.no/user"

converter_test_url = "https://mock-converter.no"
fake_token = "1234567890"

sample_response_start_job = """
{
  "jobId": "01FZWP8R3PHDYD5QQS4CY1RKBW"
}
"""

sample_response_get_job_summary = """
{
  "converter.job.info": 1.0,
  "converter.rawdata.messages.total{result=fail}": 0.0,
  "converter.rawdata.messages.total{result=skip}": 0.0,
  "converter.rawdata.messages.total{result=success}": 1234.0,
  "job.dryrun": false,
  "job.id": "01FZWP8R3PHDYD5QQS4CY1RKBW",
  "job.status": "STOPPED",
  "position.current": "n/a",
  "position.start.configured": "LAST",
  "target.storage.path": "/inndata/skattemelding/utkast/2021",
  "target.storage.root": "gs://ssb-staging-skatt-person-data-produkt",
  "target.storage.version": "20220405",
  "time.elapsed": "56.342646958S",
  "time.start": "2022-04-05T11:03:01.226536Z",
  "time.stop": "2022-04-05T11:03:02.569081Z"
}
"""

sample_response_pseudo_report = """
{
  "matchedPseudoRules": [
    {
      "name": "personidentifikator",
      "pattern": "**/personidentifikator",
      "func": "fpe-fnr(testsecret1)"
    },
    {
      "name": "navn",
      "pattern": "**/navn",
      "func": "fpe-anychar(testsecret1)"
    }
  ],
  "unmatchedPseudoRules": [
    {
      "name": "adresse",
      "pattern": "**/{adresse,gateadresse}",
      "func": "fpe-anychar(testsecret1)"
    }
  ],
  "ruleToFieldMatches": {
    "navn": [
      "/root/foo/bar/navn",
      "/root/foo/bar/baz/navn"
    ],
    "personidentifikator": [
      "/root/foo/personidentifikator"
    ]
  },
  "fieldToRuleMatches": {
    "/root/foo/bar/navn": "navn",
    "/root/foo/bar/baz/navn": "navn",
    "/root/foo/personidentifikator": "personidentifikator"
  },
  "targetSchemaHierachy": "<removed for brevity>",
  "metrics": {
    "totalPseudoRulesCount": 3,
    "totalFieldsCount": 3034,
    "unmatchedPseudoRulesCount": 1,
    "pseudonymizedFieldsCount": 3,
    "matchedPseudoRulesCount": 2
  }
}
"""

sample_response_pseudo_schema = """
root record required
 |-- metadata record
 |    |-- collector record
 |    |    |-- ulid string required
 |    |    |-- timestamp long required
 |    |    |-- position string required
 |    |-- converter record
 |    |    |-- jobId string required
 |    |    |-- timestamp long required
 |    |    |-- coreVersion string required
 |    |    |-- props map
 |    |    |-- pseudo record
 |    |    |    |-- rules array
 |    |    |    |    |-- rules record
 |    |    |    |    |    |-- name string
 |    |    |    |    |    |-- pattern string required
 |    |    |    |    |    |-- func string required
 |-- foo record
 |    |-- personidentifikator string required pseudo:fpe-fnr(testsecret1)
 |    |-- bar record
 |    |    |-- navn string required pseudo:fpe-anychar(testsecret1)
 |    |    |-- baz record
 |    |    |    |-- navn string required pseudo:fpe-anychar(testsecret1)
 |    |-- opprettetDato string
"""


@mock.patch.dict(
    "dapla.auth.os.environ",
    {
        "DAPLA_SERVICE": "JUPYTERLAB",
        "DAPLA_REGION": "DAPLA_LAB",
        "OIDC_TOKEN": "dummy_token",
    },
    clear=True,
)
@responses.activate
def test_converter_start_200_response_on_dapla_lab() -> None:
    job_config: dict[str, str] = {}
    responses.add(
        responses.POST,
        "https://mock-converter.no/jobs",
        json=sample_response_start_job,
        status=200,
    )
    client = ConverterClient(converter_test_url)
    response = client.start(job_config)
    json_str = json.loads(response.json())

    assert json_str["jobId"] == json.loads(sample_response_start_job)["jobId"]


@mock.patch.dict(
    "dapla.auth.os.environ",
    {
        "DAPLA_SERVICE": "JUPYTERLAB",
        "DAPLA_REGION": "BIP",
        "LOCAL_USER_PATH": auth_endpoint_url,
    },
    clear=True,
)
@responses.activate
def test_converter_start_200_response_on_jupyterhub() -> None:
    mock_response = {
        "access_token": "fake_access_token",
    }
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=200)

    job_config: dict[str, str] = {}

    responses.add(
        responses.POST,
        "https://mock-converter.no/jobs",
        json=sample_response_start_job,
        status=200,
    )
    client = ConverterClient(converter_test_url)
    response = client.start(job_config)
    json_str = json.loads(response.json())

    assert json_str["jobId"] == json.loads(sample_response_start_job)["jobId"]


@mock.patch.dict(
    "dapla.auth.os.environ",
    {
        "DAPLA_SERVICE": "JUPYTERLAB",
        "DAPLA_REGION": "BIP",
        "LOCAL_USER_PATH": auth_endpoint_url,
    },
    clear=True,
)
@responses.activate
def test_converter_start_simulation_200_response() -> None:
    mock_response = {
        "access_token": "fake_access_token",
    }
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=200)

    job_config: dict[str, str] = {}
    responses.add(
        responses.POST,
        "https://mock-converter.no/jobs/simulation",
        json=sample_response_start_job,
        status=200,
    )
    client = ConverterClient(converter_test_url)
    response = client.start_simulation(job_config)
    json_str = json.loads(response.json())

    assert json_str["jobId"] == json.loads(sample_response_start_job)["jobId"]


@mock.patch.dict(
    "dapla.auth.os.environ",
    {
        "DAPLA_SERVICE": "JUPYTERLAB",
        "DAPLA_REGION": "BIP",
        "LOCAL_USER_PATH": auth_endpoint_url,
    },
    clear=True,
)
@responses.activate
def test_converter_get_job_summary_200_response() -> None:
    mock_response = {
        "access_token": "fake_access_token",
    }
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=200)
    responses.add(
        responses.GET,
        "https://mock-converter.no/jobs/01FZWP8R3PHDYD5QQS4CY1RKBW/execution-summary",
        json=sample_response_get_job_summary,
        status=200,
    )
    client = ConverterClient(converter_test_url)
    response = client.get_job_summary(json.loads(sample_response_start_job)["jobId"])

    assert json.loads(response.json()) == json.loads(sample_response_get_job_summary)


@mock.patch.dict(
    "dapla.auth.os.environ",
    {
        "DAPLA_SERVICE": "JUPYTERLAB",
        "DAPLA_REGION": "BIP",
        "LOCAL_USER_PATH": auth_endpoint_url,
    },
    clear=True,
)
@responses.activate
def test_converter_stop_job_200_response() -> None:
    mock_response = {
        "access_token": "fake_access_token",
    }
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=200)
    responses.add(
        responses.POST,
        "https://mock-converter.no/jobs/01FZWP8R3PHDYD5QQS4CY1RKBW/stop",
        json=sample_response_get_job_summary,
        status=200,
    )
    client = ConverterClient(converter_test_url)
    response = client.stop_job(json.loads(sample_response_start_job)["jobId"])

    assert response.status_code == 200


@mock.patch.dict(
    "dapla.auth.os.environ",
    {
        "DAPLA_SERVICE": "JUPYTERLAB",
        "DAPLA_REGION": "BIP",
        "LOCAL_USER_PATH": auth_endpoint_url,
    },
    clear=True,
)
@responses.activate
def test_converter_get_pseudo_report_200_response() -> None:
    mock_response = {
        "access_token": "fake_access_token",
    }
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=200)
    responses.add(
        responses.GET,
        "https://mock-converter.no/jobs/01FZWP8R3PHDYD5QQS4CY1RKBW/reports/pseudo",
        json=sample_response_pseudo_report,
        status=200,
    )
    client = ConverterClient(converter_test_url)
    response = client.get_pseudo_report(json.loads(sample_response_start_job)["jobId"])

    assert json.loads(response.json()) == json.loads(sample_response_pseudo_report)


@mock.patch.dict(
    "dapla.auth.os.environ",
    {
        "DAPLA_SERVICE": "JUPYTERLAB",
        "DAPLA_REGION": "BIP",
        "LOCAL_USER_PATH": auth_endpoint_url,
    },
    clear=True,
)
@responses.activate
def test_converter_get_pseudo_schema_200_response() -> None:
    mock_response = {
        "access_token": "fake_access_token",
    }
    responses.add(responses.GET, auth_endpoint_url, json=mock_response, status=200)
    responses.add(
        responses.GET,
        "https://mock-converter.no/jobs/01FZWP8R3PHDYD5QQS4CY1RKBW/reports/pseudo-schema-hierarchy",
        json=sample_response_pseudo_schema,
        status=200,
    )
    client = ConverterClient(converter_test_url)
    response = client.get_pseudo_schema(json.loads(sample_response_start_job)["jobId"])

    assert len(response.text) > 0
