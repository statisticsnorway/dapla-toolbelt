import responses
import mock

from dapla.converter import ConverterClient


converter_test_url = 'https://mock-converter.no'
fake_token = '1234567890'

sample_response_start_job = '''
{
  "jobId": "01FZWP8R3PHDYD5QQS4CY1RKBW"
}
'''

sample_response_get_job_summary = '''
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
'''


@mock.patch('dapla.auth.AuthClient')
@responses.activate
def test_converter_start_200_response(auth_client_mock):
    auth_client_mock.fetch_personal_token.return_value = fake_token
    job_config = {}
    responses.add(responses.POST, 'https://mock-converter.no/jobs', json=sample_response_start_job, status=200)
    client = ConverterClient(converter_test_url)
    res = client.start(job_config)
    print(res)