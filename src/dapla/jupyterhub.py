import os

import requests
from jupyterhub.services.auth import HubAuth


def generate_api_token(
    expires_in: int = 3600, description: str = "Generated API token from Dapla Toolbelt"
) -> dict[str, str]:
    """Generate a new API token for the logged in Jupyterhub user.

    Such tokens can be used by third party applications to connect to Jupyterhub running remotely.
    Examples are IDEs like VSCode or Pycharm.
    :param expires_in: number of seconds until the token expires
    :param description: optional description of the token
    :return: a dict that contains the token value, and token URL
    """
    hub = HubAuth()
    body = {"expires_in": expires_in, "note": description}
    hub_response = requests.post(
        os.environ["JUPYTERHUB_API_URL"]
        + "/users/"
        + os.environ["JUPYTERHUB_USER"]
        + "/tokens",
        json=body,
        headers={"Authorization": "token %s" % hub.api_token},
        allow_redirects=False,
    )
    hub_response.raise_for_status()
    return {
        "token": hub_response.json()["token"],
        "token_url": os.environ["JUPYTERHUB_SERVICE_PREFIX"]
        + "?token="
        + hub_response.json()["token"],
    }
