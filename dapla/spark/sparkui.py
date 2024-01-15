import os
from typing import Any
from urllib.parse import urlparse


def uiWebUrl(self: Any) -> str:
    """Fix the Spark UI link to point to the jupyter-server-proxy.

    From https://github.com/jupyterhub/jupyter-server-proxy/issues/57
    """
    web_url = self._jsc.sc().uiWebUrl().get()
    port = urlparse(web_url).port
    return os.environ["JUPYTERHUB_SERVICE_PREFIX"] + f"proxy/{port}/jobs/"
