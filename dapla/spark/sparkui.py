import os
from urllib.parse import urlparse


def uiWebUrl(self):
    """
    From https://github.com/jupyterhub/jupyter-server-proxy/issues/57
    Fix the Spark UI link to point to the jupyter-server-proxy
    """
    web_url = self._jsc.sc().uiWebUrl().get()
    port = urlparse(web_url).port
    return os.environ['JUPYTERHUB_SERVICE_PREFIX'] + "proxy/{}/jobs/".format(port)
