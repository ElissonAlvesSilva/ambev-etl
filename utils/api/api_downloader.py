import time

import requests
from requests.exceptions import RequestException

from utils.log import Log
from utils.singleton import Singleton
from utils.system_exiter import SystemExiter


@Singleton
class APIDownloader:

    """
    Download data from API.
    """

    def __init__(self):
        self.retries = 10
 
    def download(self, path, user=None, psw=None, params={}):
        teste = False
        cont = 1
        while teste is False:
            teste = True
            try:
                Log.Instance().appendFinalReport("GET: " + path + " " + str(params))
                r = requests.get(path, params=params, auth=(user, psw))
                if r.status_code == 200:
                    Log.Instance().appendFinalReport("Successfully downloaded data")
                    return r.json()
                else:
                    raise RequestException
            except RequestException:
                if cont == self.retries:
                    SystemExiter.Instance().exit("<<< Tried " + str(self.retries) + " times and failed, ABORTING! >>>")
                Log.Instance().appendFinalReport("<<< EXCEPTION RequestException, RETRY! (" + str(cont) + " time)>>>")
                teste = False
                cont += 1
                time.sleep(60)
