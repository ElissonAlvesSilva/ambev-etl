import time

import requests
from requests.exceptions import RequestException

from config import Config
from utils.log import Log
from utils.singleton import Singleton
from utils.system_exiter import SystemExiter


@Singleton
class APIDeleter:

    """
    Delete KPIs from Dashboard API and Topn API
    """

    def __init__(self):
        self.retries = 10
 
    def delete(self, start_date, end_date, kpis, clients):
        Log.Instance().appendFinalReport("\nDeleting data...")
        Log.Instance().appendFinalReport("\nKPIs: " + str(kpis))
        Log.Instance().appendFinalReport("\nClients: " + str(clients) + "\n")
        teste = False
        cont = 1
        while teste is False:
            teste = True
            try:
                for kpi in kpis:
                    for client in clients:
                        self._delete_kpi_for_client(start_date, end_date,
                                                    kpi, client, kpis[kpi])
            except RequestException:
                if cont == self.retries:
                    SystemExiter.Instance().exit("<<< Tried " + str(self.retries) + " times to DELETE KPIs and failed, ABORTING! >>>")
                Log.Instance().appendFinalReport("<<< EXCEPTION RequestException, RETRY! (" + str(cont) + " time)>>>")
                teste = False
                cont += 1
                time.sleep(60)
        Log.Instance().appendFinalReport("\n...finished deleting data.\n")

    def _delete_kpi_for_client(self, start_date, end_date, kpi, client, details):
        # TOPN does not need to delete old data because
        # when post same date it will overwrite
        if details['api'] == Config.DASHBOARD_API['name']:
            user = Config.DASHBOARD_API['auth']['user']
            psw = Config.DASHBOARD_API['auth']['pwd']
            path = Config.CURRENT_ENV['dashboard-api'] + kpi
            params = {'from': start_date, 'to': end_date, 'client': client}
            Log.Instance().append("REMOVE: " + path + " " + str(params))
            r = requests.delete(path, data=params, auth=(user, psw))
            if r.status_code == 200:
                Log.Instance().append("Successfully removed kpi " + details['label'] + "\n")
            else:
                Log.Instance().append("Failed to remove kpi " + details['label'] + "\n")
