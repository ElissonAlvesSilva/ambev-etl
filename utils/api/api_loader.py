import json
from datetime import datetime
from datetime import timedelta

import requests
from requests.exceptions import RequestException

from config import Config
from utils.api.api_deleter import APIDeleter
from utils.file_manager import FileManager
from utils.log import Log
from utils.singleton import Singleton


@Singleton
class Loader:
    """
    Load KPIs to Dashboard API and TopN
    """

    def __init__(self):
        self.daterange = self._generate_date_range()

    def _generate_date_range(self):
        temp = Config.START_DATE
        results = []
        while temp <= Config.END_DATE:
            results.append(temp.strftime("%Y-%m-%d"))
            temp = temp + timedelta(days=1)
        return results

    def run(self):
        Log.Instance().appendFinalReport("Starting LOAD stage...\n===================")
        arrayjson_kpis = self._load_json_files()
        self._delete_old_data(arrayjson_kpis)
        self._show_message(arrayjson_kpis)
        for kpi in arrayjson_kpis:
            if kpi['etl_meta']['api'] == Config.TOPN_API['name']:
                date = datetime.strptime(kpi['etl_meta']['timestamp'],
                                         '%Y-%m-%dT%H:%M:%S').strftime('%Y-%m-%d')
                for client_json in kpi['etl_data']:
                    path = '{host}{apikey}/{kpi_name}/{date}'\
                        .format(host=Config.CURRENT_ENV['topn-api'],
                                apikey=client_json['client'],
                                kpi_name=kpi['etl_meta']['kpi_name'],
                                date=date)
                    filtered_data = self._filter_clients(client_json['data'],
                                                         kpi['etl_meta']['api'],
                                                         client_json['client'])
                    if filtered_data:
                        self.post_kpi(path, filtered_data, kpi['etl_meta']['api'])
            else:
                path = Config.CURRENT_ENV['dashboard-api'] + kpi['etl_meta']['kpi_name']
                filtered_data = self._filter_clients(kpi['etl_data'], kpi['etl_meta']['api'])
                self.post_kpi(path, filtered_data, kpi['etl_meta']['api'])
        Log.Instance().appendFinalReport("===================\nLOAD stage ended.")

    def _delete_old_data(self, arrayjson_kpis):
        kpis_names = {}
        for kpi in arrayjson_kpis:
            kpis_names[kpi['etl_meta']['kpi_name']] = {
                'api': kpi['etl_meta']['api'],
                'label': kpi['etl_meta']['label']
            }
        APIDeleter.Instance().delete(
            Config.START_DATE,
            Config.END_DATE,
            kpis_names,
            Config.CLIENTS_DEFAULT
        )

    def _show_message(self, arrayjson_kpis):
        kpis = []
        for kpi in arrayjson_kpis:
            kpi_name = kpi['etl_meta']['label']
            if kpi_name not in kpis:
                kpis.append(kpi_name)
        Log.Instance().appendFinalReport("\nKPIs to be loaded: " + ", ".join(kpis))
        Log.Instance().appendFinalReport("\nDates to be loaded: " + ', '.join(self.daterange))
        Log.Instance().appendFinalReport("\nClients to be loaded: " + ', '.join(Config.CLIENTS_DEFAULT)+"\n")

    def _load_json_files(self):
        arrayjson_kpis = []
        for kpi in Config.JOBS_NAMES:
            for date in self.daterange:
                data = FileManager.read_from_json_file(Config.WORKDIRECTORY_FOR_KPIS.format(date=date), kpi)
                if data:
                    arrayjson_kpis.append(data)
        return arrayjson_kpis

    def _filter_clients(self, kpi_dict, api, client=None):
        results = []
        if api == Config.TOPN_API['name']:
            if client in Config.CLIENTS_DEFAULT:
                results = kpi_dict
        else:
            for item in kpi_dict:
                if 'client' in item and item['client'] in Config.CLIENTS_DEFAULT:
                    results.append(item)
        return results

    def post_kpi(self, path, data, api):
        if api == Config.TOPN_API['name']:
            user = Config.TOPN_API['auth']['user']
            psw = Config.TOPN_API['auth']['pwd']
        else:
            user = Config.DASHBOARD_API['auth']['user']
            psw = Config.DASHBOARD_API['auth']['pwd']
        Log.Instance().appendFinalReport("POST: " + path)
        headers = {'content-type': 'application/json'}
        if data:
            if api == Config.TOPN_API['name']:
                data = {
                    'data': data
                }
            r = requests.post(path, data=json.dumps(data), headers=headers, auth=(user, psw))
            Log.Instance().appendFinalReport("Result (" + str(r.status_code) + "): " + str(r.content) + "\n")
            if r.status_code != 200:
               raise RequestException
