import json
from datetime import datetime
from datetime import timedelta

import requests
from requests.exceptions import RequestException

from config import Config
from utils.file_manager import FileManager
from utils.log import Log
from utils.singleton import Singleton


@Singleton
class Loader:
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
			self._show_message(arrayjson_kpis)
			for kpi in arrayjson_kpis:
				if kpi['etl_meta']['api'] == 'ambev-mip':
					for item in kpi['etl_data']:
						path = '{host}/feedstock-results/{kpi_name}/{date}'\
								.format(host=Config.CURRENT_ENV['api'],
												kpi_name=kpi['etl_meta']['kpi_name'],
												date=kpi['etl_meta']['timestamp'])
						self.post_kpi(path, item)
				else:
					for item in kpi['etl_data']:
						path = '{host}/content-results/{kpi_name}/{date}'\
								.format(host=Config.CURRENT_ENV['api'],
												kpi_name=kpi['etl_meta']['kpi_name'],
												date=kpi['etl_meta']['timestamp'])
						self.post_kpi(path, item)
			Log.Instance().appendFinalReport("===================\nLOAD stage ended.")

		def _show_message(self, arrayjson_kpis):
			kpis = []
			for kpi in arrayjson_kpis:
				kpi_name = kpi['etl_meta']['label']
				if kpi_name not in kpis:
					kpis.append(kpi_name)
			Log.Instance().appendFinalReport("\nKPIs to be loaded: " + ", ".join(kpis))
			Log.Instance().appendFinalReport("\nDates to be loaded: " + ', '.join(self.daterange))

		def _load_json_files(self):
			arrayjson_kpis = []
			for kpi in Config.JOBS_NAMES:
				for date in self.daterange:
					data = FileManager.read_from_json_file(Config.WORKDIRECTORY_FOR_KPIS.format(date=date), kpi)
					if data:
						arrayjson_kpis.append(data)
			return arrayjson_kpis

		def post_kpi(self, path, data):
			# user = Config.TOPN_API['auth']['user']
			# psw = Config.TOPN_API['auth']['pwd']
			data = {
				"data": data
			}
			Log.Instance().appendFinalReport("POST: " + path)
			headers = {'content-type': 'application/json'}
			if data:
				r = requests.post(path, data=json.dumps(data), headers=headers)
				Log.Instance().appendFinalReport("Result (" + str(r.status_code) + "): " + str(r.content) + "\n")
				if r.status_code != 200:
					raise RequestException
