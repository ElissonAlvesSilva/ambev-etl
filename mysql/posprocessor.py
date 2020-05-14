#!/usr/bin/env python
# -*- coding: utf-8 -*-

from unicodedata import normalize

from config import Config
from utils.log import Log
from utils.singleton import Singleton


@Singleton
class PosProcessor:
    """
    Process the data resultant from the Transform phase in order to perform the final 
    modifications to ensure that the data content is compatible with API.
    """
    def __init__(self):
        pass

    def run(self, collections):
      Log.Instance().appendFinalReport("\nStarting POSPROCESSING stage...\n===================")
      for collection in collections:
        Log.Instance().append("Posprocessing " + collection['etl_meta']['label'] + "...")
        lentemp = len(collection['etl_data'])
        collection['etl_data'] = self._process_to_api_final(collection['etl_data'], kpi_name=collection['etl_meta']['kpi_name'])
        result_log = "( " + collection['etl_meta']['label'] + ":" + str(lentemp)
        result_log += " = " + collection['etl_meta']['label'] +\
                      ":" + str(len(collection['etl_data'])) + " )"
        Log.Instance().append(result_log)
      Log.Instance().appendFinalReport("===================\nPOSPROCESSING stage ended.")
      return collections

    def _process_to_api_final(self, collection, kpi_name):
      return collection[kpi_name]
