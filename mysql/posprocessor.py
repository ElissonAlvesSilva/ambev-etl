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
        Log.Instance().append("Posprocessing...")
        processed_collections = {}
        processed_collections['etl_data'] = self._process_to_api_final_(collections)
            
        Log.Instance().appendFinalReport("===================\nPOSPROCESSING stage ended.")
        return processed_collections

    def _process_to_api_final_(self, collection):
        etl_data = { 'mip': [], 'volume': [] }
        for item in collection:
            if 'product' in item:
                etl_data['volume'].append(item)
            else:
                etl_data['mip'].append(item)
        return etl_data
