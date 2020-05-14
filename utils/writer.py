from config import Config
from utils.file_manager import FileManager
from utils.log import Log
from utils.singleton import Singleton


@Singleton
class Writer:
    """
    Writes the resultant KPIs to files.
    """

    def __init__(self):
        pass

    def run(self, collections):
        Log.Instance().appendFinalReport("\nStarting WRITING stage...\n===================")
        for collection in collections:
            Log.Instance().append("Writing " + collection['etl_meta']['label'] +
                                  " for " + collection['etl_meta']['timestamp'] + "...")
            if collection['etl_meta']['is_kpi']:
                filepath = Config.WORKDIRECTORY_FOR_KPIS
            else:
                filepath = Config.WORKDIRECTORY_FOR_TEMPS
            filepath = filepath.format(date=collection['etl_meta']['timestamp'][0:10])
            FileManager.create_if_dont_exist(filepath)
            print(collection)
            FileManager.write_json_to_file(filepath, collection['etl_meta']['label'], collection)
        Log.Instance().appendFinalReport("===================\nWRITING stage ended.")
