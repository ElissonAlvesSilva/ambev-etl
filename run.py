import time
import traceback

from utils.system_configurer import SystemConfigurer
from utils.system_exiter import SystemExiter
from utils.args_parser import ArgsParser
from utils.etl_execution_info import ETLExecutionInfo
from utils.writer import Writer
from utils.log import Log

from mysql.jobs_loader import JobsLoader
from mysql.jobs_manager import JobsManager
from mysql.mysql_connection import MysqlConnection
from mysql.posprocessor import PosProcessor

class Run:
  def __init__(self):
    self.args = ArgsParser.Instance().parse_arguments()

  def run(self):
    SystemConfigurer.Instance().configure_system(self.args)
    self._execute()

  def _execute(self):
    etl_execution_info = ETLExecutionInfo("ETL")

    transform_execution_data = None
    if self.args.transform:
      transform_execution_data = self._execute_transform()

  def _execute_transform(self):
    execution_info = ETLExecutionInfo("TRANSFORM")
    loaded_jobs = JobsLoader.Instance().loaded_jobs
    job_manager = JobsManager(loaded_jobs)
    results = job_manager.run()
    results = PosProcessor.Instance().run(results)
    Writer.Instance().run(results)
    execution_info.end()
    Log.Instance().appendFinalReport("[TRANSFORM executed in: " +
                                     str(execution_info.execution_data['value']) + " minutes ]")
    return execution_info.execution_data
    
### main part
if __name__ == "__main__":
  run = Run()
  try:
    run.run()
  except Exception as e:
    tb = traceback.format_exc()
    SystemExiter.Instance().exit("Exception - "+str(e)+", type: "+str(type(e))+"\n\n"+str(tb))