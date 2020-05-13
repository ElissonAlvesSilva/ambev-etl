from datetime import datetime

from config import Config


class ETLExecutionInfo:
    """
    Encapsulates the ETL execution info.
    """

    def __init__(self, etlphase):
        self.etlphase = etlphase
        self.start_execution = datetime.now()
        self.execution_data = {}

    def end(self):
        end_execution = datetime.now()
        elapsed_minutes = self._calculate_elapsed_minutes(self.start_execution, end_execution)
        self.execution_data = {'value': elapsed_minutes, 
                        'client': 'linx',
                        'start-execution': self.start_execution.strftime("%Y-%m-%dT%H:%M:%S"),
                        'end-execution': end_execution.strftime("%Y-%m-%dT%H:%M:%S"),
                        'etlphase': self.etlphase,
                        'day': Config.END_DATE.strftime("%Y-%m-%dT00:00:00")
                        }

    def _calculate_elapsed_minutes(self, start_execution, end_execution):
        elapsed_time = end_execution - start_execution
        elapsed_seconds = int(elapsed_time.total_seconds())
        elapsed_minutes = elapsed_seconds/60.0
        elapsed_minutes = int(elapsed_minutes * 100) / 100.0
        return elapsed_minutes
