import os
import sys

from config import Config
from mysql.jobs_loader import JobsLoader
from utils.log import Log
from utils.mail_report import MailReport
from utils.singleton import Singleton
from utils.system_exiter import SystemExiter


@Singleton
class SystemConfigurer:
    """
    Centralize all the system configuration.
    """

    def configure_system(self, args):
        print("\nConfiguring ETL4, please wait...\n")
        self.args = args
        Config.START_DATE = self.args.start_date
        Config.END_DATE = self.args.end_date
        self._configure_environment()
        self._config_job_variables()
        self._config_general_variables()
        self._configure_workdirectory()
        Config.generate_definition_files_path()
        JobsLoader.Instance().load_jobs()
        if not args.only_print_query:
            self._show_config_message()
            MailReport.Instance().configure(self._configuration_message(self.args))
        else:
            Config.THREAD_POOL = 1

    def _configure_environment(self):
        if self.args.env not in Config.ENVIRONMENTS.keys():
            SystemExiter.Instance().exit("Error: invalid environment! (valid envs: " + str(Config.ENVIRONMENTS.keys()) + ")")
        Config.CURRENT_ENV = Config.ENVIRONMENTS[self.args.env]

    def _config_job_variables(self):
        Config.JOBS_NAMES = []
        if self.args.jobs:
            Config.JOBS_NAMES = self.args.jobs.split(',')
            if len(Config.JOBS_NAMES) == 0:
                SystemExiter.Instance().exit("Error: If you use -j, you must choose a job to run!")
        else:
            Config.RUN_JOBS = True

    def _config_general_variables(self):
        Config.DEBUG_MODE = self.args.debug
        Config.VERBOSE_MODE = self.args.verbose
        Config.PARTIAL_RUN = self.args.partial_run
        Config.SEND_REPORT = self.args.report
        Config.SKIP_TO_RESULTS = self.args.skip_to_results
        Config.SKIP_WARMUP = self.args.skip_warmup
        Config.SKIP_INSERT = self.args.skip_insert
        if Config.CURRENT_ENV['label'] == 'prod':
            Config.TEMP_TABLES = self.args.temp_tables
        else:
            Config.TEMP_TABLES = True
        if Config.PARTIAL_RUN and not self.args.jobs:
            SystemExiter.Instance().exit('Error: You can\'t omit param --jobs if you are using --partial-run.')
        if Config.SKIP_TO_RESULTS:
            Config.PARTIAL_RUN = True
        if self.args.temp_label:
            Config.TEMP_TABLES_LABEL = "_" + self.args.temp_label

    def _show_config_message(self):
        Log.Instance().appendFinalReport(self._configuration_message(self.args))
        if not self.args.force_config:
            print('#  Start ETL with this configuration? (y/n)')
            option = sys.stdin.readline().strip().lower()
            if option not in ['y', 'yes']:
                raise SystemExit

    def _configure_workdirectory(self):
        Config.WORKDIRECTORY = Config.CURRENT_ENV['workdirectory']
        if self.args.output:
            Config.WORKDIRECTORY = self.args.output
        Config.WORKDIRECTORY_FOR_KPIS = Config.WORKDIRECTORY + '{date}' + Config.PATH_FOR_KPIS
        Config.WORKDIRECTORY_FOR_TEMPS = Config.WORKDIRECTORY + '{date}' + Config.PATH_FOR_TEMPS

    def _configuration_message(self, args):
        message = '#  DASHBOARD ETL4 CONFIGURATION'
        message += '\n#'
        message += '\n#                   from: '+str(Config.START_DATE)
        message += '\n#                     to: '+str(Config.END_DATE)
        message += '\n#       steps to execute: '
        steps = []
        if args.transform: steps.append('T')
        if args.load: steps.append('L')
        message += ', '.join(steps)
        message += '\n# BASIC:'
        message += '\n#                   jobs: '+ str(', '.join(Config.JOBS_NAMES))
        message += '\n#                    env: '+ Config.CURRENT_ENV['label']
        message += '\n#            output path: '+ Config.WORKDIRECTORY
        message += '\n#             target API: '+ Config.CURRENT_ENV['api']
        message += '\n#'
        message += '\n# ADVANCED:'
        message += '\n#            debug mode?: '+ str(Config.DEBUG_MODE)
        message += '\n#           send report?: '+ str(Config.SEND_REPORT)
        message += '\n#           partial run?: '+ str(Config.PARTIAL_RUN)
        message += '\n#       skip to results?: '+ str(Config.SKIP_TO_RESULTS)
        message += '\n#           skip warmup?: '+ str(Config.SKIP_WARMUP)
        message += '\n#           temp tables?: '+ str(Config.TEMP_TABLES)
        if Config.TEMP_TABLES:
            message += '\n#      temp tables label: '+ str(Config.TEMP_TABLES_LABEL)
        message += '\n#'
        return message
