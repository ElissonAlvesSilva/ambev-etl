import glob
from os.path import expanduser

class Config:
  """
    Contain all configurable variables of the system.
    Also translate clients names between default and specific.
    """

  # MYSQL SERVERS
  MYSQL_SERVERS = {
      'prod': {
          'host': '',
          'port': 10001,
          'user': '',
          'password': '',
          'database': 'default',
          'label': 'prod'
      },
      'test': {
          'host': 'localhost',
          'port': 10000,
          'user': '',
          'password': '',
          'database': 'default',
          'label': 'test'
      },
      'dev': {
          'host': 'localhost',
          'port': 3306,
          'user': 'ambev',
          'password': 'Amb3v123',
          'database': 'ambev_development',
          'label': 'dev'
      }
  }

  AMBEV_API = {
    'urls': {
        'dev': 'http://localhost:4001/ambevapi',
        'prod': ''
    },
    'auth': {'user': '', 'pwd': ''},
    'path': '/v1/<CRIAR>/',
    'name': 'ambev-api'
  }

  # ENVIRONMENTS
  ENVIRONMENTS = {
      'dev': {
          'api': AMBEV_API['urls']['dev'],
          'ambev-api': AMBEV_API['urls']['dev'] + AMBEV_API['path'],
          'workdirectory': expanduser("~")+'/etl-data-dev/',
          'mysql-server': MYSQL_SERVERS['dev'],
          'label': 'dev'
      },
      'prod': {
          'api': AMBEV_API['urls']['prod'],
          'dashboard-api': AMBEV_API['urls']['prod'] + AMBEV_API['path'],
          'workdirectory': expanduser("~")+'/etl-data/',
          'mysql-server': MYSQL_SERVERS['prod'],
          'label': 'prod'
      },
      'test_virtual': {
          'api': AMBEV_API['urls']['dev'],
          'dashboard-api': AMBEV_API['urls']['dev'] + AMBEV_API['path'],
          'workdirectory': expanduser("~")+'/etl-data-test/',
          'mysql-server': MYSQL_SERVERS['test'],
          'label': 'test_virtual'
      },
      'test_prod': {
          'api': AMBEV_API['urls']['dev'],
          'dashboard-api': AMBEV_API['urls']['dev'] + AMBEV_API['path'],
          'workdirectory': expanduser("~")+'/etl-data-test/',
          'mysql-server': MYSQL_SERVERS['prod'],
          'label': 'test_prod'
      }
  }
  CURRENT_ENV = None

  # WORKDIRECTORY (where the logs and KPIs will be saved)
  WORKDIRECTORY = expanduser("~") + '/etl-data-dev/'
  PATH_FOR_TEMPS = '/temp/'
  PATH_FOR_KPIS = '/kpis/'
  WORKDIRECTORY_FOR_KPIS = ""
  WORKDIRECTORY_FOR_TEMPS = ""

  # DEFINITION FILES DIRECTORIES (define the queries to be executed)
  ROOT_FOLDER_LEVEL = ""
  DEFINITION_FILES = []
  @staticmethod
  def generate_definition_files_path():
      if Config.RUN_JOBS:
          Config.DEFINITION_FILES = glob.glob(Config.ROOT_FOLDER_LEVEL + 'jobs_definitions/ambev/*/*.json')
      else:
          Config.DEFINITION_FILES += glob.glob(Config.ROOT_FOLDER_LEVEL + 'jobs_definitions/ambev/*/*.json')

  
  # USER CONFIGURABLE VARIABLES
  JOBS_NAMES = []                     # queries that will be executed
  RUN_JOBS = False                    # if True, automatically run all jobs for service
  START_DATE = None
  END_DATE = None
  DEBUG_MODE = False                  # if True, the result of source and intermediare queries will also be saved into files
  PARTIAL_RUN = False                 # if True, only the exact named queries will be run, not the previous queries
  SKIP_TO_RESULTS = False             # if True, just obtain the query result already processed in the MySQL table
  VERBOSE_MODE = False                # if True more messages will be printed, including the HQL query and preview results after each query is finished
  SEND_REPORT = False                 # if True, send mail reports to emails in SEND_REPORT_TO
  TEMP_TABLES = False                 # if True, the results will be processed in tables with alternative names, instead of the original tables
  TEMP_TABLES_LABEL = "temp"          # the additional label given to temp tables
  SKIP_WARMUP = False                 # if True, ignores warmup. Useful when running ETL for old dumps.
  SKIP_INSERT = False                 # if True, do not execute queries that use statement INSERT INTO (used to fill ambev table)

  # OTHER CONFIGURATIONS
  DEBUG_MAXRESULTS = 50               # if VERBOSE_MODE is True, this is the quantity of preview results that will be shown
  JOB_RETRIES = 20                    # how many times a job (i.e.: the Thread processing a query in MySQL) will retry in case of exceptions, before aborting the system
  THREAD_POOL = 10                    # how many Threads will be created to run queries in parallel
  SILENCED_MODE = False               # if True, nothing is printed at console
  SEND_REPORT_TO = ['nossilesilva@gmail.com']
