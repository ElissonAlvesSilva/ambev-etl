import pymysql.cursors
import warnings
from config import Config
from utils.singleton import Singleton

@Singleton
class MysqlConnection:
  """
    Connects to the Mysql server to processes the queries.
  """
  def execute(self, statement, table_name='', return_result=False):
    # Connect to the database
    connection = pymysql.connect(
            host=Config.CURRENT_ENV['mysql-server']['host'],
            user=Config.CURRENT_ENV['mysql-server']['user'],
            password=Config.CURRENT_ENV['mysql-server']['password'],
            db=Config.CURRENT_ENV['mysql-server']['database'],
            cursorclass=pymysql.cursors.DictCursor)
    try:
      with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        cur = connection.cursor()
        cur.execute(statement)
        if return_result is False:
          connection.commit()
        elif return_result is True:
            array = []
            for i in cur.fetchall():
                array.append(i)
            schema = [row[0] for row in cur.description]
            return schema, array
    finally:
      connection.close()
  
  def table_exists(self, name):
    connection = pymysql.connect(host = Config.CURRENT_ENV['mysql-server']['host'],
        user=Config.CURRENT_ENV['mysql-server']['user'],
        password=Config.CURRENT_ENV['mysql-server']['password'],
        db=Config.CURRENT_ENV['mysql-server']['database'],
        cursorclass=pymysql.cursors.DictCursor)
    with connection.cursor() as cur:
      cur.execute("SHOW TABLES LIKE '" + name + "'")
      if len(cur.fetchall()) == 0:
        return False
      else:
        return True