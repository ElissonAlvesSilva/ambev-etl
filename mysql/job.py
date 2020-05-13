#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time

import sqlparse  # pretty print of SQL statement

from config import Config
from .mysql_connection import MysqlConnection
from .output_formatter import OutputFormatter
from .query_formatter import QueryFormatter
from utils.log import Log
from utils.system_exiter import SystemExiter


class Job():

    DELETE_STATEMENT_TEMPLATE = "DROP TABLE IF EXISTS {table_name}"
    REMOVE_STATEMENT_TEMPLATE = "DELETE FROM {table_name}"
    CREATE_STATEMENT_TEMPLATE = "CREATE TABLE IF NOT EXISTS {table_name} AS {query}"
    INSERT_STATEMENT_TEMPLATE = "REPLACE INTO {table_name} {schema} {query}"
    SHOW_RESULT_STATEMENT_TEMPLATE = "SELECT * FROM {table_name}"

    def __init__(self,
                 name,
                 kpi_name,
                 api,
                 previous_jobs,
                 raw_query,
                 datatype,
                 action,
                 table_name,
                 schema):
        self.name = name
        self.kpi_name = kpi_name
        self.api = api
        self.previous_jobs = previous_jobs or []
        self.datatype = datatype
        self.action = action
        self.table_name = table_name
        self.executed = True if Config.SKIP_INSERT and action == 'insert' else False
        self.execution_time = -1
        self.schema = schema
        self.query = QueryFormatter.Instance().create_constrained_query(raw_query, previous_jobs)

    def execute(self, method, retries):
      test = False
      cont = 1
      while test is False:
        test = True
        try:
          return getattr(self, method)()
        except Exception as e:
          Log.Instance().appendFinalReport("THREAD EXCEPTION at " + method + ", " +
                                          self.name + ": " + str(e) + ", type: " +
                                          str(type(e)) + ", RETRYING (" + str(retries) +
                                          " times), retry: " + str(cont))
          test = False
          cont += 1
          if cont > retries:
            message = "<<< Tried " + str(retries) +\
                      " times and failed, ABORTING THREAD! >>>\n\n"
            message += self.name + " - " + str(e) + ", type: " + str(type(e))
            message += JobsExecutionInfo.Instance().report()
            SystemExiter.Instance().exit(message, isThread=True)
          else:
            time.sleep(60)

    def create(self):
      Log.Instance().appendFinalReport("Executing: " + self._table_name() + "  ")
      start_time = time.time()
      if Config.VERBOSE_MODE:
        Log.Instance().append("\nExecuting:\n"+self._get_drop_statement())
      while MysqlConnection.Instance().table_exists(self._table_name()):
        MysqlConnection.Instance().execute(self._get_drop_statement(), self._table_name())
      if Config.VERBOSE_MODE:
        Log.Instance().append("\nExecuting:\n" + self._get_create_statement())
        Log.Instance().append("\n---------------\n")
      MysqlConnection.Instance().execute(self._get_create_statement(), self._table_name())
      elapsed_seconds = time.time() - start_time
      self.execution_time = int(elapsed_seconds/float(60) * 100) / 100.0
      self.executed = True
      Log.Instance().appendFinalReport("...finished executing: " + self._table_name() +
                                       " (" + str(self.execution_time) + " mins)  ")

    def insert(self):
      Log.Instance().appendFinalReport("Executing: " + self.table_name + "  ")
      start_time = time.time()
      if Config.VERBOSE_MODE:
        Log.Instance().append("\nExecuting:\n" + self._get_insert_statement())
        Log.Instance().append("\n---------------\n")
      MysqlConnection.Instance().execute(self._get_remove_statement(), self.table_name)
      MysqlConnection.Instance().execute(self._get_insert_statement(), self.table_name)
      elapsed_seconds = time.time() - start_time
      self.execution_time = int(elapsed_seconds/float(60) * 100) / 100.0
      self.executed = True
      Log.Instance().appendFinalReport("...finished executing: " + self.table_name +
                                       " (" + str(self.execution_time) + " mins)  ")

    def _get_drop_statement(self):
      return self.DELETE_STATEMENT_TEMPLATE.format(table_name=self._table_name())
    
    def _get_remove_statement(self):
      return self.REMOVE_STATEMENT_TEMPLATE.format(table_name=self.table_name)

    def _table_name(self):
      return QueryFormatter.Instance().TABLE_NAME_FORMAT.format(job_name=self.name)

    def _get_create_statement(self):
      return self.CREATE_STATEMENT_TEMPLATE.format(
        table_name=self._table_name(),
        query=sqlparse.format(self.query, reindent=True)
      )

    def _get_insert_statement(self):
      if self.is_kpi():
        return self.INSERT_STATEMENT_TEMPLATE.format(
          table_name=self.table_name,
          query=sqlparse.format(self.query, reindent=True)
        )
      else:
        return self.INSERT_STATEMENT_TEMPLATE.format(
          table_name=self.table_name,
          schema=self.schema,
          query=sqlparse.format(self.query, reindent=True)
        )
    
    def results(self):
      Log.Instance().appendFinalReport("Getting result: " + self._table_name() + "  ")
      result_statement = self.SHOW_RESULT_STATEMENT_TEMPLATE.format(table_name=self._table_name())
      schema, data = MysqlConnection.Instance().execute(result_statement, self._table_name(),
                                                       return_result=True)
      schema = [x.replace(self._table_name() + ".", "") for x in schema]
      Log.Instance().appendFinalReport("...finished getting result: " + self._table_name() + "  ")
      return OutputFormatter(
        self.name,
        self.kpi_name,
        self.api,
        self.execution_time,
        schema,
        data,
        self.is_kpi(),
        self.datatype,
      )
    
    def is_kpi(self):
      if self.datatype == 'dailyfact' or self.datatype == 'rawdata' or self.datatype == 'unicity':
        return True
      return False