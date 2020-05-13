#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re

from config import Config
from utils.singleton import Singleton


@Singleton
class QueryFormatter():

  TABLE_NAME_FORMAT = "ambev_etl_{job_name}"
  FILES_PATTERN = '{{{file_name}}}'                   # '{','}' have to be escaped with other pair of { }
  PREVIOUS_JOB_TABLE_PATTERN = '{{{job_name}}}'       # '{','}' have to be escaped with other pair of { }
  CONSTRAIN_RE = re.compile('(\{.*?\})')    
  CONSTRAIN_FIELDS_RE = re.compile('([^\.\{\}]+)')

  def __init__(self):
    self.start_date = Config.START_DATE
    self.end_date = Config.END_DATE
    if 'test' in Config.CURRENT_ENV['label']:
      self.TABLE_NAME_FORMAT += "_test"
    if Config.TEMP_TABLES:
      self.TABLE_NAME_FORMAT += Config.TEMP_TABLES_LABEL

  def create_constrained_query(self, raw_query, previous_jobs):
    raw_query = self._replace_previous_jobs(raw_query, previous_jobs)
    return self._format_query(raw_query)

  def _replace_previous_jobs(self, raw_query, previous_jobs):
    for previous_job_name in previous_jobs:
      pattern = self.PREVIOUS_JOB_TABLE_PATTERN.format(job_name=previous_job_name)
      pj_name = self.TABLE_NAME_FORMAT.format(job_name=previous_job_name)
      raw_query = raw_query.replace(pattern, pj_name)
    return raw_query

  def _format_query(self, query):
    query_parameters = self._get_query_parameters(query)
    for p in query_parameters:
      constraint = self._constrain_parameter(p)
      query = query.replace(p, constraint)
    return query

  def _get_query_parameters(self, query):
    return set(self.CONSTRAIN_RE.findall(query))

  def _constrain_parameter(self, parameter):
    table, field = self._parse_parameter(parameter)
    if field == 'start_date':
      return str(self.start_date)
    elif field == 'end_date':
      return str(self.end_date)
    else:
      raise NameError(field + ' unknown')

  def _parse_parameter(self, parameter):
    matching_parts = self.CONSTRAIN_FIELDS_RE.findall(parameter)
    table_accessor = '.'.join(matching_parts[:-1])
    field_accessor = matching_parts[-1]
    return [table_accessor, field_accessor]
    
  def _table_accessor(self, table):
    return table + '.' if table else ''
