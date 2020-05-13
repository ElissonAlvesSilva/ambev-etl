#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re

import simplejson as json

from .job import Job
from config import Config
from utils.log import Log
from utils.singleton import Singleton
from utils.system_exiter import SystemExiter

@Singleton
class JobsLoader:
  
  def __init__(self):
    self.loaded_jobs = {}

  def load_jobs(self):
    loaded_jobs_definitions = self._load_jobs_definitions()
    for job_definition in loaded_jobs_definitions:
      new_job = self._create_job_from_definition(job_definition)
      self.loaded_jobs[new_job.name] = new_job
    self._validate_jobs()

  def _load_jobs_definitions(self):
    loaded_jobs_definitions = []
    for filename in Config.DEFINITION_FILES:
        file_content = self._read_jobs_definitions_file(filename)
        loaded_jobs_definitions += (self._parse_jobs_definitions(file_content))
    return loaded_jobs_definitions

  def _read_jobs_definitions_file(self, filename):
    file_pointer = open(filename, 'r', encoding='utf-8')
    file_content = file_pointer.read()
    file_content = self._remove_comments(file_content)
    file_content = self._remove_new_lines(file_content)
    file_pointer.close()
    return file_content

  def _remove_new_lines(self, content):
      return content.replace('\n', '')

  def _remove_comments(self, content):
    return re.compile("//.*").sub('', content)

  def _parse_jobs_definitions(self, definitions):
    return json.loads(definitions)

  def _create_job_from_definition(self, job_definition):
    name = job_definition['name']
    kpi_name = job_definition.get('kpi_name', name)
    api = job_definition.get('api', None)
    previous_jobs = job_definition.get('previous_jobs', [])
    query = job_definition['query']
    datatype = job_definition.get('datatype', None)
    action = job_definition.get('action', 'create')
    table_name = job_definition.get('table_name', None)
    schema = job_definition.get('schema', None)
    job = Job(name, kpi_name, api, previous_jobs, query, datatype, action, table_name, schema)
    return job

  def _validate_jobs(self):
    Log.Instance().appendFinalReport('\nValidating kpis/jobs...\n')
    for job in list(self.loaded_jobs.keys()):
      if self.loaded_jobs[job].action == 'insert' and self.loaded_jobs[job].table_name is None:
        SystemExiter.Instance().exit('Error: ' + job + ' needs table name to insert data')
    if len(Config.JOBS_NAMES) == 0:
      all_final_jobs = [job_name for job_name in list(self.loaded_jobs.keys())
                        if self.loaded_jobs[job_name].is_kpi()]
      if Config.RUN_JOBS:
        Config.JOBS_NAMES += [job_name for job_name in all_final_jobs]
    for job_name in Config.JOBS_NAMES:
      if job_name not in self.loaded_jobs:
        if Config.RUN_JOBS:
          SystemExiter.Instance().exit('Error: ' + job_name +
                                        ' not found in jobs definitions')
        else:
          SystemExiter.Instance().exit('Error: ' + job_name +
                                      ' not found in jobs definitions ')


