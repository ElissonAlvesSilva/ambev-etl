#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import defaultdict
from datetime import datetime

from config import Config
from utils.log import Log


class OutputFormatter():

  def __init__(self, name, kpi_name, api, execution_time,
             schema, data, is_kpi, datatype):
    self.name = name
    self.kpi_name = kpi_name
    self.api = api
    self.execution_time = execution_time
    self.schema = schema
    self.data = data
    self.is_kpi = is_kpi
    self.datatype = datatype

  def to_json_collections(self):
    result_date_field = self._add_date_field(self.data)
    result = self._remove_none(result_date_field)
    return result
  
  def _remove_none(self, collection):
    results = []
    for item in collection:
      result = {}
      for k,v in item.items():
        if v == None:
          result[k] = 'null'
        else:
          result[k] = v
        results.append(result)
    return results

  def _add_date_field(self, collection):
    results = []
    count_errors = 0
    for item in collection:
      if 'created_at' in item:
        date = item['created_at']
        item['date'] = date.strftime("%Y-%m-%d")
        item.pop('created_at')
      else:
        try:
          item_date = datetime(int(item['year']), int(item['month']), int(item['day']))
          item['date'] = item_date.strftime("%Y-%m-%d")
        except KeyError:
          item['date'] = "temp"
        except TypeError:
          count_errors += 1
          continue
        except ValueError:
          count_errors += 1
          continue
      results.append(item)
    Log.Instance().appendFinalReport("Error parsing date field. Count: %s" %(count_errors))
    return results

  def generate_message(self):
    message = "===========================================\n\n"
    message += "Results for " + self.name + " (%.2f minutes)." % (self.execution_time) + "\n\n"
    message += "Query schema: " + str(self.schema) + "\n\n"
    message += "Printing results...\n"
    if len(self.data) == 0:
      message += "There are no results to be shown.\n"
    for i, item in enumerate(self.data):
      if i < Config.DEBUG_MAXRESULTS:
        message += str(item) + "\n"
      else:
        message += "The full result is too big to be printed (" + str(len(self.data)) + ").\n"
        message += "Printing the first " + str(i) + " results.\n"
        break
    message += "===========================================\n\n"
    return message

