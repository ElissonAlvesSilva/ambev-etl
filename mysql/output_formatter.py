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
    if self.data != []:
      result_date_field = self._add_date_field(self.data)
      result_rm_none = self._remove_none(result_date_field)
      result = self._add_structure_per_group(result_rm_none)
      return result
    return []
  
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
        item['created_at'] = date.strftime("%Y-%m-%d")
      else:
        try:
          item_date = datetime(int(item['year']), int(item['month']), int(item['day']))
          item['created_at'] = item_date.strftime("%Y-%m-%d")
        except KeyError:
          item['created_at'] = "temp"
        except TypeError:
          count_errors += 1
          continue
        except ValueError:
          count_errors += 1
          continue
      results.append(item)
    Log.Instance().appendFinalReport("Error parsing created_at field. Count: %s" %(count_errors))
    return results

  def _group_by_kpi_name(self, collections): 
    grouped_results = []
    collection = dict()
    for item in collections:
      if self.kpi_name not in item['elt_data']:
        collection[self.kpi_name] = [item]
      else:
        collection[self.kpi_name].append(item)
      grouped_results.append(collection)
    return grouped_results
  
  def _add_structure_per_group(self, grouped_results):
    collections = []
    for item in grouped_results:
      collection = self._create_collection(item)
      collections.append(collection)
    return collections

  def _create_collection(self, item):
    collection = {
      "etl_data": item,
      "etl_meta": {
        "timestamp": item['created_at'],
        "label": self.name,
        "is_kpi": self.is_kpi,
        "datatype": self.datatype,
        "api": self.api,
        "kpi_name": self.kpi_name
      }
    }
    return collection

  

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

