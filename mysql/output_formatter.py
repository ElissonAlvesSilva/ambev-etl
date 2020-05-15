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
      result_fix_field = self._fix_total_field(result_date_field)
      result_rm_none = self._remove_none(result_fix_field)
      result_group_by_kpi_name = self._group_kpi_name(result_rm_none)
      result = self._add_structure_per_group(result_group_by_kpi_name)
      return result
    return []
  
  def _remove_none(self, collection):
    results = []
    for item in collection:
      for k,v in item.items():
        if v == None:
          item[k] = 'null'
      results.append(item)
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
  
  def _fix_total_field(self, collections):
    results = []
    for item in collections:
      if 'total' in item:
        item['total'] = round(item['total'], 2)
      if 'total_spent' in item:
        item['total_spent'] = round(item['total_spent'], 2)
      if 'total_amount' in item:
        item['total_amount'] = round(item['total_amount'], 2)
      if 'total_pc' in item:
        item['total_pc'] = round(item['total_pc'], 2)
      if 'total_qty' in item:
        item['total_qty'] = round(item['total_qty'], 2)
      if 'total_hl' in item:
        item['total_hl'] = round(item['total_hl'], 2)
      results.append(item)
    return results

  def _group_kpi_name(self, collections):
    grouped_results = defaultdict(list)
    grouped_by_created_at = dict()
    for item in collections:
      kpi_name = self.kpi_name
      created_at = item['created_at']
      if created_at not in grouped_by_created_at:
        grouped_by_created_at[created_at] = defaultdict(list)
      grouped_by_created_at[created_at][kpi_name].append(item)
      grouped_results = grouped_by_created_at
    return grouped_results
    
  def _add_structure_per_group(self, grouped_results):
    collections = []
    for grouper in grouped_results:
      collection = self._create_collection(grouped_results, grouper)
      collections.append(collection)
    return collections

  def _create_collection(self, grouped_results, date):
    collection = {
      "etl_data": grouped_results[date],
      "etl_meta": {
        "timestamp": date,
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

