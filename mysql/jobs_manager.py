#!/usr/bin/env python
# -*- coding: utf-8 -*-

import time
from collections import OrderedDict
from multiprocessing.pool import ThreadPool
from mysql.graph import Graph

from config import Config
from mysql.jobs_execution import JobsExecutionInfo
from utils.log import Log

class JobsManager():

	def __init__(self, loaded_jobs):
		self.loaded_jobs = loaded_jobs
		self.jobs_dont_get_results = ['']

	def run(self):
		Log.Instance().appendFinalReport("\nStarting EXTRACT/TRANSFORM stage...\n================")
		jobs_to_run = self._generate_jobs_to_run()
		if not Config.SKIP_TO_RESULTS:
			execution_graph = Graph(jobs_to_run)
			jobs_to_get_results = self._execute_queries(jobs_to_run, execution_graph)
		else:
			jobs_to_get_results = self._generate_jobs_to_get_results(jobs_to_run)
		final_results = self._get_queries_results(jobs_to_get_results)        
		Log.Instance().appendFinalReport("================\nEXTRACT/TRANSFORM stage ended.")
		return final_results

	def _generate_jobs_to_run(self):
		if Config.PARTIAL_RUN:
			jobs_names_to_run = Config.JOBS_NAMES
		else:
			jobs_names_to_run = self._get_job_chain(Config.JOBS_NAMES)
		jobs_to_run = []
		for job_name in jobs_names_to_run:
			if self.loaded_jobs[job_name].executed is False:
				jobs_to_run.append(self.loaded_jobs[job_name])
		return jobs_to_run

	def _generate_jobs_to_get_results(self, jobs_to_run):
		jobs_to_get_results = []
		for job in jobs_to_run:
			if job.is_kpi() or (Config.DEBUG_MODE and job.name not in self.jobs_dont_get_results):
				jobs_to_get_results.append(job)
		return jobs_to_get_results

	def _get_job_chain(self, job_names):
		loaded_jobs_to_run = [self._get_previous_jobs(jn) for jn in job_names]
		loaded_jobs_to_run = sum(loaded_jobs_to_run, [])                        # flat list
		return list(OrderedDict.fromkeys(loaded_jobs_to_run))                   # remove duplicated jobs, preserve order

	def _get_previous_jobs(self, job_name):
		job_definition = self.loaded_jobs[job_name]
		job_chain = [job_definition.name]
		for previous_job in job_definition.previous_jobs:
			previous_job_chain = self._get_previous_jobs(previous_job)
			job_chain = previous_job_chain + job_chain
		return job_chain

	def _execute_queries(self, jobs_to_run, graph):
		Log.Instance().appendFinalReport("++++++ RUNNING STAGE ++++++")
		pool_execution = ThreadPool(Config.THREAD_POOL)
		JobsExecutionInfo.Instance().initialize(jobs_to_run, self.loaded_jobs)
		jobs_to_get_results = []
		jobs_to_prometheus = []
		while len(jobs_to_run) > 0:
			for job in jobs_to_run:
				if self._job_is_ready(job, graph) and not job.executed:
					pool_execution.apply_async(job.execute, (job.action, Config.JOB_RETRIES))
					jobs_to_run.remove(job)
					jobs_to_prometheus.append(job)
					if job.is_kpi() or\
							(Config.DEBUG_MODE and job.name not in self.jobs_dont_get_results):
						jobs_to_get_results.append(job)
				time.sleep(5)
		pool_execution.close()
		pool_execution.join()
		return jobs_to_get_results

	def _job_is_ready(self, job, graph):
		if graph.previous_jobs(job) == []:
			return True
		for previous_job in graph.previous_jobs(job):
			if self.loaded_jobs[previous_job].executed == False:
				return False
		return True

	def _get_queries_results(self, jobs_to_get_results):
		thread_pool = ThreadPool(1)
		raw_thread_results = self._get_raw_results(thread_pool, jobs_to_get_results)
		final_results = self._process_raw_results(raw_thread_results)
		thread_pool.close()
		thread_pool.join()
		return final_results

	def _get_raw_results(self, thread_pool, jobs_to_get_results):
		Log.Instance().appendFinalReport("\n++++++ GET RESULTS STAGE ++++++")
		results = []
		while len(jobs_to_get_results) > 0:
			for job in jobs_to_get_results:
				results.append(thread_pool.apply_async(job.execute, ("results", 100)))
				jobs_to_get_results.remove(job)
		for result in results:
			result.wait()
		return results

	def _process_raw_results(self, results):
		Log.Instance().appendFinalReport("\n++++++ PROCESS RESULTS STAGE ++++++\n")
		final_results = []
		while len(results) > 0:
			for result in results:
				query_result = result.get()
				results.remove(result)
				final_results += query_result.to_json_collections()
				if Config.VERBOSE_MODE:
					Log.Instance().append(query_result.generate_message())
		return final_results
