#!/usr/bin/env python
# -*- coding: utf-8 -*-

from utils.singleton import Singleton

@Singleton
class JobsExecutionInfo:
    """
    Encapsulates the jobs execution info.
    """

    def __init__(self):
        self.all_jobs = []
        self.loaded_jobs = {}
        self.executed_jobs = []
        self.jobs_to_execute = []

    def initialize(self, jobs_to_run, loaded_jobs):
        self.all_jobs = [job.name for job in jobs_to_run]
        self.loaded_jobs = loaded_jobs

    def report(self):
        self.executed_jobs = []
        self.jobs_to_execute = []
        for job in self.all_jobs:
            if self.loaded_jobs[job].executed:
                self.executed_jobs.append(job)
            else:
                self.jobs_to_execute.append(job)
        message = "\n\n<b>## JOBS EXECUTION STATUS: ##</b>"
        message += "\n\n<b>Finished jobs:</b> "+str(self.executed_jobs)
        message += "\n\n<b>Jobs that didn't finish</b>: "+str(self.jobs_to_execute)
        if len(self.jobs_to_execute) > 0:
            message += "\n\nThe ETL seems to have failed during the RUNNING STAGE of the Transform"
            message += "\n- Use these parameters to run ETL from where it stopped: -p -j \"" +\
                       ','.join(self.jobs_to_execute) + "\""
            message += "\n(Ensure that ETL ins't still running jobs before executing it again!)"
        else:
            message += "\n\nThe ETL seems to have failed during the GET RESULTS STAGE or" \
                       " the PROCESS RESULTS STAGE of the Transform"
            message += "\n- Use this parameter to run ETL from where it stopped: -sr"
        message += "\n\n"
        return message
