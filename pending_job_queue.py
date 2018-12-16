# coding: UTF-8

import logging
from typing import List, Sized
from libs.jobs import Job


class PendingJobQueue(Sized):
    def __init__(self) -> None:

        self._pending_list_latency: List[Job] = list()      # pending latency-critical workload pairs
        self._pending_list_throughput: List[Job] = list()   # pending throughput-oriented workload pairs

    def __len__(self) -> int:
        num_lat_job = len(self._pending_list_latency)
        num_thr_job = len(self._pending_list_throughput)
        if num_lat_job > 0 or num_thr_job > 0:
            num_of_pending_jobs = num_lat_job + num_thr_job
            return num_of_pending_jobs

    def add(self, job: Job):
        logger = logging.getLogger('pending')
        logger.info(f'Job ({job.name}) is added...')

        # Always latency pending list is firstly checked!
        if job.objective == 'latency':
            self._pending_list_latency.append(job)
        elif job.objective == 'throughput':
            self._pending_list_throughput.append(job)

    def pop(self, job_obj) -> Job:
        if len(self) is 0:
            raise IndexError(f'{self} is empty')
        if job_obj == 'latency':
            return self._pending_list_latency.pop()
        elif job_obj == 'throughput':
            return self._pending_list_throughput.pop()
