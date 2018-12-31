#!/usr/bin/env python3
# coding: UTF-8

import argparse
import datetime
import logging
import os
import sys
import time
from typing import Dict

import socket
import libs

from pending_job_queue import PendingJobQueue
from polling_thread import PollingThread
from libs.node import Node
from libs.jobs import Job
from node_tracker import NodeTracker

MIN_PYTHON = (3, 6)


class ClusterScheduler:
    def __init__(self, metric_buf_size: int) -> None:
        self._pending_job_queue: PendingJobQueue = PendingJobQueue()

        self._interval: float = 1.0  # scheduling interval (sec)

        self._dispatch_candidate_jobs: Dict[Job, str] = dict()  #

        self._polling_thread = PollingThread(metric_buf_size, self._pending_job_queue)
        self._node_tracker = NodeTracker(metric_buf_size=50)  # aggr_metric_bufsize is initially set to 50

    def _pick_job_from_pending_queue(self) -> None:
        """
        This function checks the pending jobs and if there any pending jobs, then dispatch it to least cont node
        :return:
        """
        logger = logging.getLogger(__name__)
        job_queue = self._pending_job_queue
        while len(job_queue) > 0:
            if job_queue.lat_jobs > 0:
                pending_latency_job: Job = self._pending_job_queue.pop('latency')
                logger.info(f'{pending_latency_job} is created')
                self._dispatch_candidate_jobs[pending_latency_job] = 'ready to dispatch'

            elif job_queue.lat_jobs == 0 and job_queue.thr_jobs > 0:
                pending_throughput_job: Job = self._pending_job_queue.pop('throughput')
                logger.info(f'{pending_throughput_job} is created')
                self._dispatch_candidate_jobs[pending_throughput_job] = 'ready to dispatch'

    def dispatch_jobs(self) -> None:
        logger = logging.getLogger(__name__)

        for job in self._dispatch_candidate_jobs.keys():
            logger.info('')
            logger.info(f'*********dispatch of {job.name} ({job.objective} {job.type} job) ***********')
            self._do_dispatch_job(job)
            self._dispatch_candidate_jobs[job] = f'dispatch success dest_node ({job.dest_ip}:{job.dest_port})'

    def _do_dispatch_job(self, job: Job) -> None:
        logger = logging.getLogger(__name__)
        self._node_tracker.find_min_aggr_cont_node()
        dest_node: Node = self._node_tracker.min_aggr_cont_node
        print(f'dest_node: {dest_node}')
        if dest_node is not None:
            job.dest_ip = dest_node.ip_addr
            job.dest_port = dest_node.port
            logger.info(f'{job.name} is dispatched to the {job.type} host-{dest_node.ip_addr}')
            # FIXME: socket communication is blocking-based. It should be fixed in non-blocking manner
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((dest_node.ip_addr, dest_node.port))
                data = f'{job.name},{job.type},{job.preferences}'
                s.sendall(data.encode())
                resp = s.recv(1024)
                logger.info(f'Received from {resp.decode()} {dest_node.ip_addr}:{dest_node.port}!')

    def run(self) -> None:
        self._polling_thread.start()
        self._node_tracker.start()

        logger = logging.getLogger(__name__)
        logger.info('starting cluster scheduler loop')
        while True:
            self._pick_job_from_pending_queue()

            time.sleep(self._interval)
            self.dispatch_jobs()


def main() -> None:
    parser = argparse.ArgumentParser(description='Run workloads that given by parameter.')
    parser.add_argument('-b', '--metric-buf-size', dest='buf_size', default='50', type=int,
                        help='metric buffer size per thread. (default : 50)')

    os.makedirs('logs', exist_ok=True)

    args = parser.parse_args()

    formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s')
    stream_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(f'logs/debug_{datetime.datetime.now().isoformat()}.log')
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    controller_logger = logging.getLogger(__name__)
    controller_logger.setLevel(logging.INFO)
    controller_logger.addHandler(stream_handler)
    controller_logger.addHandler(file_handler)

    module_logger = logging.getLogger(libs.__name__)
    module_logger.setLevel(logging.DEBUG)
    module_logger.addHandler(stream_handler)
    module_logger.addHandler(file_handler)

    monitoring_logger = logging.getLogger('monitoring')
    monitoring_logger.setLevel(logging.INFO)
    monitoring_logger.addHandler(stream_handler)
    monitoring_logger.addHandler(file_handler)

    cluster_scheduler = ClusterScheduler(args.buf_size)
    cluster_scheduler.run()
    node_tracker = NodeTracker(args.buf_size)
    node_tracker.run()


if __name__ == '__main__':
    if sys.version_info < MIN_PYTHON:
        sys.exit('Python {}.{} or later is required.\n'.format(*MIN_PYTHON))

    main()
