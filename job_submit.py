#!/usr/bin/env python3
# coding: UTF-8

import sys
import pika
import json
import logging
import time

from typing import Dict, Any

from pathlib import Path
from logging import Handler, LogRecord
from pika.adapters.blocking_connection import BlockingChannel

MIN_PYTHON = (3, 6)


class JobSubmitter:
    def __init__(self, submit_interval: float):
        config_file = Path.cwd() / 'submit_config.json'
        self._job_cfg_file = config_file
        self._job_requests: Dict[int, str] = None
        self._job_submit_interval: float = submit_interval  # seconds

    @property
    def job_requests(self):
        return self._job_requests

    def open_and_parse_config(self):
        print('open and parse config')
        if not self._job_cfg_file.exists():
            print(f'{self._job_cfg_file.resolve()} is not exist.', file=sys.stderr)
            return False

        job_requests: Dict[int, str] = dict()   # req_num : req_body
        with self._job_cfg_file.open() as job_config_fp:
            job_cfg_source: Dict[str, Any] = json.load(job_config_fp)
            job_cfgs = job_cfg_source['jobs']
            for req_num, job_cfg in enumerate(job_cfgs):
                job_name = job_cfg['name']
                job_type = job_cfg['type']
                job_preferences = job_cfg['preferences']
                job_objective = job_cfg['objective']
                job_requests[req_num] = f'{job_name},{job_type},{job_preferences},{job_objective}'
                self._job_requests = job_requests

    @staticmethod
    def do_submit(job_request: str):
        print(f'do_submit! : {job_request}')
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        channel: BlockingChannel = connection.channel()

        queue_name: str = f'job_submission'
        channel.queue_declare(queue=queue_name)

        # Send job description to cluster scheduler's polling thread
        channel.queue_declare(queue=queue_name)
        channel.basic_publish(exchange='', routing_key=queue_name, body=job_request)


def main() -> None:
    interval = 5.0
    job_submitter = JobSubmitter(submit_interval=interval)
    job_submitter.open_and_parse_config()

    number_of_jobs = len(job_submitter.job_requests)
    left_jobs = number_of_jobs
    req_count = 0
    print(f'left_jobs: {left_jobs}')
    while left_jobs:
        time.sleep(interval)
        job_req = job_submitter.job_requests[req_count]
        job_submitter.do_submit(job_req)
        left_jobs -= 1
        req_count += 1
        print(f'left_jobs: {left_jobs}')


if __name__ == '__main__':
    if sys.version_info < MIN_PYTHON:
        sys.exit('Python {}.{} or later is required.\n'.format(*MIN_PYTHON))

    main()

