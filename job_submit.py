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

    @staticmethod
    def do_submit(job_request: str):
        RabbitMQHandler(job_request)


class RabbitMQHandler(Handler):
    def __init__(self, job_request: str):
        super().__init__()
        # TODO: upgrade to async version
        self._connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
        self._channel: BlockingChannel = self._connection.channel()

        self._queue_name: str = f'job_submission'
        self._channel.queue_declare(queue=self._queue_name)

        # Send job description to cluster scheduler's polling thread
        self._channel.queue_declare(queue=self._queue_name)
        self._channel.basic_publish(exchange='', routing_key=self._queue_name,
                                    body=job_request)

    def emit(self, record: LogRecord):
        formatted: str = self.format(record)

        self._channel.basic_publish(exchange='', routing_key=self._queue_name, body=formatted)

    def close(self):
        super().close()
        try:
            self._channel.queue_delete(self._queue_name)
        except:
            pass
        self._connection.close()

    def __repr__(self):
        level = logging.getLevelName(self.level)
        return f'<{self.__class__.__name__} {self._queue_name} ({level})>'


def main() -> None:
    interval = 10.0
    job_submitter = JobSubmitter(submit_interval=interval)
    job_submitter.open_and_parse_config()

    number_of_jobs = len(job_submitter.job_requests)
    left_jobs = number_of_jobs
    req_count = 0
    while left_jobs:
        time.sleep(interval)
        job_req = job_submitter.job_requests[req_count]
        job_submitter.do_submit(job_req)
