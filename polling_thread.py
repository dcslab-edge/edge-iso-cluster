# coding: UTF-8

import logging
from threading import Thread

import pika
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic

from libs.jobs import Job
from pending_job_queue import PendingJobQueue
from libs.utils.machine_type import MachineChecker, NodeType


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


# Polling new job
class PollingThread(Thread, metaclass=Singleton):
    def __init__(self, metric_buf_size: int, pending_job_queue: PendingJobQueue) -> None:
        super().__init__(daemon=True)
        self._metric_buf_size = metric_buf_size
        self._node_type = MachineChecker.get_node_type()

        self._rmq_host = 'localhost'
        self._rmq_job_submission_queue = 'job_submission'

        self._pending_jobs = pending_job_queue

    def _cbk_job_submission(self, ch: BlockingChannel, method: Basic.Deliver, _: BasicProperties, body: bytes) -> None:
        ch.basic_ack(method.delivery_tag)

        arr = body.decode().strip().split(',')

        logger = logging.getLogger('monitoring.job_submission')
        logger.debug(f'{arr} is received from job_submission queue')

        if len(arr) != 4:
            return

        job_name, job_type, job_preferences, job_objective = arr

        job = Job(job_name, job_type, job_preferences, job_objective)
        if job_type == 'bg':
            logger.info(f'{job_name} is background job')
        else:
            logger.info(f'{job_name} is foreground job')

        self._pending_jobs.add(job)

    def run(self) -> None:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._rmq_host))
        channel = connection.channel()

        channel.queue_declare(self._rmq_job_submission_queue)
        channel.basic_consume(self._cbk_job_submission, self._rmq_job_submission_queue)

        try:
            logger = logging.getLogger('monitoring')
            logger.info('starting job submission queue')
            channel.start_consuming()

        except KeyboardInterrupt:
            channel.close()
            connection.close()
