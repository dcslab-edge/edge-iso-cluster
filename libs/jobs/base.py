# coding: UTF-8

from abc import ABCMeta, abstractmethod


class Job(metaclass=ABCMeta):

    def __init__(self, job_name: str, job_type: str, job_preferences: str, job_objective) -> None:
        self._name = job_name                   # workload name (e.g., SparkDSLRCpu)
        self._type = job_type                   # fg or bg
        # job_preferences: cpu or gpu; If gpu is selected, the cluster scheduler will dispatch this job to GPU Node
        self._preferences = job_preferences
        self._objective = job_objective  # latency or throughput

    @property
    def name(self):
        return self._name

    @property
    def type(self):
        return self._type

    @property
    def preferences(self):
        return self._preferences

    @property
    def objective(self):
        return self._objective
