# coding: UTF-8

from collections import deque
from itertools import chain
from typing import Deque, Iterable, Optional, Set, Tuple

from .metric_container.basic_metric import BasicMetric, MetricDiff


class Node:
    def __init__(self, ip_addr: str, port: str, node_type: str):
        self._ip_addr = ip_addr
        self._port = port
        self._node_type = node_type             # gpu or cpu
        self._identifier = self._ip_addr + '_' + self._node_type

        # Workload related
        self._num_workloads: int = None
        self._num_of_fg_wls: int = None  # Assumed a single fg wls
        self._num_of_bg_wls: int = None
        #self._aggr_contention = None     # Diff?
        self._metrics: Deque[BasicMetric] = deque()

    @property
    def ip_addr(self):
        return self._ip_addr

    @property
    def port(self):
        return self._port

    @property
    def node_type(self):
        return self._node_type

    @property
    def identifier(self):
        return self._identifier

    @property
    def num_workloads(self):
        return self._num_workloads

    @property
    def num_of_fg_wls(self):
        return self._num_of_fg_wls

    @property
    def num_of_bg_wls(self):
        return self._num_of_bg_wls

    @property
    def aggr_contention(self):
        return self._aggr_contention

    @num_workloads.setter
    def num_workloads(self, num_wls: int):
        self._num_workloads = num_wls

    @num_of_fg_wls.setter
    def num_of_fg_wls(self, num_fg_wls: int):
        self._num_of_fg_wls = num_fg_wls

    @num_of_bg_wls.setter
    def num_of_bg_wls(self, num_bg_wls: int):
        self._num_of_bg_wls = num_bg_wls

    @aggr_contention.setter
    def aggr_contention(self, aggr_cont: float):
        self._aggr_contention = aggr_cont

    @node_type.setter
    def node_type(self, node_type: str):
        self._node_type = node_type

    @property
    def metrics(self) -> Deque[BasicMetric]:
        return self._metrics
