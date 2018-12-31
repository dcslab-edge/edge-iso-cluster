# coding: UTF-8

from collections import deque
from itertools import chain
from typing import Deque, List

from .metric_container.basic_metric import BasicMetric, MetricDiff
from .workload import Workload


class Node:
    def __init__(self, ip_addr: str, port: str, node_type: str, hostname: str):
        self._ip_addr = ip_addr                 # ip addr. of node
        self._port = port                       # port of edge-profiler which process dispatched commands
        self._node_type = node_type             # gpu or cpu
        self._hostname = hostname
        self._identifier = self._ip_addr + '_' + self._node_type

        # Workload related
        self._num_workloads: int = None
        self._num_of_fg_wls: int = None  # Assumed a single fg wls
        self._num_of_bg_wls: int = None

        self._avg_metric: BasicMetric = None
        self._aggr_metric: BasicMetric = None
        self._workloads: List[Workload] = list()

    @property
    def ip_addr(self):
        return self._ip_addr

    @property
    def node_type(self):
        return self._node_type

    @property
    def hostname(self):
        return self._hostname

    @property
    def identifier(self):
        return self._identifier

    @property
    def workloads(self):
        return self._workloads

    @workloads.setter
    def workloads(self, new_workload: Workload):
        self._workloads.append(new_workload)

    @property
    def num_workloads(self):
        return self._num_workloads

    @property
    def num_of_fg_wls(self):
        return self._num_of_fg_wls

    @property
    def num_of_bg_wls(self):
        return self._num_of_bg_wls

    #@property
    #def aggr_contention(self):
    #    return self._aggr_contention

    @num_workloads.setter
    def num_workloads(self, num_wls: int):
        self._num_workloads = num_wls

    @num_of_fg_wls.setter
    def num_of_fg_wls(self, num_fg_wls: int):
        self._num_of_fg_wls = num_fg_wls

    @num_of_bg_wls.setter
    def num_of_bg_wls(self, num_bg_wls: int):
        self._num_of_bg_wls = num_bg_wls

    #@aggr_contention.setter
    #def aggr_contention(self, aggr_cont: float):
    #    self._aggr_contention = aggr_cont

    @node_type.setter
    def node_type(self, node_type: str):
        self._node_type = node_type

    @property
    def aggr_contention(self) -> BasicMetric:
        return self._aggr_metric

    def update_node_aggr_metric(self) -> None:
        aggr_metric = BasicMetric(0, 0, 0, 0, 0, 0, 0, 0, 200) # init to zero
        # Calculating avg_metric
        for wl in self.workloads:
            avg_metric = BasicMetric.calc_avg(wl.metrics, len(wl.metrics))
            wl.avg_metric = avg_metric

        # Aggregate all workloads' avg_metrics (window size == len of wl.metrics)
        for wl in self.workloads:
            aggr_metric._llc_references += wl.avg_metric.llc_references
            aggr_metric._llc_misses += wl.avg_metric.llc_misses
            aggr_metric._instructions += wl.avg_metric.instruction
            aggr_metric._cycles += wl.avg_metric.cycle
            aggr_metric._gpu_core_util += wl.avg_metric.gpu_core_util
            aggr_metric._gpu_core_freq += wl.avg_metric.gpu_core_freq
            aggr_metric._gpu_emc_util += wl.avg_metric.gpu_emc_util
            aggr_metric._gpu_emc_freq += wl.avg_metric.gpu_emc_freq
            aggr_metric._interval += wl.avg_metric.interval

        """
        # llc_hit_ratio will be used to estimate LLC Contention
        # llc_misses is used to estimate MEM BW Contention
        # gpu_core_util is used to estimate GPU Load
        """
        self._aggr_metric = aggr_metric # update aggr_metric
