# coding: UTF-8

import functools
import json
import logging
from threading import Thread

from typing import Dict

import pika
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic

from libs.metric_container.basic_metric import BasicMetric
from libs.workload import Workload
from libs.node import Node
from libs.utils.machine_type import MachineChecker, NodeType


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class NodeTracker(Thread, metaclass=Singleton):
    def __init__(self, metric_buf_size: int) -> None:
        super().__init__(daemon=True)
        self._metric_buf_size = metric_buf_size
        #self._node_type = MachineChecker.get_node_type()

        self._rmq_host = 'localhost'
        self._rmq_tracking_node_queue = 'tracking_nodes'    # edge-profiler should use this queue

        self._cluster_nodes: Dict[str, Node] = dict()
        self._node_contentions: Dict[Node, float] = None
        self._min_aggr_cont_node = None


    @property
    def cluster_nodes(self):
        return self._cluster_nodes

    @property
    def node_contentions(self):
        return self._node_contentions

    @property
    def min_aggr_cont_node(self):
        return self._min_aggr_cont_node

    def setup_cluster_nodes(self) -> None:
        # FIXME: hard coded
        # Jetson1, Jetson2, SDC1, SDC2
        node_ipaddrs = ['147.46.242.201', '147.46.242.243', '147.46.242.219', '147.46.242.206']
        node_ports = ['10010']*4
        gpu_nodes = ['147.46.242.201', '147.46.242.243']
        cpu_nodes = ['147.46.242.219', '147.46.242.206']
        node_type = None
        for idx, node_ipaddr in enumerate(node_ipaddrs):
            if node_ipaddr in gpu_nodes:
                node_type = 'gpu_node'
            elif node_ipaddr in cpu_nodes:
                node_type = 'cpu_node'

            self._cluster_nodes[node_ipaddr] = Node(node_ipaddr, node_ports[idx], node_type)

    def update_node_contention(self) -> None:
        for node in self._cluster_nodes.values():
            self._node_contentions[node] = node.aggr_contention()

    def find_min_aggr_cont_node(self) -> None:
        min_cont_node = None
        min_cont = None
        for node, node_cont in self._node_contentions.items():
            if min_cont_node is None:
                min_cont_node = node
                min_cont = node_cont
            elif min_cont_node is not None:
                if min_cont > node_cont:
                    min_cont_node = node

        self._min_aggr_cont_node = min_cont_node

    # Tracking nodes related ...

    def _cbk_connecting_nodes(self, ch: BlockingChannel, method: Basic.Deliver, _: BasicProperties, body: bytes) -> None:
        ch.basic_ack(method.delivery_tag)

        arr = body.decode().strip().split(',')

        logger = logging.getLogger('monitoring.tracking_nodes')
        logger.debug(f'{arr} is received from tracking_node queue')

        if len(arr) != 6:
            return

        ip_addr, aggr_contention, num_workloads, num_of_fg_wls, num_of_bg_wls, node_type = arr
        aggr_contention = float(aggr_contention)
        num_workloads = int(num_workloads)
        num_of_fg_wls = int(num_of_fg_wls)
        num_of_bg_wls = int(num_of_bg_wls)
        # node_type is either 'gpu' or 'cpu'

        tracked_node = self._cluster_nodes[ip_addr]
        tracked_node.aggr_contention = aggr_contention
        tracked_node.num_workloads = num_workloads
        tracked_node.num_of_fg_wls = num_of_fg_wls
        tracked_node.num_of_bg_wls = num_of_bg_wls
        tracked_node.node_type = node_type

        #self.monitor_workloads.add(workload, max_wls)

        node_queue_name = '{}_node_({})'.format(tracked_node.node_type, tracked_node.ip_addr)
        ch.queue_declare(node_queue_name)
        ch.basic_consume(functools.partial(self._cbk_node_monitor, tracked_node), node_queue_name)

    def _cbk_node_monitor(self, node: Node,
                        ch: BlockingChannel, method: Basic.Deliver, _: BasicProperties, body: bytes) -> None:
        metric = json.loads(body.decode())      # Through json format, node monitor can get aggr contention info
        ch.basic_ack(method.delivery_tag)

        item = BasicMetric(metric['llc_references'],
                           metric['llc_misses'],
                           metric['instructions'],
                           metric['cycles'],
                           metric['gpu_core_util'],
                           metric['gpu_core_freq'],
                           metric['gpu_emc_util'],
                           metric['gpu_emc_freq'],
                           200)

        logger = logging.getLogger(f'monitoring.metric.{node}')
        logger.debug(f'{metric} is given from ')

        metric_que = node.metrics

        if len(metric_que) == self._metric_buf_size:
            metric_que.pop()

        metric_que.appendleft(item)

    def run(self) -> None:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._rmq_host))
        channel = connection.channel()

        channel.queue_declare(self._rmq_tracking_node_queue)
        channel.basic_consume(self._cbk_connecting_nodes, self._rmq_tracking_node_queue)

        try:
            logger = logging.getLogger('monitoring')
            logger.info('starting consuming thread')
            channel.start_consuming()

        except KeyboardInterrupt:
            channel.close()
            connection.close()
