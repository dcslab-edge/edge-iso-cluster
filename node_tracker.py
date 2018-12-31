# coding: UTF-8

import sys
import functools
import json
import logging
from threading import Thread
from multiprocessing import Process

from typing import Dict, Any
from pathlib import Path

import pika
from pika import BasicProperties
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic

from libs.metric_container.basic_metric import BasicMetric
from libs.node import Node
from libs.workload import Workload
from libs.utils.machine_type import NodeType


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

        self._node_tracker_cfg_file = Path.cwd() / 'node_tracker_config.json'
        self._cpu_nodes = None
        self._gpu_nodes = None

        # FIXME: hard coded
        self._rmq_host = 'jetson2'
        self._rmq_creation_exchanges: Dict[str, str] = dict()
        #self._rmq_tracking_node_queue = 'node_tracking'            # edge-profiler should use this queue

        self._cluster_nodes: Dict[str, Node] = dict()
        self._node_contentions: Dict[Node, BasicMetric] = None
        self._min_aggr_cont_node: Node = None

    @property
    def cluster_nodes(self) -> Dict[str, Node]:
        return self._cluster_nodes

    @property
    def node_contentions(self):
        return self._node_contentions

    @property
    def min_aggr_cont_node(self):
        return self._min_aggr_cont_node

    def setup_cluster_nodes(self) -> None:
        """
        This function reads `node_tracker_config.json` file and initialize the cluster node information
        such as `node ip` and `node type`
        :return:
        """
        # FIXME: hard coded
        # jetson1, jetson2, sdc1, sdc2
        # node_ipaddrs = ['147.46.242.201', '147.46.240.168', '147.46.242.219', '147.46.242.206']
        # node_ports = ['10010']*4
        # gpu_nodes = ['147.46.242.201', '147.46.240.168']
        # cpu_nodes = ['147.46.242.219', '147.46.242.206']

        if not self._node_tracker_cfg_file.exists():
                print(f'{self._node_tracker_cfg_file.resolve()} is not exist.', file=sys.stderr)
                return

        with self._node_tracker_cfg_file.open() as node_config_fp:
            node_cfg_source: Dict[str, Any] = json.load(node_config_fp)
            node_cfgs = node_cfg_source['node']
            for node_idx, node_cfg in enumerate(node_cfgs):
                node_ipaddr = node_cfg['ip']                    # ip addr of node to communicate with
                node_port = node_cfg['port']                    # port should be pre-defined
                node_name = node_cfg['hostname']
                node_type = node_cfg['type']
                print (f'{node_ipaddr},{node_port},{node_name},{node_type}')
                if node_type == 'gpu' or node_type == 'GPU':
                    self._gpu_nodes.append(node_ipaddr)
                elif node_type == 'cpu' or node_type == 'CPU':
                    self._cpu_nodes.append(node_ipaddr)
                self._cluster_nodes[node_ipaddr] = Node(node_ipaddr, node_port, node_type, node_name)

    def update_node_contention(self) -> None:
        for node in self._cluster_nodes.values():
            self._node_contentions[node]: BasicMetric = node.aggr_contention

    def find_min_aggr_cont_node(self) -> None:
        """
        Find Lowest LLC Memory BW Contention Node
        :return:
        """
        self.update_node_contention()
        min_membw_cont_node = None
        min_membw_cont = None

        if self._node_contentions is not None:
            for node, node_cont in self._node_contentions.items():
                if node.ip_addr in self._cpu_nodes:
                    if min_membw_cont_node is None:
                        min_membw_cont_node = node
                        min_membw_cont = node_cont
                    elif min_membw_cont_node is not None:
                        if min_membw_cont.llc_misses > node_cont.llc_misses:
                            min_membw_cont_node = node

            self._min_aggr_cont_node = min_membw_cont_node

    # Tracking nodes related ...

    def _cbk_connecting_nodes(self, ip_addr: str,
                              ch: BlockingChannel, method: Basic.Deliver, _: BasicProperties, body: bytes) \
            -> None:
        ch.basic_ack(method.delivery_tag)

        arr = body.decode().strip().split(',')

        logger = logging.getLogger('monitoring.tracking_nodes')
        logger.debug(f'{arr} is received from tracking_node queue')
        if len(arr) != 8:
            return

        wl_identifier, wl_type, pid, perf_pid, perf_interval, tegra_pid, tegra_interval, max_workloads = arr
        pid = int(pid)
        perf_pid = int(perf_pid)
        perf_interval = int(perf_interval)
        item = wl_identifier.split('_')
        wl_name = item[0]
        max_wls = int(max_workloads)

        workload = Workload(wl_name, wl_type, pid, perf_pid, perf_interval)
        # Adding Workload to Node
        node = self._cluster_nodes[ip_addr]
        node.workloads = workload
        node.num_workloads += 1
        if wl_type == 'bg':
            logger.info(f'{workload} is background process')
            self._cluster_nodes[ip_addr].num_of_bg_wls += 1
        else:
            logger.info(f'{workload} is foreground process')
            self._cluster_nodes[ip_addr].num_of_fg_wls += 1

        node_name = self._cluster_nodes[ip_addr].hostname
        wl_queue_name = 'rmq-{}-{}({})'.format(node_name, wl_name, pid)
        rmq_bench_exchange = f'ex-{node_name}-{wl_name}({pid})'
        ch.exchange_declare(exchange=rmq_bench_exchange, exchange_type='fanout')
        ch.queue_bind(exchange=rmq_bench_exchange, queue=wl_queue_name)
        print('[node_tracker] _cbk_connecting_nodes')
        ch.basic_consume(functools.partial(self._cbk_node_monitor, node, workload), wl_queue_name)

    def _cbk_node_monitor(self, node: Node, workload: Workload,
                          ch: BlockingChannel, method: Basic.Deliver, _: BasicProperties, body: bytes) -> None:
        metric = json.loads(body.decode())      # Through json format, node monitor can get aggr contention info
        ch.basic_ack(method.delivery_tag)
        print('[node_tracker] _cbk_node_monitor')
        # FIXME: Hard coded (200ms as interval)
        if node.node_type == NodeType.IntegratedGPU:
            item = BasicMetric(metric['llc_references'],
                               metric['llc_misses'],
                               metric['instructions'],
                               metric['cycles'],
                               metric['gpu_core_util'],
                               metric['gpu_core_freq'],
                               metric['gpu_emc_util'],
                               metric['gpu_emc_freq'],
                               200)
        if node.node_type == NodeType.IntegratedGPU:
            item = BasicMetric(metric['llc_references'],
                               metric['llc_misses'],
                               metric['instructions'],
                               metric['cycles'],
                               0,
                               0,
                               0,
                               0,
                               200)

        logger = logging.getLogger(f'monitoring.metric.{node}')
        logger.debug(f'{metric} is given from ')

        metric_que = workload.metrics

        if len(metric_que) == self._metric_buf_size:
            metric_que.pop()

        metric_que.appendleft(item)

    def connecting_and_consume(self, ip_addr: str):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self._rmq_host))
        channel = connection.channel()

        channel.exchange_declare(exchange=self._rmq_creation_exchanges[ip_addr], exchange_type='fanout')
        result = channel.queue_declare(exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange=self._rmq_creation_exchanges[ip_addr], queue=queue_name)
        channel.basic_consume(functools.partial(self._cbk_connecting_nodes, ip_addr), queue_name)

        try:
            logger = logging.getLogger('tracking')
            logger.info('starting node tracker thread')
            channel.start_consuming()

        except KeyboardInterrupt:
            channel.close()
            connection.close()

    def run(self) -> None:
        node_ips = self._cluster_nodes.keys()
        join_list = list()
        for node_ipaddr in node_ips:
            node = self._cluster_nodes[node_ipaddr]
            self._rmq_creation_exchanges[node_ipaddr] = f'workload_creation({node.hostname})'
            connect_p = Process(target=self.connecting_and_consume(ip_addr=node_ipaddr,))
            join_list.append(connect_p)
            connect_p.start()

        for connect_p in join_list:
            connect_p.join()

        """
        # channel.exchange_declare(exchange=self._rmq_creation_exchange[node_ipaddr], exchange_type='fanout')
        # result = channel.queue_declare(queue_name=, exclusive=True)
        # queue_name = result.method.queue
        #
        # channel.queue_bind(exchange=self._rmq_creation_exchange[node_ipaddr], queue=queue_name)
        # channel.basic_consume(functools.partial(self._cbk_connecting_nodes, node.ip_addr), queue_name)
        
        try:
            logger = logging.getLogger('tracking')
            logger.info('starting node tracker thread')
            channel.start_consuming()

        except KeyboardInterrupt:
            channel.close()
            connection.close()
        """
