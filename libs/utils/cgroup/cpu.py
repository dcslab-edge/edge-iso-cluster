# coding: UTF-8


import subprocess
from typing import ClassVar, Set

from .base import BaseCgroup
from ..hyphen import convert_to_set


class Cpu(BaseCgroup):
    CONTROLLER: ClassVar[str] = 'cpu'
    MAX_PERCENT = 100
    STEP = 10
    MIN_PERCENT = 10

    @staticmethod
    def limit_cpu_quota(group_name, quota: int, period: int) -> None:
        subprocess.check_call(args=('cgset', '-r', f'cpu.cfs_quota_us={quota}', group_name))
        subprocess.check_call(args=('cgset', '-r', f'cpu.cfs_period_us={period}', group_name))

    @staticmethod
    def read_cpus(group_name):
        cpus = subprocess.check_output(args=('cgget', '-nvr', 'cpuset.cpus', group_name), encoding='ASCII')
        if cpus is '':
            raise ProcessLookupError()
        return convert_to_set(cpus)

    @staticmethod
    def get_cfs_period_us(group_name):
        period = subprocess.check_output(args=('cgget', '-nvr', 'cpu.cfs_period_us', group_name))
        return period

    @staticmethod
    def limit_cycle_percentage(group_name, limit_percentage, period=None):
        cores: Set[int] = Cpu.read_cpus(group_name)
        if period is None:
            period = Cpu.get_cfs_period_us(group_name)
        quota = int(period * limit_percentage / 100 * len(cores))
        Cpu.limit_cpu_quota(quota, period)
