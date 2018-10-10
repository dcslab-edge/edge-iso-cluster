# coding: UTF-8

import logging
from typing import Optional

from .base_isolator import Isolator
from .. import NextStep
from ...metric_container.basic_metric import MetricDiff
from ...utils import DVFS
from ...workload import Workload


class MemoryIsolator(Isolator):
    _DOD_THRESHOLD = 0.005
    _FORCE_THRESHOLD = 0.1

    def __init__(self, foreground_wl: Workload, background_wl: Workload) -> None:
        super().__init__(foreground_wl, background_wl)

        # FIXME: hard coded
        self._cur_step: int = DVFS.MAX
        self._stored_config: Optional[int] = None

    def strengthen(self) -> 'MemoryIsolator':
        self._cur_step -= DVFS.STEP
        return self

    def weaken(self) -> 'MemoryIsolator':
        self._cur_step += DVFS.STEP
        return self

    @property
    def is_max_level(self) -> bool:
        # FIXME: hard coded
        return self._cur_step - DVFS.STEP < DVFS.MIN

    @property
    def is_min_level(self) -> bool:
        # FIXME: hard coded
        return DVFS.MAX <= self._cur_step + DVFS.STEP

    def _enforce(self) -> None:
        logger = logging.getLogger(__name__)
        logger.info(f'frequency of bound_cores {self._background_wl.bound_cores} is {self._cur_step / 1_000_000}GHz')

        DVFS.set_freq(self._cur_step, self._background_wl.bound_cores)

    def _first_decision(self, metric_diff: MetricDiff) -> NextStep:
        curr_diff = metric_diff.local_mem_util_ps

        logger = logging.getLogger(__name__)
        logger.debug(f'current diff: {curr_diff:>7.4f}')

        if curr_diff < 0:
            if self.is_max_level:
                return NextStep.STOP
            else:
                return NextStep.STRENGTHEN
        elif curr_diff <= MemoryIsolator._FORCE_THRESHOLD:
            return NextStep.STOP
        else:
            if self.is_min_level:
                return NextStep.STOP
            else:
                return NextStep.WEAKEN

    def _monitoring_result(self, prev_metric_diff: MetricDiff, cur_metric_diff: MetricDiff) -> NextStep:
        curr_diff = cur_metric_diff.local_mem_util_ps
        prev_diff = prev_metric_diff.local_mem_util_ps
        diff_of_diff = curr_diff - prev_diff

        logger = logging.getLogger(__name__)
        logger.debug(f'diff of diff is {diff_of_diff:>7.4f}')
        logger.debug(f'current diff: {curr_diff:>7.4f}, previous diff: {prev_diff:>7.4f}')

        if self.is_min_level or self.is_max_level \
                or abs(diff_of_diff) <= MemoryIsolator._DOD_THRESHOLD \
                or abs(curr_diff) <= MemoryIsolator._DOD_THRESHOLD:
            return NextStep.STOP

        elif curr_diff > 0:
            return NextStep.WEAKEN

        else:
            return NextStep.STRENGTHEN

    def reset(self) -> None:
        DVFS.set_freq(DVFS.MAX, self._background_wl.orig_bound_cores)

    def store_cur_config(self) -> None:
        self._stored_config = self._cur_step

    def load_cur_config(self) -> None:
        super().load_cur_config()

        self._cur_step = self._stored_config
        self._enforce()
        self._stored_config = None
