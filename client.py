import copy
import random
import typing
from typing import Any, Dict, List, Optional

import traffic as traffic_lib
import utils

if typing.TYPE_CHECKING:
    import clock as clock_lib


@utils.add_unique_id
class Client:
    def __init__(
        self,
        location: utils.GeographicalRegion,
        traffic_expired_time: Optional[int] = None,
        period_tick: Optional[int] = None,
    ) -> None:
        self.clock: Optional["clock_lib.Clock"] = None
        self.location = location
        self.traffic_expired_time = traffic_expired_time
        self.period_tick = period_tick or 1

    def register_clock(self, clock: "clock_lib.Clock") -> None:
        self.clock = clock

    @property
    def tick(self) -> int:
        assert self.clock is not None, "Clock is not registered"
        return self.clock.tick

    def observe(self) -> List[traffic_lib.Traffic]:
        if self.tick % self.period_tick == 0:
            return [
                t.set_start_time(self.tick)
                .set_expired_time(self.traffic_expired_time)
                .set_client_location(self.location)
                for t in self._observe()
            ]
        return []

    def _observe(self) -> List[traffic_lib.Traffic]:
        """Observe the current state of client.

        **NOTICE: REMEMBER TO SET start_time FOR TRAFFIC OBJECTS.**

        Returns:
            A list of traffic objects.
        """
        raise NotImplementedError

    def meta_info(self) -> Dict[str, Any]:
        """Return the information of this load balancer.

        Returns:
            A dict of information.
        """
        return {
            "id": self.id,  # type: ignore # pylint: disable=no-member
            "name": self.__class__.__name__,
            "location": self.location.value,
            "traffic_expired_time": self.traffic_expired_time,
            "period_tick": self.period_tick,
        }


class FixedTrafficClient(Client):
    def __init__(
        self,
        traffics: List[Optional[traffic_lib.Traffic]],
        location: utils.GeographicalRegion,
        traffic_expired_time: Optional[int] = None,
        period_tick: Optional[int] = None,
    ) -> None:
        super().__init__(location, traffic_expired_time, period_tick)
        self.traffics = traffics
        self.idx = 0

    def _observe(self) -> List[traffic_lib.Traffic]:
        cur = self.traffics[self.idx]
        self.idx = (self.idx + 1) % len(self.traffics)
        return [copy.deepcopy(cur)] if cur is not None else []

    def meta_info(self) -> Dict[str, Any]:
        return {
            **super().meta_info(),
            "traffics": [
                t.meta_info() if t is not None else None for t in self.traffics
            ],
        }


class RandomChoiceWorkloadClient(Client):
    def __init__(
        self,
        workload_candidates: List[int],
        location: utils.GeographicalRegion,
        traffic_expired_time: Optional[int] = None,
        period_tick: Optional[int] = None,
    ) -> None:
        super().__init__(location, traffic_expired_time, period_tick)
        self.workload_candidates = workload_candidates

    def _observe(self) -> List[traffic_lib.Traffic]:
        return [traffic_lib.Traffic(random.choice(self.workload_candidates))]

    def meta_info(self) -> Dict[str, Any]:
        return {
            **super().meta_info(),
            "workload_candidates": self.workload_candidates,
        }


class RandomSendRequestClient(Client):
    def __init__(
        self,
        prob: float,
        workload: int,
        location: utils.GeographicalRegion,
        traffic_expired_time: Optional[int] = None,
        period_tick: Optional[int] = None,
    ) -> None:
        super().__init__(location, traffic_expired_time, period_tick)
        self.prob = prob
        self.workload = workload

    def _observe(self) -> List[traffic_lib.Traffic]:
        if random.random() < self.prob:
            return [traffic_lib.Traffic(self.workload)]
        return []

    def meta_info(self) -> Dict[str, Any]:
        return {
            **super().meta_info(),
            "prob": self.prob,
            "workload": self.workload,
        }


class DayAndNightClient(Client):
    def __init__(
        self,
        day_prob: float,
        night_prob: float,
        workload: int,
        day_tick: int,
        night_tick: int,
        num_req: int,
        location: utils.GeographicalRegion,
        traffic_expired_time: Optional[int] = None,
        period_tick: Optional[int] = None,
    ) -> None:
        super().__init__(location, traffic_expired_time, period_tick)
        self.day_prob = day_prob
        self.night_prob = night_prob
        self.workload = workload
        self.day_tick = day_tick
        self.night_tick = night_tick
        self.num_req = num_req

    def prob(self) -> float:
        idx = self.tick % (self.day_tick + self.night_tick)
        if idx < self.day_tick:
            return self.day_prob
        return self.night_prob

    def _observe(self) -> List[traffic_lib.Traffic]:
        if random.random() < self.prob():
            return [traffic_lib.Traffic(self.workload) for _ in range(self.num_req)]
        return []

    def meta_info(self) -> Dict[str, Any]:
        return {
            **super().meta_info(),
        }
