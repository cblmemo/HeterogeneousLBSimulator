import copy
import random
import typing
from typing import Any, Dict, List, Optional

import traffic as traffic_lib
import utils

if typing.TYPE_CHECKING:
    import clock as clock_lib

# Define a registry for client types
CLIENT_TYPES = {}
DEFAULT_CLIENT_TYPE = None


@utils.add_unique_id
class Client:
    _default_client_type = None

    def __init__(self, **kwargs) -> None:
        self.clock: Optional["clock_lib.Clock"] = None
        self.location = kwargs.pop("location", None)
        self.traffic_expired_time = kwargs.pop("traffic_expired_time", None)
        self.period_tick = kwargs.pop("period_tick", 1)

    def __init_subclass__(cls, name: str, default: bool = False):
        CLIENT_TYPES[name] = cls
        if default:
            assert (
                cls._default_client_type is None
            ), "Only one client type can be default."
            cls._default_client_type = name

    @classmethod
    def make(cls, client_type: Optional[str] = None, **kwargs) -> "Client":
        """Create a client from a type name."""
        if client_type is None:
            client_type = cls._default_client_type

        if client_type not in CLIENT_TYPES:
            raise ValueError(f"Unknown client type: {client_type}")
        return CLIENT_TYPES[client_type](**kwargs)

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
        """Return the information of this client.

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


class FixedTrafficClient(Client, name="fixed_traffic", default=True):
    def __init__(self, traffics: List[Optional[traffic_lib.Traffic]], **kwargs) -> None:
        super().__init__(**kwargs)
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


class RandomChoiceWorkloadClient(Client, name="random_choice"):
    def __init__(self, workload_candidates: List[int], **kwargs) -> None:
        super().__init__(**kwargs)
        self.workload_candidates = workload_candidates

    def _observe(self) -> List[traffic_lib.Traffic]:
        return [traffic_lib.Traffic(random.choice(self.workload_candidates))]

    def meta_info(self) -> Dict[str, Any]:
        return {
            **super().meta_info(),
            "workload_candidates": self.workload_candidates,
        }


class RandomSendRequestClient(Client, name="random_send_request"):
    def __init__(self, prob: float, workload: int, **kwargs) -> None:
        super().__init__(**kwargs)
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


class DayAndNightClient(Client, name="day_and_night"):
    def __init__(
        self,
        day_prob: float,
        night_prob: float,
        workload: int,
        day_tick: int,
        night_tick: int,
        num_req: int,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
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


class BurstyClient(Client, name='bursty'):
    def __init__(self, burst_size: int, burst_interval: int, **kwargs) -> None:
        super().__init__(**kwargs)
        self.burst_size = burst_size
        self.burst_interval = burst_interval

    def _observe(self) -> List[traffic_lib.Traffic]:
        if self.tick % self.burst_interval == 0:
            return [traffic_lib.Traffic(random.randint(1, 5)) for _ in range(self.burst_size)]
        return []

    def meta_info(self) -> Dict[str, Any]:
        return {
            **super().meta_info(),
            "burst_size": self.burst_size,
            "burst_interval": self.burst_interval,
        }

class PriorityClient(Client, name='priority'):
    def __init__(self, priority_levels: List[int], **kwargs) -> None:
        super().__init__(**kwargs)
        self.priority_levels = priority_levels

    def _observe(self) -> List[traffic_lib.Traffic]:
        priority = random.choice(self.priority_levels)
        return [traffic_lib.Traffic(priority)]

    def meta_info(self) -> Dict[str, Any]:
        return {
            **super().meta_info(),
            "priority_levels": self.priority_levels,
        }