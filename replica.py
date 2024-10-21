from typing import Any, Dict, List, Optional

import clock as clock_lib
import traffic as traffic_lib
import utils

# Define a registry for replica types
REPLICA_TYPES = {}
DEFAULT_REPLICA_TYPE = None


@utils.add_unique_id
class Replica:
    _default_replica_type = None  # Add this line

    def __init__(self, location: utils.GeographicalRegion) -> None:
        self.location = location
        self.clock: Optional["clock_lib.Clock"] = None
        self.queue: List[traffic_lib.Traffic] = []

    def __init_subclass__(cls, name: str, default: bool = False):
        REPLICA_TYPES[name] = cls
        if default:
            assert (
                cls._default_replica_type is None
            ), "Only one replica type can be default."
            cls._default_replica_type = name

    @classmethod
    def make(cls, replica_type: Optional[str] = None, **kwargs) -> "Replica":
        """Create a replica from a type name."""
        if replica_type is None:
            replica_type = DEFAULT_REPLICA_TYPE

        if replica_type not in REPLICA_TYPES:
            raise ValueError(f"Unknown replica type: {replica_type}")
        return REPLICA_TYPES[replica_type](**kwargs)

    def register_clock(self, clock: "clock_lib.Clock") -> None:
        self.clock = clock

    @property
    def tick(self) -> int:
        assert self.clock is not None, "Clock is not registered"
        return self.clock.tick

    def step(self, traffics: List[traffic_lib.Traffic]) -> List[traffic_lib.Traffic]:
        """Step the replica.

        Args:
            traffics: A list of traffic objects that assigned to
                this replica.

        Returns:
            A list of traffic objects that finished in this tick.
        """
        self.queue.extend(traffics)
        return []

    def meta_info(self) -> Dict[str, Any]:
        return {
            "id": self.id,  # type: ignore # pylint: disable=no-member
            "name": self.__class__.__name__,
            "location": self.location.value,
        }

    def info(self) -> Dict[str, Any]:
        """Return the information of this replica.

        Returns:
            A dict of information.
        """
        return {
            "id": self.id,  # type: ignore # pylint: disable=no-member
            "queue_length": len(self.queue),
            "queue": [t.id for t in self.queue],  # type: ignore # pylint: disable=no-member
        }


class AcceleratorReplica(Replica, name="accelerator", default=True):
    def __init__(
        self,
        location: utils.GeographicalRegion,
        accelerator: utils.AcceleratorType,
    ) -> None:
        super().__init__(location)
        self.accelerator = accelerator

    def step(
        self,
        traffics: List[traffic_lib.Traffic],
    ) -> List[traffic_lib.Traffic]:
        super().step(traffics)
        # TODO: Continuous batching.
        tot_compute_used = 0
        for traffic in self.queue:
            compute_on_traffic = min(
                traffic.remaining_processing_time - tot_compute_used,
                self.accelerator.value,
            )
            traffic.remaining_processing_time -= compute_on_traffic
            tot_compute_used += compute_on_traffic
            if tot_compute_used == self.accelerator.value:
                break
        finished_traffics = []
        for traffic in self.queue:
            if traffic.remaining_processing_time == 0:
                self.queue.remove(traffic)
                finished_traffics.append(traffic.set_finish_time(self.tick))
        return finished_traffics

    def info(self) -> Dict[str, Any]:
        return {
            **super().info(),
            "accelerator": str(self.accelerator),
        }
