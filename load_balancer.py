import collections
import random
import typing
from typing import Any, DefaultDict, Dict, List, Optional

import replica as replica_lib
import traffic as traffic_lib
import utils

if typing.TYPE_CHECKING:
    import clock as clock_lib


class LoadBalancer:
    def __init__(self) -> None:
        self.clock: Optional["clock_lib.Clock"] = None
        self.replicas: List[replica_lib.Replica] = []

    def register_clock(self, clock: "clock_lib.Clock") -> None:
        self.clock = clock

    @property
    def tick(self) -> int:
        assert self.clock is not None, "Clock is not registered"
        return self.clock.tick

    def register(self, replica: replica_lib.Replica) -> None:
        """Register a replica.

        Args:
            replica: A replica.
        """
        self.replicas.append(replica)

    def step(
        self, traffic: List[traffic_lib.Traffic]
    ) -> Dict[replica_lib.Replica, List[traffic_lib.Traffic]]:
        """Step the load balancer. This should assign traffic to replicas.

        Args:
            traffic: A list of traffic objects.

        Returns:
            A dict of replica objects to the assigned traffic.
            **This should include all registered replicas.**
        """
        raise NotImplementedError

    def meta_info(self) -> Dict[str, Any]:
        return {
            "name": self.__class__.__name__,
            "num_replicas": len(self.replicas),
            "replicas": [replica.meta_info() for replica in self.replicas],
        }

    def info(self) -> Dict[str, Any]:
        """Return the information of this load balancer.

        Returns:
            A dict of information.
        """
        return {
            "replicas": [replica.info() for replica in self.replicas],
        }


class RoundRobinLoadBalancer(LoadBalancer):
    def __init__(self) -> None:
        super().__init__()
        self.idx = 0

    def step(
        self, traffic: List[traffic_lib.Traffic]
    ) -> Dict[replica_lib.Replica, List[traffic_lib.Traffic]]:
        replica2traffics: Dict[replica_lib.Replica, List[traffic_lib.Traffic]] = {
            replica: [] for replica in self.replicas
        }
        for t in traffic:
            target_replica = self.replicas[self.idx]
            self.idx = (self.idx + 1) % len(self.replicas)
            replica2traffics[target_replica].append(t)
        return replica2traffics

    def info(self) -> Dict[str, Any]:
        return {
            **super().info(),
            "idx": self.idx,
        }

    def __repr__(self) -> str:
        return "RoundRobinLoadBalancer"


class LeastLoadLoadBalancer(LoadBalancer):
    def step(
        self, traffic: List[traffic_lib.Traffic]
    ) -> Dict[replica_lib.Replica, List[traffic_lib.Traffic]]:
        replica2traffics: Dict[replica_lib.Replica, List[traffic_lib.Traffic]] = {
            replica: [] for replica in self.replicas
        }
        def _get_replica_queue_length(replica: replica_lib.Replica) -> int:
            return len(replica.queue) + len(replica2traffics[replica])
        for t in traffic:
            sel = min(self.replicas, key=_get_replica_queue_length)
            replica2traffics[sel].append(t)
        return replica2traffics

    def __repr__(self) -> str:
        return "LeastLoadLoadBalancer"
