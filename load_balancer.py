import collections
import random
import typing
from typing import Any, Dict, List, Optional

import replica as replica_lib
import traffic as traffic_lib
import utils

if typing.TYPE_CHECKING:
    import clock as clock_lib

# Define a registry for load balancing policies
LB_POLICIES = {}
DEFAULT_LB_POLICY = None


class LoadBalancer:
    """Abstract class for load balancing policies."""

    _default_lb_policy = None

    def __init__(self) -> None:
        self.clock: Optional["clock_lib.Clock"] = None
        self.replicas: List[replica_lib.Replica] = []

    def __init_subclass__(cls, name: str, default: bool = False):
        LB_POLICIES[name] = cls
        if default:
            assert cls._default_lb_policy is None, "Only one policy can be default."
            cls._default_lb_policy = name

    @classmethod
    def make(cls, policy_name: Optional[str] = None) -> "LoadBalancer":
        """Create a load balancing policy from a name."""
        if policy_name is None:
            policy_name = cls._default_lb_policy

        if policy_name not in LB_POLICIES:
            raise ValueError(f"Unknown load balancing policy: {policy_name}")
        return LB_POLICIES[policy_name]()

    def register_clock(self, clock: "clock_lib.Clock") -> None:
        self.clock = clock

    @property
    def tick(self) -> int:
        assert self.clock is not None, "Clock is not registered"
        return self.clock.tick

    def register(self, replica: replica_lib.Replica) -> None:
        """Register a replica."""
        self.replicas.append(replica)

    def step(
        self, traffic: List[traffic_lib.Traffic]
    ) -> Dict[replica_lib.Replica, List[traffic_lib.Traffic]]:
        """Step the load balancer. This should assign traffic to replicas."""
        raise NotImplementedError

    def meta_info(self) -> Dict[str, Any]:
        return {
            "name": self.__class__.__name__,
            "num_replicas": len(self.replicas),
            "replicas": [replica.meta_info() for replica in self.replicas],
        }

    def info(self) -> Dict[str, Any]:
        """Return the information of this load balancer."""
        return {
            "replicas": [replica.info() for replica in self.replicas],
        }


class RoundRobinLoadBalancer(LoadBalancer, name="round_robin", default=True):
    """Round-robin load balancing policy."""

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


class LeastLoadLoadBalancer(LoadBalancer, name="least_load"):
    """Least-load load balancing policy."""

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
