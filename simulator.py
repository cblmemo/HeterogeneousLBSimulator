import copy
import json
import multiprocessing
import os
import random
import time
from typing import Any, Dict, List, Optional, Union

import tqdm  # type: ignore
from rich import print as rp

import client as client_lib
import clock as clock_lib
import load_balancer as lb_lib
import replica as replica_lib
import traffic as traffic_lib
import utils

SMOOTH_WINDOW_SIZE = 500


def _simulate_one(
    clients: Union[client_lib.Client, List[client_lib.Client]],
    lb: lb_lib.LoadBalancer,
    replicas: List[replica_lib.Replica],
    output_path: str,
    max_tick: Optional[int] = None,
    step_tick: Optional[int] = None,
    interactive: bool = False,
    simulate_id: int = 0,
    lock: Optional[Any] = None,
    with_pbar: bool = True,
) -> None:
    if not output_path.endswith(".jsonl"):
        raise ValueError("output_path should end with .jsonl")
    if with_pbar:
        pbar = tqdm.tqdm(total=max_tick, position=simulate_id, desc=f"{str(lb):<50}")
    random.seed(time.time())
    clock = clock_lib.Clock()
    if not isinstance(clients, list):
        clients = [clients]
    for client in clients:
        client.register_clock(clock)
    lb.register_clock(clock)
    for replica in replicas:
        lb.register(replica)
        replica.register_clock(clock)
    with open(output_path, "w") as f:
        meta_info = {
            "type": "meta_info",
            "clients": [client.meta_info() for client in clients],
            "lb": lb.meta_info(),
        }
        f.write(json.dumps(meta_info) + "\n")
        while True:
            tick = clock.step()
            new_traffic: List[traffic_lib.Traffic] = []
            for client in clients:
                new_traffic.extend(client.observe())
            meta_data: Dict[str, Any] = {
                "type": "meta_info",
                "new_traffic": [t.meta_info() for t in new_traffic],
            }
            replica2traffics = lb.step(new_traffic)
            finished_traffics: List[traffic_lib.Traffic] = []
            for replica, traffics in replica2traffics.items():
                finished_traffic = replica.step(traffics)
                finished_traffics.extend(finished_traffic)
            success_traffics = [t for t in finished_traffics if not t.expired]
            failure_traffics = [t for t in finished_traffics if t.expired]
            info = {
                "type": "tick_info",
                "tick": tick,
                "finished_traffics": [(t.id, t.latency(), t.expired) for t in finished_traffics],  # type: ignore # pylint: disable=no-member
                "lb_info": lb.info(),
            }
            f.write(json.dumps(info) + "\n")
            f.write(json.dumps(meta_data) + "\n")
            if step_tick is not None and tick % step_tick == 0:
                if interactive:
                    # os.system("clear")
                    rp(info)
                    input()
                # rp(lb, f"{tick}/{max_tick}")
            if max_tick is not None and tick >= max_tick:
                break
            if with_pbar:
                if lock is not None:
                    with lock:
                        pbar.update(1)
                else:
                    pbar.update(1)
    if with_pbar:
        pbar.close()


def dummy_simulate():
    for lb in [lb_lib.RoundRobinLoadBalancer(), lb_lib.LeastLoadLoadBalancer()]:
        _simulate_one(
            [
                client_lib.RandomChoiceWorkloadClient(
                    location=utils.GeographicalRegion.US,
                    workload_candidates=list(range(1, 4)),
                    traffic_expired_time=100.0 / clock_lib.TICK_PERIOD_S,
                ) for _ in range(3)
            ],
            lb, # TODO: location for LB
            [
                replica_lib.AcceleratorReplica(
                    location=utils.GeographicalRegion.US,
                    accelerator=utils.AcceleratorType.A100,
                ),
            ] + [
                replica_lib.AcceleratorReplica(
                    location=utils.GeographicalRegion.ASIA,
                    accelerator=utils.AcceleratorType.T4,
                ) for _ in range(4)
            ],
            output_path="res/temp.jsonl",
            max_tick=3000,
            # step_tick=10,
            # interactive=True,
        )
        latencies = []
        failure = 0
        with open("res/temp.jsonl", "r") as f:
            for line in f.readlines():
                info = json.loads(line)
                if info["type"] == "tick_info":
                    for tid, lat, expired in info["finished_traffics"]:
                        if expired:
                            failure += 1
                        else:
                            latencies.append(lat)
                    # print(list((len(r["queue"]), r["accelerator"]) for r in info["lb_info"]["replicas"]))
        print(f"Failure rate: {failure / (len(latencies) + failure):.2f}")
        print(f"Average latency: {sum(latencies) / len(latencies):.2f}")
        print(f"95th percentile latency: {sorted(latencies)[int(len(latencies) * 0.95)]}")
        print(f"99th percentile latency: {sorted(latencies)[int(len(latencies) * 0.99)]}")
        print(f"99.9th percentile latency: {sorted(latencies)[int(len(latencies) * 0.999)]}")


if __name__ == "__main__":
    dummy_simulate()
