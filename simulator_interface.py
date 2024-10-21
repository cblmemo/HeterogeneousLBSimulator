from typing import Any, Dict, List

import yaml

import client as client_lib
import clock as clock_lib
import load_balancer as lb_lib
import replica as replica_lib
import utils
from simulator import _simulate_one


def load_yaml_config(file_path: str) -> Dict[str, Any]:
    with open(file_path, "r") as file:
        return yaml.safe_load(file)


def setup_simulation(simulation_config: Dict[str, Any]):
    # Extract simulation settings
    simulation_settings = simulation_config.get("simulation", {})
    clients_config = simulation_config.get("clients", [])
    lb_config = simulation_config.get("load_balancer", {})
    replicas_config = simulation_config.get("replicas", [])

    # Validate required simulation settings
    if not clients_config:
        raise ValueError("At least one client configuration is required.")
    if not replicas_config:
        raise ValueError("At least one replica configuration is required.")
    if not simulation_settings.get("output_path"):
        raise ValueError("Output path is required in simulation settings.")

    # Create clients
    clients = [
        client_lib.Client.make(
            client_type=client.get("type"),
            location=utils.GeographicalRegion[client.get("location")],
            traffics=client.get("traffics", []),
            workload_candidates=client.get("workload_candidates", []),
            prob=client.get("prob", 0.0),
            workload=client.get("workload", 0),
            day_prob=client.get("day_prob", 0.0),
            night_prob=client.get("night_prob", 0.0),
            day_tick=client.get("day_tick", 0),
            night_tick=client.get("night_tick", 0),
            num_req=client.get("num_req", 1),
            traffic_expired_time=client.get("traffic_expired_time", 0)
            / clock_lib.TICK_PERIOD_S,
            period_tick=client.get("period_tick", 1),
            burst_size=client.get("burst_size", 0),
            burst_interval=client.get("burst_interval", 1),
            priority_levels=client.get("priority_levels", [1]),
        )
        for client in clients_config
    ]

    # Create load balancer, default to None which will invoke the default policy
    lb_type = lb_config.get("type")
    lb = lb_lib.LoadBalancer.make(lb_type)

    # Create replicas
    replicas = [
        replica_lib.Replica.make(
            replica_type=replica.get("type"),
            location=utils.GeographicalRegion[replica.get("location")],
            accelerator=utils.AcceleratorType[replica.get("accelerator")],
        )
        for replica in replicas_config
    ]

    # Run simulation
    _simulate_one(
        clients=clients,
        lb=lb,
        replicas=replicas,
        output_path=simulation_settings["output_path"],
        max_tick=simulation_settings.get("max_tick", 1000),
        step_tick=simulation_settings.get("step_tick", 1),
        interactive=simulation_settings.get("interactive", False),
        with_pbar=simulation_settings.get("with_pbar", True),
    )


if __name__ == "__main__":
    config = load_yaml_config("simulator_config.yaml")
    setup_simulation(config)
