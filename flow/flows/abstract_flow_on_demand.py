from dataclasses import dataclass
import os
from typing import List

from prefect import Client, Flow, Task
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import UniversalRun
from prefect.storage import Local

# from flows.abstract_demands import AbstractDemands
from flows.abstract_tasks import AbstractTasks
# from flows.idetail_tasks import IdetailTasks

PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', 'etude-Prefect')

class AbstractFlowOnDemand:
    def __init__(self, flow_name, tasks) -> None:
        self.flow = Flow(
            name=flow_name,
            run_config=UniversalRun(),
            storage=Local(add_default_labels=False),
            executor=LocalDaskExecutor())

        self.basic_flow = self.flow.copy()
        self.tasks = tasks

    def build(self, demands: list):
        self.build_basic_flow()
        self.build_flow_on_demand(demands)

        if not demands:
            self.flow = self.basic_flow

    def build_basic_flow(self):
        raise NotImplementedError

    def build_flow_on_demand(self, demands: list):
        tasks_on_demand = [
            self.tasks.get_by_solution(demand) for demand in demands]

        def get_dependent_tasks(task_on_demand: Task):
            dependent_tasks = set()
            upstream_tasks = self.basic_flow.upstream_tasks(task_on_demand)
            while len(upstream_tasks):
                for task in upstream_tasks:
                    dependent_tasks.add(task)
                    upstream_tasks = upstream_tasks | self.basic_flow.upstream_tasks(task)
                    upstream_tasks.remove(task)
                upstream_tasks = upstream_tasks - dependent_tasks
            dependent_tasks.add(task_on_demand)
            return dependent_tasks

        def get_all_dependent_tasks(tasks_on_demand: List[Task]):
            all_dependent_tasks = set()
            for task in tasks_on_demand:
                all_dependent_tasks = all_dependent_tasks | get_dependent_tasks(task)
            return all_dependent_tasks

        for dependent_task in get_all_dependent_tasks(tasks_on_demand):
            base_edges = self.basic_flow.edges_to(dependent_task)
            for edge in base_edges:
                self.flow.add_edge(
                    upstream_task=edge.upstream_task,
                    downstream_task=edge.downstream_task,
                    key=edge.key,
                    mapped=edge.mapped,
                    flattened=edge.flattened
                )

    def register(self):
        return self.flow.register(project_name=PROJECT_NAME)

    def run(self, flow_id: str, parameters: dict = {}):
        Client().create_flow_run(flow_id=flow_id, parameters=parameters)
