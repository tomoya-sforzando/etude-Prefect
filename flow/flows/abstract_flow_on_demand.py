import os
from abc import ABC, ABCMeta, abstractmethod
from typing import List

from prefect import Client, Flow, Task

from tasks.abstract_meta_task import AbstractMetaTask

PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', 'etude-Prefect')

class AbstractFlowOnDemand(Flow, metaclass=ABCMeta):
    def __init__(self, name, meta_task=AbstractMetaTask(), *args, **kwargs):
        self.meta_task = meta_task
        self.basic_flow = Flow(name=name)
        super().__init__(name=name, *args, **kwargs)

    def build(self, tasks_on_demand: List[AbstractMetaTask]):
        self.build_basic_flow()
        self.build_flow_on_demand(tasks_on_demand)

        if not tasks_on_demand:
            self = self.basic_flow

    @abstractmethod
    def build_basic_flow(self):
        # Build task dependencies with Task class or Flow class
        # ref. https://docs.prefect.io/api/latest/core/task.html
        # ref. https://docs.prefect.io/api/latest/core/flow.html
        raise NotImplementedError

    def build_flow_on_demand(self, tasks_on_demand: List[AbstractMetaTask]):
        self.tasks = set() # Reset tasks
        tasks_on_demand = [
            self.meta_task.get_by_demand(task) for task in tasks_on_demand]

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
                self.add_edge(
                    upstream_task=edge.upstream_task,
                    downstream_task=edge.downstream_task,
                    key=edge.key,
                    mapped=edge.mapped,
                    flattened=edge.flattened
                )

    def register(self):
        return super().register(project_name=PROJECT_NAME)

    @staticmethod
    def run(flow_id: str, parameters: dict = {}):
        Client().create_flow_run(flow_id=flow_id, parameters=parameters)
