import os

from prefect import Client, Flow
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import UniversalRun
from prefect.storage import Local

PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', 'etude-Prefect')

class AbstractFlow:
    flow_name = None

    def __init__(self, parameters = [], e_tasks = [], t_tasks = [], l_tasks = []) -> None:
        self.flow = Flow(
            name=self.flow_name,
            run_config=UniversalRun(),
            storage=Local(add_default_labels=False),
            executor=LocalDaskExecutor())

        for parameter in parameters:
            self.flow.add_task(parameter)

        self.e_tasks = e_tasks
        self.t_tasks = t_tasks
        self.l_tasks = l_tasks

    def register(self):
        self.extract()
        self.transform()
        self.load()

        return self.flow.register(project_name=PROJECT_NAME)

    def run(self, flow_id: str, parameters: dict = {}):
        Client().create_flow_run(flow_id=flow_id, parameters=parameters)

    def extract(self):
        raise NotImplementedError

    def transform(self):
        raise NotImplementedError

    def load(self):
        raise NotImplementedError
