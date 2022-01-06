import os
from typing import List

from prefect import Client, Flow, Task
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import UniversalRun
from prefect.storage import Local

PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', 'etude-Prefect')

class AbstractFlow:
    def __init__(self, client: Client, flow_name: str = "no_named_flow", e_tasks: List[Task] = [], t_tasks: List[Task] = [], l_tasks: List[Task] = []) -> None:
        self.client = client
        self.flow = Flow(name=flow_name, storage=Local(add_default_labels=False), executor=LocalDaskExecutor())
        self.e_tasks = e_tasks
        self.t_tasks = t_tasks
        self.l_tasks = l_tasks

    def run(self):
        self.extract()
        self.transform()
        self.load()

        self.flow.run_config = UniversalRun()
        flow_id = self.flow.register(project_name=PROJECT_NAME)

        self.client.create_flow_run(flow_id=flow_id)

    def extract(self):
        for e_task in self.e_tasks:
            self.flow.add_task(e_task)

    def transform(self):
        for t_task in self.t_tasks:
            self.flow.add_task(t_task)

    def load(self):
        for l_task in self.l_tasks:
            self.flow.add_task(l_task)

class SayHelloTask(Task):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.module_name = 'say_hello_task'

    def run(self):
        self.logger.info('Hello World!')

# Setup client
client = Client()
client.create_project(project_name=PROJECT_NAME)

# Setup flow
basicFlow = AbstractFlow(client=client, flow_name="basicFlow", e_tasks=[SayHelloTask()], t_tasks=[], l_tasks=[])

# Run flow
basicFlow.run()
