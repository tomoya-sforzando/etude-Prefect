import os

import prefect
from prefect import task, Flow, Client
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import UniversalRun
from prefect.storage import Local

PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', 'etude-Prefect')


@task
def hello_task():
    logger = prefect.context.get('logger')
    logger.info('Hello World!')


with Flow('hello-flow', storage=Local(add_default_labels=False), executor=LocalDaskExecutor()) as flow:
    hello_task()

client = Client()
client.create_project(project_name=PROJECT_NAME)

flow.run_config = UniversalRun()
flow_id = flow.register(project_name=PROJECT_NAME)

client.create_flow_run(flow_id=flow_id)
