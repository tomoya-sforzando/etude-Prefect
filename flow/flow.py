import os

import prefect
from prefect import task, Flow
from prefect.run_configs import UniversalRun
from prefect.storage import Local


@task
def hello_task():
    logger = prefect.context.get('logger')
    logger.info('Hello World!')


with Flow('hello-flow', storage=Local(add_default_labels=False)) as flow:
    hello_task()

flow.run_config = UniversalRun()
flow.register(project_name=os.getenv('PREFECT_PROJECT_NAME', 'etude-Prefect'))
flow.run()
