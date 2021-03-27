#!/usr/bin/env python3

import prefect
import yaml
from prefect import Flow, task
from prefect.run_configs import ECSRun
from prefect.storage import Docker

STORAGE = Docker(
    registry_url='910376070004.dkr.ecr.ap-northeast-1.amazonaws.com',
    image_name='msd-nector-prefect-agent',
    image_tag='latest',
    dockerfile='./Dockerfile'
)

# DEFINITION = yaml.safe_load(
#     """
# networkMode: awsvpc
# cpu: 1024
# memory: 2048
# requiresCompatibilities:
#   - FARGATE
#     """
# )

RUN_CONFIG = ECSRun(
    run_task_kwargs={
        'cluster': 'msd-nector-development-prefect-cluster'
    },
    # task_definition=DEFINITION,
    # image='msd-nector-prefect-agent',
    execution_role_arn='arn:aws:iam::910376070004:role/msd-nector-ecs-fullaccess-role',
    # task_role_arn='arn:aws:iam::910376070004:role/msd-nector-ecs-fullaccess-role',
    labels=['ecs', 'dev']
)


@task
def say_hello():
    logger = prefect.context.get('logger')
    logger.info(f'Hello Prefect!')


with Flow('msd-nector-prefect-agent', storage=STORAGE, run_config=RUN_CONFIG) as flow:
    say_hello()

flow.register(project_name='nector-staging')
