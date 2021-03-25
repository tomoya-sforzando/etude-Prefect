#!/usr/bin/env python3
import os

import prefect
from prefect import task, Flow, Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import LocalRun


@task
def say_hello(name):
    greeting = os.environ.get('GREETING')
    logger = prefect.context.get('logger')
    logger.info(f'{greeting}, {name}!')


with Flow('hello-flow') as flow:
    people = Parameter('people', default=['Alice', 'Bob'])
    say_hello.map(people)

flow.run_config = LocalRun(env={'GREETING': 'Good bye'})
flow.executor = LocalDaskExecutor()
flow.register(project_name='etude')
