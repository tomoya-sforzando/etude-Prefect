import os
from datetime import datetime
from typing import List

import prefect
from prefect import Client, Flow, Task, Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import UniversalRun
from prefect.storage import Local
from prefect.tasks.notifications.email_task import EmailTask

PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', 'etude-Prefect')

class AbstractFlow:
    def __init__(self, client: Client, flow_name: str = "no_named_flow", e_tasks: List[Task] = [], t_tasks: List[Task] = [], l_tasks: List[Task] = []) -> None:
        self.client = client
        self.flow_name = flow_name
        self.flow = Flow(name=flow_name, storage=Local(add_default_labels=False), executor=LocalDaskExecutor())

        self.param = Parameter('msg', default='this is parameter')
        self.datetime_param = prefect.core.parameter.DateTimeParameter('from_date', required=False)
        self.flow.add_task(self.param)
        self.flow.add_task(self.datetime_param)

        self.e_tasks = e_tasks
        self.t_tasks = t_tasks
        self.l_tasks = l_tasks

    def run(self):
        self.extract()
        self.transform()
        self.load()

        self.flow.run_config = UniversalRun()
        flow_id = self.flow.register(project_name=PROJECT_NAME)

        self.client.create_flow_run(flow_id=flow_id) #, parameters={'msg': "run from AbstractFlow", 'from_date': "2022-02-17T20:13:00+09:00"})

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


    def run(self, from_date: datetime = None, **kwargs):
        flow_params = prefect.context.get("parameters", {})

        self.logger.info(f'Hello World! {flow_params=} {type(from_date)=} {from_date=}')

# Setup client
client = Client()
client.create_project(project_name=PROJECT_NAME)

# Setup tasks
email_task = EmailTask(
    subject='Prefect Notification - Flow finished',
    msg='This message is sent with AWS SES SMTP.',
    smtp_server='email-smtp.ap-northeast-1.amazonaws.com',
    email_from='<Email address needs domain that it was verified identities in Amazon SES>',
    email_to='')

# Setup flow
basicFlow = AbstractFlow(client=client, flow_name="basicFlow", e_tasks=[SayHelloTask()], t_tasks=[], l_tasks=[email_task])


# Run flow
basicFlow.run()
