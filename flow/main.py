import os
from abc import abstractmethod
from datetime import datetime

import prefect
from prefect import Client, Flow, Task, Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import UniversalRun
from prefect.storage import Local
from prefect.tasks.notifications.email_task import EmailTask

PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', 'etude-Prefect')

class AbstractFlow:
    def __init__(self, flow_name: str = "no_named_flow", parameters: tuple[Task] = [], e_tasks: tuple[Task] = [], t_tasks: tuple[Task] = [], l_tasks: tuple[Task] = []) -> None:
        self.flow = Flow(
            name=flow_name,
            run_config=UniversalRun(),
            storage=Local(add_default_labels=False),
            executor=LocalDaskExecutor())

        for parameter in parameters:
            self.flow.add_task(parameter)

        self.e_tasks = e_tasks
        self.t_tasks = t_tasks
        self.l_tasks = l_tasks

    def register(self):
        master_csv_paths, products = self.extract()
        master_data, resource_data = self.transform(master_csv_paths=master_csv_paths, products=products)
        self.load(master_data=master_data, resource_data=resource_data)

        return self.flow.register(project_name=PROJECT_NAME)

    def run(self, flow_id: str, parameters: dict = {}):
        Client().create_flow_run(flow_id=flow_id, parameters=parameters)

    @abstractmethod
    def extract(self) -> tuple[tuple[Task], tuple[Task]]:
        raise NotImplementedError

    @abstractmethod
    def transform(self, master_csv_paths: tuple[Task], products: tuple[Task] = None) -> tuple[tuple[Task], tuple[Task]]:
        raise NotImplementedError

    @abstractmethod
    def load(self, master_data: tuple[Task], resource_data: tuple[Task] = None):
        raise NotImplementedError

class IdetailFlow(AbstractFlow):
    def extract(self) -> tuple[tuple[Task], tuple[Task]]:
        master_csv_paths = ""
        products = self.e_tasks[0]
        return products

    def transform(self, master_csv_paths: tuple[Task], products: tuple[Task] = None) -> tuple[tuple[Task], tuple[Task]]:
        pass

    def load(self, master_data: tuple[Task], resource_data: tuple[Task] = None):
        pass

class SayHelloTask(Task):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.module_name = 'say_hello_task'


    def run(self, from_date: datetime = None, **kwargs):
        flow_params = prefect.context.get("parameters", {})

        self.logger.info(f'Hello World! {flow_params=} {type(from_date)=} {from_date=}')

# Setup prefect cloud client and create project
Client().create_project(project_name=PROJECT_NAME)

# Setup parameters
message_parameter = Parameter('msg', default='this is parameter')
datetime_parameter = prefect.core.parameter.DateTimeParameter('from_date', required=False)

# Setup tasks
email_task = EmailTask(
    subject='Prefect Notification - Flow finished',
    msg='This message is sent with AWS SES SMTP.',
    smtp_server='email-smtp.ap-northeast-1.amazonaws.com',
    email_from='<Email address needs domain that it was verified identities in Amazon SES>',
    email_to='')

# Setup flow
basicFlow = AbstractFlow(
    flow_name="basicFlow",
    parameters=[message_parameter, datetime_parameter],
    e_tasks=[SayHelloTask()],
    t_tasks=[],
    l_tasks=[])

# Register flow
flow_id = basicFlow.register()

# Run flow
basicFlow.run(flow_id=flow_id, parameters={'msg': "Run registered flow.", 'from_date': "2022-02-17T20:13:00+09:00"})
