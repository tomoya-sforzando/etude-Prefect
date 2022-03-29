import os
# from abc import abstractmethod
from datetime import datetime
from pathlib import Path
from typing import List

import prefect
from prefect import Client, Flow, Task, Parameter, flatten, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import UniversalRun
from prefect.storage import Local
from prefect.tasks.notifications.email_task import EmailTask

PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', 'etude-Prefect')

# Flow classes
class AbstractFlow:
    def __init__(self, flow_name: str = "no_named_flow", parameters = [], e_tasks = [], t_tasks = [], l_tasks = []) -> None:
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
        self.load(master_data=master_data, resource_data=resource_data, products=products)

        return self.flow.register(project_name=PROJECT_NAME)

    def run(self, flow_id: str, parameters: dict = {}):
        Client().create_flow_run(flow_id=flow_id, parameters=parameters)

    def extract(self):
        raise NotImplementedError

    def transform(self, master_csv_paths, products = None):
        raise NotImplementedError

    def load(self, master_data, resource_data = None, products = None):
        raise NotImplementedError

class IdetailFlow(AbstractFlow):
    def extract(self):
        for e_task in self.e_tasks:
            self.flow.add_task(e_task)
        # master_csv_paths = [Path("csv/idetail_master.csv")]
        # products = self.flow.set_dependencies(
        #     self.e_tasks[0],
        #     upstream_tasks=None,
        #     downstream_tasks=None,
        #     keyword_tasks=None,
        #     mapped=False,
        #     validate=None)
        # return master_csv_paths, products
        return None, None

    def transform(self, master_csv_paths, products = None):
        # # resource_data = self.t_tasks[0].map(product_name=products)
        # resource_data = self.flow.set_dependencies(
        #     self.t_tasks[0],
        #     keyword_tasks={"product_name": products},
        #     mapped=True)
        # # master_data = self.t_tasks[1].map(resource_data=flatten(resource_data), master_csv_path=unmapped(master_csv_paths[0]))
        # master_data = self.flow.set_dependencies(
        #     self.t_tasks[1],
        #     keyword_tasks={"resource_data": flatten(resource_data), "master_csv_path": unmapped(master_csv_paths[0])},
        #     mapped=True)
        # return master_data, resource_data
        return None, None

    def load(self, master_data, resource_data = None, products = None):
        # # contents_deleted = self.l_tasks[0].map(resource_data=flatten(resource_data))
        # contents_deleted = self.flow.set_dependencies(
        #     self.l_tasks[0],
        #     keyword_tasks={"resource_data": flatten(resource_data)},
        #     mapped=True)
        # # contents_registered = self.l_tasks[1].map(
        # #     master_data=master_data,
        # #     resource_data=flatten(resource_data),
        # #     upstream_tasks=[contents_deleted])
        # contents_registered = self.flow.set_dependencies(
        #     self.l_tasks[1],
        #     upstream_tasks=[contents_deleted],
        #     keyword_tasks={"master_data": master_data, "resource_data": flatten(resource_data)},
        #     mapped=True)
        # # resources_updated = self.l_tasks[2].map(
        # #     product_name=products,
        # #     resource_data=resource_data)
        # resources_updated = self.flow.set_dependencies(
        #     self.l_tasks[2],
        #     keyword_tasks={"product_name": products, "resource_data": resource_data},
        #     mapped=True)
        # # self.l_tasks[3].map(
        # #     resource_data=resource_data,
        # #     upstream_tasks=[contents_registered, resources_updated])
        # self.flow.set_dependencies(
        #     self.l_tasks[3],
        #     upstream_tasks=[contents_registered, resources_updated],
        #     keyword_tasks={"resource_data": resource_data},
        #     mapped=True)
        pass

# Task classes
class SayHelloTask(Task):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.module_name = 'say_hello_task'


    def run(self, from_date: datetime = None, **kwargs):
        flow_params = prefect.context.get("parameters", {})

        self.logger.info(f'Hello World! {flow_params=} {type(from_date)=} {from_date=}')

class GetProductsTask(Task):
    def run(self):
        self.logger.info(f"{self.__class__.__name__}")
        return ['DUM', 'BEL']

class GetCsvResourceDataByProductTask(Task):
    def run(self, product_name: str):
        self.logger.info(f"{self.__class__.__name__}: {product_name=}")
        return [
            {"key_message_id": "DUM0000001", "is_slide": True},
            {"key_message_id": "BEL0000009", "is_slide": False}
        ]

class GetCsvMasterDataTask(Task):
    def run(self, resource_data: dict, master_csv_path: Path = None):
        self.logger.info(f"{self.__class__.__name__}: {resource_data=}, {master_csv_path=}")
        return [
            {"key_message_id": "DUM0000001", "product_name": "DUM"},
            {"key_message_id": "BEL0000009", "product_name": "BEL"}
        ]
class DeleteContentsTask(Task):
    def run(self, resource_data: dict):
        self.logger.info(f"{self.__class__.__name__}: {resource_data=}")

class RegisterContentsTask(Task):
    def run(self, master_data: dict, resource_data: dict):
        self.logger.info(f"{self.__class__.__name__}: {master_data=}, {resource_data=}")

class UpdateResourcesByProductTask(Task):
    def run(self, product_name: str, resource_data: List[dict]):
        self.logger.info(f"{self.__class__.__name__}: {product_name=}, {resource_data=}")

class UpdateStatusByS3RawDataPathTask(Task):
    def run(self, resource_data: List[dict]):
        self.logger.info(f"{self.__class__.__name__}: {resource_data=}")


# Main process
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
basic_flow = AbstractFlow(
    flow_name="basic_flow",
    parameters=[message_parameter, datetime_parameter],
    e_tasks=[SayHelloTask()],
    t_tasks=[],
    l_tasks=[])

idetail_flow = IdetailFlow(
    flow_name="idetail_flow",
    parameters=[message_parameter, datetime_parameter],
    e_tasks=[SayHelloTask()],
    t_tasks=[],
    l_tasks=[]
    # e_tasks=[GetProductsTask()],
    # t_tasks=[GetCsvResourceDataByProductTask(), GetCsvMasterDataTask()],
    # l_tasks=[DeleteContentsTask(), RegisterContentsTask(), UpdateResourcesByProductTask(), UpdateStatusByS3RawDataPathTask()]
)

# Register flow
# basic_flow_id = basic_flow.register()
idetail_flow_id = idetail_flow.register()

# Run flow
# basic_flow.run(flow_id=basic_flow_id, parameters={'msg': "Run registered flow.", 'from_date': "2022-02-17T20:13:00+09:00"})
idetail_flow.run(flow_id=idetail_flow_id, parameters={'msg': "Run registered flow.", 'from_date': "2022-02-17T20:13:00+09:00"})
