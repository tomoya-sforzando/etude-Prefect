import os
# from abc import abstractmethod
from datetime import datetime
from pathlib import Path
from typing import List

import prefect
from prefect import Client, Flow, Parameter, flatten, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import UniversalRun
from prefect.storage import Local
from prefect.tasks.notifications.email_task import EmailTask

from flow.tasks.say_hello_task import SayHelloTask

from flow.tasks.idetail.delete_contents_task import DeleteContentsTask
from flow.tasks.idetail.get_csv_master_data_task import GetCsvMasterDataTask
from flow.tasks.idetail.get_csv_resource_data_by_product_task import GetCsvResourceDataByProductTask
from flow.tasks.idetail.get_paths_of_master_csv_task import GetPathsOfMasterCsvTask
from flow.tasks.idetail.get_products_task import GetProductsTask
from flow.tasks.idetail.register_contents_task import RegisterContentsTask
from flow.tasks.idetail.update_resources_by_product_task import UpdateResourcesByProductTask
from flow.tasks.idetail.update_status_by_s3_raw_data_path_task import UpdateStatusByS3RawDataPathTask

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

class IdetailFlow(AbstractFlow):
    def extract(self):
        master_csv_paths = self.flow.set_dependencies(
            self.e_tasks[0],
            upstream_tasks=None,
            downstream_tasks=None,
            keyword_tasks=None,
            mapped=False,
            validate=None)
        # master_csv_paths = [Path("csv/idetail_master.csv")]

        products = self.flow.set_dependencies(
            self.e_tasks[1],
            upstream_tasks=None,
            downstream_tasks=None,
            keyword_tasks=None,
            mapped=False,
            validate=None)

    def transform(self):
        # resource_data = self.t_tasks[0].map(product_name=products)
        self.flow.set_dependencies(
            self.t_tasks[0],
            upstream_tasks=[self.e_tasks[1]],
            keyword_tasks={"product_name": self.e_tasks[1]},
            mapped=True)
        # master_data = self.t_tasks[1].map(resource_data=flatten(resource_data), master_csv_path=unmapped(master_csv_paths[0]))
        master_data = self.flow.set_dependencies(
            self.t_tasks[1],
            upstream_tasks=[self.t_tasks[0], self.e_tasks[0]],
            keyword_tasks={"resource_data": flatten(self.t_tasks[0]), "master_csv_path": unmapped(self.e_tasks[0])},
            mapped=True)

    def load(self):
        # contents_deleted = self.l_tasks[0].map(resource_data=flatten(resource_data))
        self.flow.set_dependencies(
            self.l_tasks[0],
            keyword_tasks={"resource_data": flatten(self.t_tasks[0])},
            mapped=True)
        # contents_registered = self.l_tasks[1].map(
        #     master_data=master_data,
        #     resource_data=flatten(resource_data),
        #     upstream_tasks=[contents_deleted])
        contents_registered = self.flow.set_dependencies(
            self.l_tasks[1],
            upstream_tasks=[self.l_tasks[0]],
            keyword_tasks={"master_data": self.t_tasks[1], "resource_data": flatten(self.t_tasks[0])},
            mapped=True)
        # resources_updated = self.l_tasks[2].map(
        #     product_name=products,
        #     resource_data=resource_data)
        self.flow.set_dependencies(
            self.l_tasks[2],
            keyword_tasks={"product_name": self.e_tasks[1], "resource_data": self.t_tasks[0]},
            mapped=True)
        # self.l_tasks[3].map(
        #     resource_data=resource_data,
        #     upstream_tasks=[contents_registered, resources_updated])
        self.flow.set_dependencies(
            self.l_tasks[3],
            upstream_tasks=[self.l_tasks[1], self.l_tasks[2]],
            keyword_tasks={"resource_data": self.t_tasks[0]},
            mapped=True)

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
    e_tasks=[GetPathsOfMasterCsvTask(), GetProductsTask()],
    t_tasks=[GetCsvResourceDataByProductTask(), GetCsvMasterDataTask()],
    l_tasks=[DeleteContentsTask(), RegisterContentsTask(), UpdateResourcesByProductTask(), UpdateStatusByS3RawDataPathTask()]
)

# Register flow
# basic_flow_id = basic_flow.register()
idetail_flow_id = idetail_flow.register()

# Run flow
# basic_flow.run(flow_id=basic_flow_id, parameters={'msg': "Run registered flow.", 'from_date': "2022-02-17T20:13:00+09:00"})
idetail_flow.run(flow_id=idetail_flow_id, parameters={'msg': "Run registered flow.", 'from_date': "2022-02-17T20:13:00+09:00"})
