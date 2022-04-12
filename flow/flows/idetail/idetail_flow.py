import os
from typing import List

from prefect import flatten

from flows.abstract_flow_on_demand import AbstractFlowOnDemand
from flows.idetail.idetail_demands import IdetailDemands
from flows.idetail.idetail_tasks import IdetailTasks

PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', 'etude-Prefect')

class IdetailFlow(AbstractFlowOnDemand):
    def __init__(self, flow_name = "idetail_flow", tasks = IdetailTasks()):
        super().__init__(flow_name, tasks)

    def build(self, demands: List[IdetailDemands] = [IdetailDemands.update_status_by_s3_raw_data_path_task]):
        super().build(demands)

    def build_basic_flow(self):
        self.tasks.get_csv_resource_data_by_product_task.set_upstream(
            flow=self.basic_flow,
            task=self.tasks.get_products_task, key="product_name", mapped=True)

        self.tasks.get_csv_master_data_task.set_upstream(
            flow=self.basic_flow,
            task=flatten(self.tasks.get_csv_resource_data_by_product_task), key="resource_data", mapped=True)
        self.tasks.get_csv_master_data_task.set_upstream(
            flow=self.basic_flow,
            task=self.tasks.get_paths_of_master_csv_task, key="master_csv_path", mapped=False)

        self.tasks.delete_contents_task.set_upstream(
            flow=self.basic_flow,
            task=flatten(self.tasks.get_csv_resource_data_by_product_task), key="resource_data", mapped=True)

        self.tasks.register_contents_task.set_upstream(
            flow=self.basic_flow,
            task=self.tasks.delete_contents_task)
        self.tasks.register_contents_task.set_upstream(
            flow=self.basic_flow,
            task=self.tasks.get_csv_master_data_task, key="master_data", mapped=True)
        self.tasks.register_contents_task.set_upstream(
            flow=self.basic_flow,
            task=flatten(self.tasks.get_csv_resource_data_by_product_task), key="resource_data", mapped=True)

        self.tasks.update_resources_by_product_task.set_upstream(
            flow=self.basic_flow,
            task=self.tasks.get_products_task, key="product_name", mapped=True)
        self.tasks.update_resources_by_product_task.set_upstream(
            flow=self.basic_flow,
            task=self.tasks.get_csv_resource_data_by_product_task, key="resource_data", mapped=True)

        self.tasks.update_status_by_s3_raw_data_path_task.set_upstream(
            flow=self.basic_flow,
            task=self.tasks.register_contents_task)
        self.tasks.update_status_by_s3_raw_data_path_task.set_upstream(
            flow=self.basic_flow,
            task=self.tasks.update_resources_by_product_task)
        self.tasks.update_status_by_s3_raw_data_path_task.set_upstream(
            flow=self.basic_flow,
            task=self.tasks.get_csv_resource_data_by_product_task, key="resource_data", mapped=True)
