import os
from typing import List

from prefect import flatten, unmapped

from tasks.idetail.meta_task import MetaTask as IdetailTask
from flows.abstract_flow_on_demand import AbstractFlowOnDemand

PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', 'etude-Prefect')

class Flow(AbstractFlowOnDemand):
    def __init__(self):
        self.meta_task = IdetailTask()
        super().__init__(name="idetail_flow", meta_task=self.meta_task)

    def build(self, tasks_on_demand: List[IdetailTask] = [IdetailTask.update_status_by_s3_raw_data_path]):
        super().build(tasks_on_demand)

    def build_basic_flow(self):
        self.meta_task.get_csv_resource_data_by_product.set_dependencies(
            mapped=True,
            keyword_tasks={
                "product_name": self.meta_task.get_products
            },
            flow=self.basic_flow,
        )

        self.meta_task.get_csv_master_data.set_dependencies(
            mapped=True,
            keyword_tasks={
                "resource_data": flatten(self.meta_task.get_csv_resource_data_by_product),
                "master_csv_path": unmapped(self.meta_task.get_paths_of_master_csv)
            },
            flow=self.basic_flow,
        )

        self.meta_task.delete_contents.set_dependencies(
            mapped=True,
            keyword_tasks={
                "resource_data": flatten(self.meta_task.get_csv_resource_data_by_product)
            },
            flow=self.basic_flow
        )

        self.meta_task.register_contents.set_dependencies(
            mapped=True,
            keyword_tasks={
                "master_data": self.meta_task.get_csv_master_data,
                "resource_data": flatten(self.meta_task.get_csv_resource_data_by_product)
            },
            upstream_tasks=[
                self.meta_task.delete_contents,
            ],
            flow=self.basic_flow,
        )

        self.meta_task.update_resources_by_product.set_dependencies(
            mapped=True,
            keyword_tasks={
                "product_name": self.meta_task.get_products,
                "resource_data": self.meta_task.get_csv_resource_data_by_product
            },
            flow=self.basic_flow,
        )

        self.meta_task.update_status_by_s3_raw_data_path.set_dependencies(
            mapped=True,
            keyword_tasks={
                "resource_data": self.meta_task.get_csv_resource_data_by_product
            },
            upstream_tasks=[
                self.meta_task.register_contents,
                self.meta_task.update_resources_by_product
            ],
            flow=self.basic_flow,
        )
