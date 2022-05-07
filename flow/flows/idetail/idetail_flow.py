import os
from typing import List

from prefect import flatten, unmapped

from flows.abstract_flow_on_demand import AbstractFlowOnDemand
from flows.idetail.idetail_settings import IdetailTask

PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', 'etude-Prefect')

class IdetailFlow(AbstractFlowOnDemand):
    def __init__(self):
        self.meta_task = IdetailTask()
        super().__init__(name="idetail_flow", meta_task=self.meta_task)

    def build(self, tasks_on_demand: List[IdetailTask] = [IdetailTask.UpdateStatusByS3RawDataPathTask]):
        super().build(tasks_on_demand)

    def build_basic_flow(self):
        self.meta_task.GetCsvResourceDataByProductTask.set_dependencies(
            mapped=True,
            keyword_tasks={
                "product_name": self.meta_task.GetProductsTask
            },
            flow=self.basic_flow,
        )

        self.meta_task.GetCsvMasterDataTask.set_dependencies(
            mapped=True,
            keyword_tasks={
                "resource_data": flatten(self.meta_task.GetCsvResourceDataByProductTask),
                "master_csv_path": unmapped(self.meta_task.GetPathsOfMasterCsvTask)
            },
            flow=self.basic_flow,
        )

        self.meta_task.DeleteContentsTask.set_dependencies(
            mapped=True,
            keyword_tasks={
                "resource_data": flatten(self.meta_task.GetCsvResourceDataByProductTask)
            },
            flow=self.basic_flow
        )

        self.meta_task.RegisterContentsTask.set_dependencies(
            mapped=True,
            keyword_tasks={
                "master_data": self.meta_task.GetCsvMasterDataTask,
                "resource_data": flatten(self.meta_task.GetCsvResourceDataByProductTask)
            },
            upstream_tasks=[
                self.meta_task.DeleteContentsTask,
            ],
            flow=self.basic_flow,
        )

        self.meta_task.UpdateResourcesByProductTask.set_dependencies(
            mapped=True,
            keyword_tasks={
                "product_name": self.meta_task.GetProductsTask,
                "resource_data": self.meta_task.GetCsvResourceDataByProductTask
            },
            flow=self.basic_flow,
        )

        self.meta_task.UpdateStatusByS3RawDataPathTask.set_dependencies(
            mapped=True,
            keyword_tasks={
                "resource_data": self.meta_task.GetCsvResourceDataByProductTask
            },
            upstream_tasks=[
                self.meta_task.RegisterContentsTask,
                self.meta_task.UpdateResourcesByProductTask
            ],
            flow=self.basic_flow,
        )
