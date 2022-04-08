import os

from prefect import Client, Flow, flatten, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import UniversalRun
from prefect.storage import Local

from tasks.idetail.delete_contents_task import DeleteContentsTask
from tasks.idetail.get_csv_master_data_task import GetCsvMasterDataTask
from tasks.idetail.get_csv_resource_data_by_product_task import GetCsvResourceDataByProductTask
from tasks.idetail.get_paths_of_master_csv_task import GetPathsOfMasterCsvTask
from tasks.idetail.get_products_task import GetProductsTask
from tasks.idetail.register_contents_task import RegisterContentsTask
from tasks.idetail.update_resources_by_product_task import UpdateResourcesByProductTask
from tasks.idetail.update_status_by_s3_raw_data_path_task import UpdateStatusByS3RawDataPathTask

PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', 'etude-Prefect')

class IdetailFlowWithSolution:
    def __init__(self) -> None:
        self.flow = Flow(
            name="idetail_flow_with_solution",
            run_config=UniversalRun(),
            storage=Local(add_default_labels=False),
            executor=LocalDaskExecutor())

    def register(self):
        self.build_flow()
        return self.flow.register(project_name=PROJECT_NAME)

    def run(self, flow_id: str, parameters: dict = {}):
        Client().create_flow_run(flow_id=flow_id, parameters=parameters)

    def build_flow(self):
        get_products_task = GetProductsTask()
        get_csv_resource_data_by_product_task = GetCsvResourceDataByProductTask()
        get_paths_of_master_csv_task = GetPathsOfMasterCsvTask()
        get_csv_master_data_task = GetCsvMasterDataTask()
        delete_contents_task = DeleteContentsTask()
        register_contents_task = RegisterContentsTask()
        update_resources_by_product_task = UpdateResourcesByProductTask()
        update_status_by_s3_raw_data_path_task = UpdateStatusByS3RawDataPathTask()

        self.flow.set_dependencies(
            get_csv_resource_data_by_product_task,
            keyword_tasks={"product_name": get_products_task},
            mapped=True)

        self.flow.set_dependencies(
            get_csv_master_data_task,
            keyword_tasks={"resource_data": flatten(get_csv_resource_data_by_product_task), "master_csv_path": unmapped(get_paths_of_master_csv_task)},
            mapped=True)

        self.flow.set_dependencies(
            delete_contents_task,
            keyword_tasks={"resource_data": flatten(get_csv_resource_data_by_product_task)},
            mapped=True)

        self.flow.set_dependencies(
            register_contents_task,
            upstream_tasks=[delete_contents_task],
            keyword_tasks={"master_data": get_csv_master_data_task, "resource_data": flatten(get_csv_resource_data_by_product_task)},
            mapped=True)

        self.flow.set_dependencies(
            update_resources_by_product_task,
            keyword_tasks={"product_name": get_products_task, "resource_data": get_csv_resource_data_by_product_task},
            mapped=True)

        self.flow.set_dependencies(
            update_status_by_s3_raw_data_path_task,
            upstream_tasks=[register_contents_task, update_resources_by_product_task],
            keyword_tasks={"resource_data": get_csv_resource_data_by_product_task},
            mapped=True)
