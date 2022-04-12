import os
from dataclasses import dataclass

from prefect import Client, Flow, Task, flatten
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

@dataclass
class IdetailDemands:
    get_products_task: str = "get_products_task"
    get_csv_resource_data_by_product_task: str = "get_csv_resource_data_by_product_task"
    get_paths_of_master_csv_task: str = "get_paths_of_master_csv_task"
    get_csv_master_data_task: str = "get_csv_master_data_task"
    delete_contents_task: str = "delete_contents_task"
    register_contents_task: str = "register_contents_task"
    update_resources_by_product_task: str = "update_resources_by_product_task"
    update_status_by_s3_raw_data_path_task: str = "update_status_by_s3_raw_data_path_task"

@dataclass
class IdetailTasks:
    get_products_task: Task = GetProductsTask()
    get_csv_resource_data_by_product_task: Task = GetCsvResourceDataByProductTask()
    get_paths_of_master_csv_task: Task = GetPathsOfMasterCsvTask()
    get_csv_master_data_task: Task = GetCsvMasterDataTask()
    delete_contents_task: Task = DeleteContentsTask()
    register_contents_task: Task = RegisterContentsTask()
    update_resources_by_product_task: Task = UpdateResourcesByProductTask()
    update_status_by_s3_raw_data_path_task: Task = UpdateStatusByS3RawDataPathTask()

    def get_by_solution(self, demand: IdetailDemands):
        if demand in (IdetailTasks.__dict__).keys():
            return IdetailTasks.__dict__.get(demand)
        else:
            return None

class IdetailFlowOnDemand:
    def __init__(self) -> None:
        self.base_flow = Flow(
            name="idetail_flow_on_demand",
            run_config=UniversalRun(),
            storage=Local(add_default_labels=False),
            executor=LocalDaskExecutor())

        self.flow_on_demand = self.base_flow.copy()

        self.idetail_tasks = IdetailTasks()

    def build(self, idetail_solution: IdetailDemands = None):
        ## set tasks with set_upstream
        self.idetail_tasks.get_csv_resource_data_by_product_task.set_upstream(
            flow=self.base_flow,
            task=self.idetail_tasks.get_products_task, key="product_name", mapped=True)

        self.idetail_tasks.get_csv_master_data_task.set_upstream(
            flow=self.base_flow,
            task=flatten(self.idetail_tasks.get_csv_resource_data_by_product_task), key="resource_data", mapped=True)
        self.idetail_tasks.get_csv_master_data_task.set_upstream(
            flow=self.base_flow,
            task=self.idetail_tasks.get_paths_of_master_csv_task, key="master_csv_path", mapped=False)

        self.idetail_tasks.delete_contents_task.set_upstream(
            flow=self.base_flow,
            task=flatten(self.idetail_tasks.get_csv_resource_data_by_product_task), key="resource_data", mapped=True)

        self.idetail_tasks.register_contents_task.set_upstream(
            flow=self.base_flow,
            task=self.idetail_tasks.delete_contents_task)
        self.idetail_tasks.register_contents_task.set_upstream(
            flow=self.base_flow,
            task=self.idetail_tasks.get_csv_master_data_task, key="master_data", mapped=True)
        self.idetail_tasks.register_contents_task.set_upstream(
            flow=self.base_flow,
            task=flatten(self.idetail_tasks.get_csv_resource_data_by_product_task), key="resource_data", mapped=True)

        self.idetail_tasks.update_resources_by_product_task.set_upstream(
            flow=self.base_flow,
            task=self.idetail_tasks.get_products_task, key="product_name", mapped=True)
        self.idetail_tasks.update_resources_by_product_task.set_upstream(
            flow=self.base_flow,
            task=self.idetail_tasks.get_csv_resource_data_by_product_task, key="resource_data", mapped=True)

        self.idetail_tasks.update_status_by_s3_raw_data_path_task.set_upstream(
            flow=self.base_flow,
            task=self.idetail_tasks.register_contents_task)
        self.idetail_tasks.update_status_by_s3_raw_data_path_task.set_upstream(
            flow=self.base_flow,
            task=self.idetail_tasks.update_resources_by_product_task)
        self.idetail_tasks.update_status_by_s3_raw_data_path_task.set_upstream(
            flow=self.base_flow,
            task=self.idetail_tasks.get_csv_resource_data_by_product_task, key="resource_data", mapped=True)

        ## build solution flow
        if idetail_solution:
            def get_target_tasks(solution_task: Task):
                target_tasks = set()
                for_search_tasks = self.base_flow.upstream_tasks(solution_task)
                while len(for_search_tasks):
                    for task in for_search_tasks:
                        target_tasks.add(task)
                        for_search_tasks = for_search_tasks | self.base_flow.upstream_tasks(task)
                        for_search_tasks.remove(task)
                    for_search_tasks = for_search_tasks - target_tasks
                target_tasks.add(solution_task)
                return target_tasks

            for task in get_target_tasks(self.idetail_tasks.get_by_solution(idetail_solution)):
                base_edges = self.base_flow.edges_to(task)
                for edge in base_edges:
                    self.flow_on_demand.add_edge(
                        upstream_task=edge.upstream_task,
                        downstream_task=edge.downstream_task,
                        key=edge.key,
                        mapped=edge.mapped,
                        flattened=edge.flattened
                    )
        else:
            self.flow_on_demand = self.base_flow

    def register(self):
        return self.flow_on_demand.register(project_name=PROJECT_NAME)

    def run(self, flow_id: str, parameters: dict = {}):
        Client().create_flow_run(flow_id=flow_id, parameters=parameters)
