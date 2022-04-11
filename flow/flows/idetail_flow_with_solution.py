import os
from sqlite3 import register_adapter

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
        self.base_flow = Flow(
            name="base_flow",
            run_config=UniversalRun(),
            storage=Local(add_default_labels=False),
            executor=LocalDaskExecutor())

        self.flow_with_solution = Flow(
            name="idetail_flow_with_solution",
            run_config=UniversalRun(),
            storage=Local(add_default_labels=False),
            executor=LocalDaskExecutor())

    def register(self):
        self.build_flow()
        return self.flow_with_solution.register(project_name=PROJECT_NAME)

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

        # ## set dependencies with Task class
        # get_csv_resource_data_by_product_task.set_dependencies(
        #     flow=self.base_flow,
        #     upstream_tasks=[get_products_task],
        #     keyword_tasks={"product_name": get_products_task},
        #     mapped=True
        # )

        # get_csv_master_data_task.set_dependencies(
        #     flow=self.base_flow,
        #     keyword_tasks={"resource_data": flatten(get_csv_resource_data_by_product_task), "master_csv_path": unmapped(get_paths_of_master_csv_task)},
        #     mapped=True
        # )

        # delete_contents_task.set_dependencies(
        #     flow=self.base_flow,
        #     keyword_tasks={"resource_data": flatten(get_csv_resource_data_by_product_task)},
        #     mapped=True
        # )

        # register_contents_task.set_dependencies(
        #     flow=self.base_flow,
        #     upstream_tasks=[delete_contents_task],
        #     keyword_tasks={"master_data": get_csv_master_data_task, "resource_data": flatten(get_csv_resource_data_by_product_task)},
        #     mapped=True
        # )

        # update_resources_by_product_task.set_dependencies(
        #     flow=self.base_flow,
        #     keyword_tasks={"product_name": get_products_task, "resource_data": get_csv_resource_data_by_product_task},
        #     mapped=True
        # )

        # update_status_by_s3_raw_data_path_task.set_dependencies(
        #     flow=self.base_flow,
        #     upstream_tasks=[register_contents_task, update_resources_by_product_task],
        #     keyword_tasks={"resource_data": get_csv_resource_data_by_product_task},
        #     mapped=True
        # )

        ## set tasks with set_upstream
        get_csv_resource_data_by_product_task.set_upstream(
            flow=self.base_flow, task=get_products_task, key="product_name", mapped=True)

        get_csv_master_data_task.set_upstream(
            flow=self.base_flow, task=flatten(get_csv_resource_data_by_product_task), key="resource_data", mapped=True)
        get_csv_master_data_task.set_upstream(
            flow=self.base_flow, task=get_paths_of_master_csv_task, key="master_csv_path", mapped=False)

        delete_contents_task.set_upstream(
            flow=self.base_flow, task=flatten(get_csv_resource_data_by_product_task), key="resource_data", mapped=True)

        register_contents_task.set_upstream(flow=self.base_flow, task=delete_contents_task)
        register_contents_task.set_upstream(
            flow=self.base_flow, task=get_csv_master_data_task, key="master_data", mapped=True)
        register_contents_task.set_upstream(
            flow=self.base_flow, task=flatten(get_csv_resource_data_by_product_task), key="resource_data", mapped=True)

        update_resources_by_product_task.set_upstream(
            flow=self.base_flow, task=get_products_task, key="product_name", mapped=True)
        update_resources_by_product_task.set_upstream(
            flow=self.base_flow, task=get_csv_resource_data_by_product_task, key="resource_data", mapped=True)

        update_status_by_s3_raw_data_path_task.set_upstream(flow=self.base_flow, task=register_contents_task)
        update_status_by_s3_raw_data_path_task.set_upstream(flow=self.base_flow, task=update_resources_by_product_task)
        update_status_by_s3_raw_data_path_task.set_upstream(
            flow=self.base_flow, task=get_csv_resource_data_by_product_task, key="resource_data", mapped=True)

        ## build solution flow
        def get_target_tasks(solution_task):
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

        for task in get_target_tasks(register_contents_task):
            base_edges = self.base_flow.edges_to(task)
            for edge in base_edges:
                self.flow_with_solution.add_edge(
                    upstream_task=edge.upstream_task,
                    downstream_task=edge.downstream_task,
                    key=edge.key,
                    mapped=edge.mapped,
                    flattened=edge.flattened
                )

        # ## add tasks(edges) to flow with Flow class
        # self.base_flow.add_task(get_products_task)
        # self.base_flow.add_edge(upstream_task=get_products_task, downstream_task=get_csv_resource_data_by_product_task, key="product_name", mapped=True)

        # ## set dependencies with Flow class
        # self.base_flow.set_dependencies(
        #     get_csv_resource_data_by_product_task,
        #     keyword_tasks={"product_name": get_products_task},
        #     mapped=True)

        # self.base_flow.set_dependencies(
        #     get_csv_master_data_task,
        #     keyword_tasks={"resource_data": flatten(get_csv_resource_data_by_product_task), "master_csv_path": unmapped(get_paths_of_master_csv_task)},
        #     mapped=True)

        # self.base_flow.set_dependencies(
        #     delete_contents_task,
        #     keyword_tasks={"resource_data": flatten(get_csv_resource_data_by_product_task)},
        #     mapped=True)

        # self.base_flow.set_dependencies(
        #     register_contents_task,
        #     upstream_tasks=[delete_contents_task],
        #     keyword_tasks={"master_data": get_csv_master_data_task, "resource_data": flatten(get_csv_resource_data_by_product_task)},
        #     mapped=True)

        # self.base_flow.set_dependencies(
        #     update_resources_by_product_task,
        #     keyword_tasks={"product_name": get_products_task, "resource_data": get_csv_resource_data_by_product_task},
        #     mapped=True)

        # self.base_flow.set_dependencies(
        #     update_status_by_s3_raw_data_path_task,
        #     upstream_tasks=[register_contents_task, update_resources_by_product_task],
        #     keyword_tasks={"resource_data": get_csv_resource_data_by_product_task},
        #     mapped=True)
