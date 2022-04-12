from dataclasses import dataclass

from flows.abstract_demands import AbstractDemands

@dataclass
class IdetailDemands(AbstractDemands):
    get_products_task: str = "get_products_task"
    get_csv_resource_data_by_product_task: str = "get_csv_resource_data_by_product_task"
    get_paths_of_master_csv_task: str = "get_paths_of_master_csv_task"
    get_csv_master_data_task: str = "get_csv_master_data_task"
    delete_contents_task: str = "delete_contents_task"
    register_contents_task: str = "register_contents_task"
    update_resources_by_product_task: str = "update_resources_by_product_task"
    update_status_by_s3_raw_data_path_task: str = "update_status_by_s3_raw_data_path_task"
