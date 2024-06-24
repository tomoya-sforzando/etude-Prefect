from dataclasses import dataclass

from prefect import Task

from tasks.abstract_meta_task import AbstractMetaTask
from .delete_contents_task import DeleteContentsTask
from .get_csv_master_data_task import GetCsvMasterDataTask
from .get_csv_resource_data_by_product_task import GetCsvResourceDataByProductTask
from .get_paths_of_master_csv_task import GetPathsOfMasterCsvTask
from .get_products_task import GetProductsTask
from .register_contents_task import RegisterContentsTask
from .update_resources_by_product_task import UpdateResourcesByProductTask
from .update_status_by_s3_raw_data_path_task import UpdateStatusByS3RawDataPathTask


@dataclass
class MetaTask(AbstractMetaTask):
    get_products: Task = GetProductsTask(
        name="get_products"
    )
    get_csv_resource_data_by_product: Task = GetCsvResourceDataByProductTask(
        name="get_csv_resource_data_by_product"
    )
    get_paths_of_master_csv: Task = GetPathsOfMasterCsvTask(
        name="get_paths_of_master_csv"
    )
    get_csv_master_data: Task = GetCsvMasterDataTask(
        name="get_csv_master_data"
    )
    delete_contents: Task = DeleteContentsTask(
        name="delete_contents"
    )
    register_contents: Task = RegisterContentsTask(
        name="register_contents"
    )
    update_resources_by_product: Task = UpdateResourcesByProductTask(
        name="update_resources_by_product"
    )
    update_status_by_s3_raw_data_path: Task = UpdateStatusByS3RawDataPathTask(
        name="update_status_by_s3_raw_data_path"
    )
