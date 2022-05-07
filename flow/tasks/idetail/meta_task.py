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
    GetProductsTask: Task = GetProductsTask()
    GetCsvResourceDataByProductTask: Task = GetCsvResourceDataByProductTask()
    GetPathsOfMasterCsvTask: Task = GetPathsOfMasterCsvTask()
    GetCsvMasterDataTask: Task = GetCsvMasterDataTask()
    DeleteContentsTask: Task = DeleteContentsTask()
    RegisterContentsTask: Task = RegisterContentsTask()
    UpdateResourcesByProductTask: Task = UpdateResourcesByProductTask()
    UpdateStatusByS3RawDataPathTask: Task = UpdateStatusByS3RawDataPathTask()
