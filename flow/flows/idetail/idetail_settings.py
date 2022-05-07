from dataclasses import dataclass

from prefect import Task

from flows.abstract_settings import AbstractMetaTask
from tasks.idetail.delete_contents_task import DeleteContentsTask
from tasks.idetail.get_csv_master_data_task import GetCsvMasterDataTask
from tasks.idetail.get_csv_resource_data_by_product_task import GetCsvResourceDataByProductTask
from tasks.idetail.get_paths_of_master_csv_task import GetPathsOfMasterCsvTask
from tasks.idetail.get_products_task import GetProductsTask
from tasks.idetail.register_contents_task import RegisterContentsTask
from tasks.idetail.update_resources_by_product_task import UpdateResourcesByProductTask
from tasks.idetail.update_status_by_s3_raw_data_path_task import UpdateStatusByS3RawDataPathTask


@dataclass
class IdetailTask(AbstractMetaTask):
    GetProductsTask: Task = GetProductsTask()
    GetCsvResourceDataByProductTask: Task = GetCsvResourceDataByProductTask()
    GetPathsOfMasterCsvTask: Task = GetPathsOfMasterCsvTask()
    GetCsvMasterDataTask: Task = GetCsvMasterDataTask()
    DeleteContentsTask: Task = DeleteContentsTask()
    RegisterContentsTask: Task = RegisterContentsTask()
    UpdateResourcesByProductTask: Task = UpdateResourcesByProductTask()
    UpdateStatusByS3RawDataPathTask: Task = UpdateStatusByS3RawDataPathTask()
