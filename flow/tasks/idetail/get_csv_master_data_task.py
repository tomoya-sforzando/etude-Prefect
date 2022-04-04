from pathlib import Path

from prefect import Task

class GetCsvMasterDataTask(Task):
    def run(self, resource_data: dict, master_csv_path: Path = None):
        self.logger.info(f"{self.__class__.__name__}: {resource_data=}, {master_csv_path=}")
        return [
            {"key_message_id": "DUM0000001", "product_name": "DUM"},
            {"key_message_id": "BEL0000009", "product_name": "BEL"}
        ]
