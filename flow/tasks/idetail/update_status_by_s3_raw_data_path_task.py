from typing import List

from prefect import Task

class UpdateStatusByS3RawDataPathTask(Task):
    def run(self, resource_data: List[dict]):
        self.logger.info(f"{self.__class__.__name__}: {resource_data=}")
