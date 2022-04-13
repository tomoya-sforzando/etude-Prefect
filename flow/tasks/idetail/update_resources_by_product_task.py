from typing import List

from prefect import Task

class UpdateResourcesByProductTask(Task):
    def run(self, product_name: str, resource_data: List[dict]):
        self.logger.info(f"{self.__class__.__name__}: {product_name=}, {resource_data=}")

