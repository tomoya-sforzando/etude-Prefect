from prefect import Task

class DeleteContentsTask(Task):
    def run(self, resource_data: dict):
        self.logger.info(f"{self.__class__.__name__}: {resource_data=}")
