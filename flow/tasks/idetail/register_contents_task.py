from prefect import Task

class RegisterContentsTask(Task):
    def run(self, master_data: dict, resource_data: dict):
        self.logger.info(f"{self.__class__.__name__}: {master_data=}, {resource_data=}")
