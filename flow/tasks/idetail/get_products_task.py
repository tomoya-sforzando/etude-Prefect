from prefect import Task

class GetProductsTask(Task):
    def run(self):
        self.logger.info(f"{self.__class__.__name__}")
        return ['DUM', 'BEL']
