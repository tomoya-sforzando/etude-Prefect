from prefect import Task

class GetPathsOfMasterCsvTask(Task):
    def run(self):
        self.logger.info(f"{self.__class__.__name__}")
        return ["csv/idetail_master.csv"]
