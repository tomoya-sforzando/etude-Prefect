from prefect import Task

class GetCsvResourceDataByProductTask(Task):
    def run(self, product_name: str):
        self.logger.info(f"{self.__class__.__name__}: {product_name=}")
        return [
            {"key_message_id": "DUM0000001", "is_slide": True},
            {"key_message_id": "BEL0000009", "is_slide": False}
        ]
