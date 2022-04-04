from datetime import datetime

import prefect
from prefect import Task

class SayHelloTask(Task):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.module_name = 'say_hello_task'


    def run(self, from_date: datetime = None, **kwargs):
        flow_params = prefect.context.get("parameters", {})

        self.logger.info(f'Hello World! {flow_params=} {type(from_date)=} {from_date=}')
