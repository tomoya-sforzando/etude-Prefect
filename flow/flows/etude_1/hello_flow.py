from prefect import Flow
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import UniversalRun
from prefect.storage import Local

from tasks.say_hello_task import SayHelloTask

hello_flow = Flow(name="hello_flow",
            run_config=UniversalRun(),
            storage=Local(add_default_labels=False),
            executor=LocalDaskExecutor())

hello_flow.set_dependencies(SayHelloTask())

# with Flow('hello-flow', storage=Local(add_default_labels=False), executor=LocalDaskExecutor()) as hello_flow:
#     SayHelloTask()
