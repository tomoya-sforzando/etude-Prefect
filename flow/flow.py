import prefect
from prefect import Client, task, Flow


@task
def hello_task():
    logger = prefect.context.get("logger")
    logger.info("Hello world!")


with Flow("hello-flow") as flow:
    hello_task()

client = Client()
client.create_project(project_name="Hello, World!")

flow.register(project_name="Hello, World!")
# flow.run()
