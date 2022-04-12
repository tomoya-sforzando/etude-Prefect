import os

from prefect import Client

from flows.etude_1.hello_flow import hello_flow


PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', 'etude-Prefect')

client = Client()
client.create_project(project_name=PROJECT_NAME)

flow_id = hello_flow.register(project_name=PROJECT_NAME)

client.create_flow_run(flow_id=flow_id)
