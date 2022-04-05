import os

import prefect
from prefect import Client, Parameter
from prefect.tasks.notifications.email_task import EmailTask

from flows.idetail_flow import IdetailFlow
from tasks.idetail.delete_contents_task import DeleteContentsTask
from tasks.idetail.get_csv_master_data_task import GetCsvMasterDataTask
from tasks.idetail.get_csv_resource_data_by_product_task import GetCsvResourceDataByProductTask
from tasks.idetail.get_paths_of_master_csv_task import GetPathsOfMasterCsvTask
from tasks.idetail.get_products_task import GetProductsTask
from tasks.idetail.register_contents_task import RegisterContentsTask
from tasks.idetail.update_resources_by_product_task import UpdateResourcesByProductTask
from tasks.idetail.update_status_by_s3_raw_data_path_task import UpdateStatusByS3RawDataPathTask

PROJECT_NAME = os.getenv('PREFECT_PROJECT_NAME', 'etude-Prefect')

# Setup prefect cloud client and create project
Client().create_project(project_name=PROJECT_NAME)

# Setup parameters
message_parameter = Parameter('msg', default='this is parameter')
datetime_parameter = prefect.core.parameter.DateTimeParameter('from_date', required=False)

# Setup tasks
email_task = EmailTask(
    subject='Prefect Notification - Flow finished',
    msg='This message is sent with AWS SES SMTP.',
    smtp_server='email-smtp.ap-northeast-1.amazonaws.com',
    email_from='<Email address needs domain that it was verified identities in Amazon SES>',
    email_to='')

# Setup flow
idetail_flow = IdetailFlow(
    flow_name="idetail_flow",
    parameters=[message_parameter, datetime_parameter],
    e_tasks=[GetPathsOfMasterCsvTask(), GetProductsTask()],
    t_tasks=[GetCsvResourceDataByProductTask(), GetCsvMasterDataTask()],
    l_tasks=[DeleteContentsTask(), RegisterContentsTask(), UpdateResourcesByProductTask(), UpdateStatusByS3RawDataPathTask()]
)

# Register flow
idetail_flow_id = idetail_flow.register()

# Run flow
idetail_flow.run(flow_id=idetail_flow_id, parameters={'msg': "Run registered flow", 'from_date': "2022-02-17T20:13:00+09:00"})
