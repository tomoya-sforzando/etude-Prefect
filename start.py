from prefect.agent.ecs.agent import ECSAgent

AGENT = ECSAgent(
    cluster='msd-nector-development-prefect-cluster',
    launch_type='fargate',
    execution_role_arn='arn:aws:iam::910376070004:role/msd-nector-ecs-fullaccess-role',
    task_role_arn='arn:aws:iam::910376070004:role/msd-nector-ecs-fullaccess-role',
    labels=['ecs', 'dev']
).start()
