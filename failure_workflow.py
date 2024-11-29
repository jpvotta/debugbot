from flytekit import task, workflow, LaunchPlan, CronSchedule

@task
def failure_task(a: str) -> str:
    raise ValueError("A value error occurred.")
    return "hello " + a


@workflow
def failure_workflow(a: str):
    failure_task(a=a)



