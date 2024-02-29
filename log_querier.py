from flytekit import task, workflow, LaunchPlan, ImageSpec, Secret
import flytekit
import os
import json

import boto3
import awscli
from flytekitplugins.flyteinteractive import vscode


aws_image = ImageSpec(
    name="aws_image",
    packages=["awscli", "boto3", "flytekit", "flytekitplugins.flyteinteractive"],
    # requirements="requirements.txt",
    registry=os.environ.get("DOCKER_REGISTRY", None),
)


SECRET_GROUP = "arn:aws:secretsmanager:us-east-2:356633062068:secret:"
SECRET_KEY = "cloudwatch-readonly-JZZBP6"


@task(
    container_image=aws_image,
    secret_requests=[
        Secret(
            group=SECRET_GROUP,
            key=SECRET_KEY,
            mount_requirement=Secret.MountType.FILE
        )
    ]
)
@vscode()
def query_logs():

    secret_val = flytekit.current_context().secrets.get(SECRET_GROUP, SECRET_KEY)
    secret_data = json.loads(secret_val)
    access_key = secret_data["AWSAccessKeyId"]
    secret_key = secret_data["AWSSecretAccessKey"]

    # Initialize a boto3 session
    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        # region_name='YOUR_PREFERRED_REGION'
    )

    # Example: Using the session to create an S3 client
    s3 = session.client('s3')


@workflow
def query_logs_wf():
    query_logs()


