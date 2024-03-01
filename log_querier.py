from flytekit import task, workflow, LaunchPlan, ImageSpec, Secret
import flytekit
import os
import json

import boto3
from flytekitplugins.flyteinteractive import vscode

aws_image = ImageSpec(
    name="aws_image",
    requirements="requirements.txt",
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
def query_logs() -> list[str]:
    secret_val = flytekit.current_context().secrets.get(SECRET_GROUP, SECRET_KEY)
    secret_data = json.loads(secret_val)
    aws_access_key_id = secret_data["AWSAccessKeyId"]
    aws_secret_access_key = secret_data["AWSSecretAccessKey"]

    # Initialize a boto3 session
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    log_group_name = "/aws/containerinsights/opta-oc-production/application"
    log_stream_name = "fluentbit-kube.var.log.containers.avp5hxlg5xb5khfwbp6q-n0-0_flytesnacks-development_avp5hxlg5xb5khfwbp6q-n0-0-298bd18bd8fb93c0a3464aad8ee76ff03b1b9e255ba775ccaa5fce932a5d1c0a.log"

    client = session.client('logs')

    response = client.get_log_events(
        logGroupName=log_group_name,
        logStreamName=log_stream_name
    )

    list_output = []

    for event in response["events"]:

        json_payload = json.loads(event["message"])
        log_json = json_payload["log"]

        log_timestamp = ""
        log_name = ""
        log_level_name = ""
        log_message = ""

        # if this is a log entry, properly format the inner json so we can parse it
        if "{\"asctime\":" in log_json:
            start_index = log_json.find("{\"asctime\":")
            test = log_json[start_index:]
            inner_json = json.loads(log_json[start_index:])

            log_timestamp = inner_json["asctime"]

            if "name" in inner_json.keys():
                log_name = inner_json["name"]
            else:
                print("Key 'name' does not exist in the parsed JSON.")

            if "levelname" in inner_json.keys():
                log_level_name = inner_json["levelname"]
            else:
                print("Key 'levelname' does not exist in the parsed JSON.")

            if "message" in inner_json.keys():
                log_message = inner_json["message"]
            else:
                print("Key 'message' does not exist in the parsed JSON.")

            output_str = log_timestamp
            if log_name != "":
                output_str = output_str + " " + log_name
            if log_level_name != "":
                output_str = output_str + " " + log_level_name
            if log_message != "":
                output_str = output_str + " " + log_message

            list_output.append(output_str)
            print(output_str)

        else:
            print(
                "Key 'asctime' does not exist in the parsed JSON and therefore we do not treat this as a log message.")

    return list_output


@workflow
def query_logs_wf() -> list[str]:
    return query_logs()


