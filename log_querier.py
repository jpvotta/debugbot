from flytekit import task, workflow, LaunchPlan, ImageSpec, Secret
import flytekit
import os
import json

import boto3
from flytekitplugins.flyteinteractive import vscode
from flytekitplugins.chatgpt import ChatGPTTask

from openai import OpenAI


aws_image = ImageSpec(
    name="aws_image",
    requirements="requirements.txt",
    registry=os.environ.get("DOCKER_REGISTRY", None),
)

LOGS_SECRET_GROUP = "arn:aws:secretsmanager:us-east-2:356633062068:secret:"
LOGS_SECRET_KEY = "cloudwatch-readonly-JZZBP6"

OPENAI_SECRET_GROUP = "arn:aws:secretsmanager:us-east-2:356633062068:secret:"
OPENAI_SECRET_KEY = "openai-PqfFLj"

OPENAI_ORG_ID = "org-0pKlDFzPsouGXeUrSMTGsFII"


@task(
    container_image=aws_image,
    secret_requests=[
        Secret(
            group=LOGS_SECRET_GROUP,
            key=LOGS_SECRET_KEY,
            mount_requirement=Secret.MountType.FILE
        )
    ]
)
@vscode()
def query_logs() -> str:
    secret_val = flytekit.current_context().secrets.get(LOGS_SECRET_GROUP, LOGS_SECRET_KEY)
    secret_data = json.loads(secret_val)
    aws_access_key_id = secret_data["AWSAccessKeyId"]
    aws_secret_access_key = secret_data["AWSSecretAccessKey"]

    # Initialize a boto3 session
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    log_group_name = "/aws/containerinsights/opta-oc-production/application"
    log_stream_name = "https://us-east-2.console.aws.amazon.com/cloudwatch/home?region=us-east-2#logsV2:log-groups/log-group/$252Faws$252Fcontainerinsights$252Fopta-oc-production$252Fapplication/log-events/fluentbit-kube.var.log.containers.f3979948cb5254dac837-n0-0_flytesnacks-development_f3979948cb5254dac837-n0-0-904f61f5c5080852c9f1e30cfc22e8a6f90401f332e8da5ee21564b98f19e639.log"
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

    if len(list_output) > 0:
        return '\n'.join(list_output)
    else:
        return ""


@task(container_image=aws_image)
def preprocess_task(input_error: str) -> str:
    preamble = "the following is an error message from the logs of a user's Flyte task. Please write a short (100 words max) explanation of what is happening in this error:\n\n"
    return preamble + input_error


# gpt_task = ChatGPTTask(
#     name="chatgpt",
#     # openai_organization="org-NayNG68kGnVXMJ8Ak4PMgQv7",
#     openai_organization=OPENAI_ORG_ID,
#     chatgpt_config={
#             "model": "gpt-3.5-turbo",
#             "temperature": 0.7,
#     },
# )


@task(
    container_image=aws_image,
    secret_requests=[
        Secret(
            group=OPENAI_SECRET_GROUP,
            key=OPENAI_SECRET_KEY,
            mount_requirement=Secret.MountType.FILE
        )
    ]
)
def call_gpt(prompt: str) -> str:
    secret_val = flytekit.current_context().secrets.get(OPENAI_SECRET_GROUP, OPENAI_SECRET_KEY)
    secret_data = json.loads(secret_val)
    openai_secret_key = secret_data["openai_secret_key"]

    client = OpenAI(
        # This is the default and can be omitted
        api_key=openai_secret_key,
    )

    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": prompt,
            }
        ],
        model="gpt-3.5-turbo",
    )

    return chat_completion.choices[0].message.content


@workflow
def query_logs_wf() -> str:
    log_error_raw = query_logs()
    prompt = preprocess_task(input_error=log_error_raw)
    response = call_gpt(prompt=prompt)
    # response = gpt_task(message=prompt)
    return prompt



