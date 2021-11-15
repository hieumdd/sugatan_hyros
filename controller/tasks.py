from typing import TypedDict
import os
import json
import uuid

from google.cloud import tasks_v2, secretmanager  # type: ignore


TASKS_CLIENT = tasks_v2.CloudTasksClient()
SECRET_CLIENT = secretmanager.SecretManagerServiceClient()


def get_secret(secret_id: str, version_id: int = 1) -> str:
    return SECRET_CLIENT.access_secret_version(
        request={
            "name": f"projects/{os.getenv('PROJECT_ID')}/secrets/{secret_id}/versions/{version_id}"
        }
    ).payload.data.decode("UTF-8")


class Account(TypedDict):
    name: str
    id: str


class HyrosClient(TypedDict):
    name: str
    user: tuple[str, int]
    pwd: tuple[str, int]
    accounts: list[Account]


class AuthHyrosClient(TypedDict):
    name: str
    user: str
    pwd: str
    accounts: list[Account]


CLIENTS: list[AuthHyrosClient] = [
    {
        "name": "SBLA",
        "user": get_secret("hyros_SBLA_user"),
        "pwd": get_secret("hyros_SBLA_pwd"),
        "accounts": [
            {
                "name": "SBLA_Beauty",
                "id": "4175347744",
            },
            {
                "name": "SBLA_AD",
                "id": "568194437023661",
            },
            {
                "name": "TikTok_SBLA",
                "id": "6984918190983741442",
            },
        ],
    },
    {
        "name": "SniffAndBark",
        "user": get_secret("hyros_SniffAndBark_user"),
        "pwd": get_secret("hyros_SniffAndBark_pwd"),
        "accounts": [
            {
                "name": "#1 - sb",
                "id": "494981854585320",
            },
            {
                "name": "#2 - sb",
                "id": "2488643648044303",
            },
            {
                "name": "#3 - sb",
                "id": "713354499505308",
            },
            {
                "name": "Hyros",
                "id": "2390622652",
            },
        ],
    },
]

CLOUD_TASKS_PATH = (os.getenv("PROJECT_ID", ""), "us-central1", "hyros")
PARENT = TASKS_CLIENT.queue_path(*CLOUD_TASKS_PATH)


def create_tasks() -> dict:
    payloads = [
        {
            "name": f"{client['name']}-{uuid.uuid4()}",
            "payload": {
                "client": client,
            },
        }
        for client in CLIENTS
    ]
    tasks = [
        {
            "name": TASKS_CLIENT.task_path(
                *CLOUD_TASKS_PATH,
                task=str(payload["name"]),
            ),
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": os.getenv("PUBLIC_URL"),
                "oidc_token": {
                    "service_account_email": os.getenv("GCP_SA"),
                },
                "headers": {
                    "Content-type": "application/json",
                },
                "body": json.dumps(payload["payload"]).encode(),
            },
        }
        for payload in payloads
    ]
    responses = [
        TASKS_CLIENT.create_task(
            request={
                "parent": PARENT,
                "task": task,
            }
        )
        for task in tasks
    ]
    return {
        "tasks": len(responses),
    }
