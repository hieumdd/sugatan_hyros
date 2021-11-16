from unittest.mock import Mock

import pytest

from main import main
from controller.tasks import CLIENTS


def run(data: dict) -> dict:
    return main(Mock(get_json=Mock(return_value=data), args=data))


@pytest.mark.parametrize(
    "client",
    CLIENTS,
    ids=[i["name"] for i in CLIENTS],
)
def test_pipelines(client: dict):
    res = run(client)
    assert res["num_processed"] >= 0
    if res["num_processed"] > 0:
        assert res["num_processed"] == res["output_rows"]


def test_task():
    res = run(
        {
            "tasks": "hyros",
        }
    )
    assert res["tasks"] > 0
