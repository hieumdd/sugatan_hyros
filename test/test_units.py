from unittest.mock import Mock

import pytest

from main import main
from controller.tasks import CLIENTS


@pytest.mark.parametrize(
    "client",
    CLIENTS,
    ids=[i["name"] for i in CLIENTS],
)
def test_pipelines(client):
    data = client
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)
    assert res["num_processed"] >= 0
    if res["num_processed"] > 0:
        assert res["num_processed"] == res["output_rows"]
