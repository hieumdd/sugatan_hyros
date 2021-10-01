import pytest
from unittest.mock import Mock

from main import main

START, END = ("2021-09-01", "2021-09-10")


@pytest.mark.parametrize(
    "table",
    [
        "FacebookAdset",
        "GoogleCampaign",
    ],
)
@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, None),
        (START, END),
    ],
    ids=[
        "auto",
        "manual",
    ],
)
def test_units(table, start, end):
    data = {
        "table": table,
        "start": start,
        "end": end,
    }
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)["results"]
    for i in res:
        assert i["num_processed"] >= 0
        if i["num_processed"] > 0:
            assert i["num_processed"] == i["output_rows"]


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, None),
        (START, END),
    ],
    ids=[
        "auto",
        "manual",
    ],
)
def test_tasks(start, end):
    data = {
        "tasks": "hyros",
        "start": start,
        "end": end,
    }
    req = Mock(get_json=Mock(return_value=data), args=data)
    res = main(req)["results"]
    for i in res:
        assert i["num_processed"] >= 0
        if i["num_processed"] > 0:
            assert i["num_processed"] == i["output_rows"]
