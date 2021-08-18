from .utils import process


def test_facebook():
    data = {
        "table": "facebook",
    }
    process(data)


def test_google():
    data = {
        "table": "google",
    }
    process(data)
