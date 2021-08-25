from .utils import process

START = "2021-08-01"
END = "2021-08-25"


def test_facebook():
    data = {
        "table": "facebook",
        "start": START,
        "end": END,
    }
    process(data)

def test_google():
    data = {
        "table": "google",
        "start": START,
        "end": END,
    }
    process(data)
