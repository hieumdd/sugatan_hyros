import os
import json
import csv
import time
import re
from datetime import datetime, timedelta

from seleniumwire import webdriver
from selenium.webdriver.chrome.options import Options
import dateparser
import requests
from google.cloud import bigquery

from controller.pipelines import run
from controller.tasks import create_tasks


def main(request):
    data = request.get_json()
    print(data)

    if "tasks" in data:
        response = create_tasks()
    else:
        response = run(data)
    print(response)
    return response
