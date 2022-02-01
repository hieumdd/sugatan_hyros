import os
import json
import csv
import time
from datetime import datetime, timedelta

from seleniumwire import webdriver
from seleniumwire.request import Request
from selenium.webdriver.chrome.options import Options
import dateparser
import requests


CHROME_OPTIONS = Options()
if os.getenv("PYTHON_ENV") == "prod":
    CHROME_OPTIONS.add_argument("--headless")
CHROME_OPTIONS.add_argument("--no-sandbox")
CHROME_OPTIONS.add_argument("--window-size=1920,1080")
CHROME_OPTIONS.add_argument("--disable-gpu")
CHROME_OPTIONS.add_argument("--disable-dev-shm-usage")
CHROME_OPTIONS.add_argument(
    f"""
    user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) 
    AppleWebKit/537.36 (KHTML, like Gecko)
    Chrome/87.0.4280.141 Safari/537.36
    """
)
CHROME_OPTIONS.add_experimental_option(
    "prefs",
    {
        "download.default_directory": "/tmp",
    },
)

NOW = datetime.utcnow()


def get_report_request(user: str, pwd: str) -> Request:
    """Get & intercept CSV request from their FE to BE

    Returns:
        str: getReport request URL
    """

    if os.getenv("PYTHON_ENV") == "dev":
        driver = webdriver.Chrome("./chromedriver", options=CHROME_OPTIONS)
    else:
        driver = webdriver.Chrome(options=CHROME_OPTIONS)
    driver.implicitly_wait(20)

    # Navtigate to URL
    driver.get("https://app.hyros.com/?avoidRedirect=true#/login")
    print("Home")

    # Input username & pwd
    username = driver.find_element_by_name("username")
    username.send_keys(user)
    password = driver.find_element_by_name("password")
    password.send_keys(pwd)
    print("Typed Login")

    # Click login
    login_button = driver.find_element_by_xpath(
        '//*[@id="page-top"]/div[3]/div/div[2]/div/form/div[3]/button'
    )
    driver.execute_script("arguments[0].click();", login_button)
    print("Login")

    # Wait for login
    time.sleep(5)

    # Navigate to Report
    driver.get("https://app.hyros.com/#/mh/reporting")
    print("Navigate to Report")

    # Click generate report
    time.sleep(5)
    last_click = driver.find_element_by_xpath(
        "/html/body/div[3]/div/div/div[1]/report-selection-directive/div[3]/div/div/div[1]"
    )
    driver.execute_script("arguments[0].click();", last_click)
    print("Generate Last Click Report")

    generate = driver.find_element_by_xpath(
        "/html/body/div[3]/div/div/div[1]/report-directive/div/div[2]/section/div[3]/button[1]"
    )
    driver.execute_script("arguments[0].click();", generate)
    time.sleep(10)

    xhr_requests = [
        request
        for request in driver.requests
        if request.response
        and "sourceboardV2" in request.url
        and "stats" in request.url
    ]
    driver.quit()
    return xhr_requests[0]


def modifiy_request(request: Request, id: str) -> tuple[str, dict, dict]:
    end = datetime.utcnow()
    start = end - timedelta(days=30)
    body = json.loads(request.body.decode())
    modified_body = {
        **body,
        "start": start.strftime("%d-%m-%Y"),
        "end": end.strftime("%d-%m-%Y"),
        "customerIds": [id],
        "timeGroupingOption": "DAY",
        "groupAConfiguration": {
            **body["groupAConfiguration"],
            "start": start.strftime("%d-%m-%Y"),
            "end": end.strftime("%d-%m-%Y"),
        }
        if body.get("groupAConfiguration", {})
        else {},
        "groupBConfiguration": {
            **body["groupBConfiguration"],
            "start": start.strftime("%d-%m-%Y"),
            "end": end.strftime("%d-%m-%Y"),
        }
        if body.get("groupBConfiguration", {})
        else {},
    }
    return request.url, request.headers, modified_body


def post_modified_request(
    session: requests.Session,
    url: str,
    headers: dict,
    body: dict,
) -> str:
    with session.post(url, headers=headers, json=body) as r:
        res = r.json()
    return res["key"]


def poll_request(
    session: requests.Session,
    url: str,
    headers: dict,
    key: str,
    attempt: int = 0,
) -> str:
    with session.get(
        url.replace("stats", "poll-current-step"),
        headers=headers,
        params={
            "key": key,
        },
    ) as r:
        res = r.json()
    if attempt < 100:
        return (
            key
            if res["reportReady"]
            else poll_request(session, url, headers, key, attempt + 1)
        )
    else:
        raise Exception(attempt)


def get_csv(session: requests.Session, url: str, headers: dict, key: str) -> list[dict]:
    with session.post(
        f"{url.replace('stats', 'export-report')}/{key}",
        headers=headers,
        json={
            "columns": [
                "AOV",
                "AD_ID",
                "BUDGET",
                "CALLS",
                "CLICKS",
                "COST",
                "COST_PER_CALL",
                "COST_PER_SALE",
                "COST_PER_LEAD",
                "COST_PER_NEW_LEAD",
                "COST_PER_UNIQUE_SALE",
                "LEADS",
                "NEW_LEADS",
                "PROFIT",
                "RECURRING_REVENUE",
                "REFUND",
                "REPORTED",
                "REPORTED_VS_REVENUE",
                "REVENUE",
                "ROI",
                "ROAS",
                "SALES",
                "STATUS",
                "TOTAL_REVENUE",
                "UNIQUE_SALES",
            ],
            "excludeInactive": True,
            "groupOption": "SOURCE_LINK",
            "reportType": "DURING",
        },
    ) as r:
        res = r.content
    decoded_content = res.decode("utf-8")
    csv_lines = decoded_content.splitlines()
    cr = csv.DictReader(
        csv_lines[2:],
        fieldnames=csv_lines[0].split(","),
    )
    return [row for row in cr]


def transform(rows: list[dict], id: str) -> list[dict]:
    """Transform the data to our liking. Transform column to their correct data representation.
    **THIS FUNCTION IS HARD-CODING

    Args:
        rows (list): List of row

    Returns:
        list: List of row transformed
    """

    def transform_date(i):
        current_year = NOW.year
        current_month = NOW.month
        parsed_date = dateparser.parse(i)
        parsed_date = (
            parsed_date.replace(year=current_year - 1)
            if (current_month == 1 and parsed_date.month != 1)
            else parsed_date.replace(year=current_year)
        )
        return parsed_date.strftime("%Y-%m-%d")

    def transform_null(i):
        return i if i != "-" else None

    def safe_float(i):
        return round(float(i), 6) if i else None

    return [
        {
            "source": transform_date(row["Source"]),
            "clicks": safe_float(row["Clicks"]),
            "cost": safe_float(row["Cost"]),
            "total_revenue": safe_float(row["Total Revenue"]),
            "revenue": safe_float(row["Revenue"]),
            "recurring_revenue": safe_float(row["Recurring revenue"]),
            "profit": safe_float(row["Profit"]),
            "reported": safe_float(row["Reported"]),
            "reported_vs_revenue": safe_float(row["Reported VS Revenue"]),
            "sales": safe_float(row["Sales"]),
            "roi": safe_float(safe_float(row["ROI"]) / 100),
            "roas": safe_float(safe_float(row["ROAS"]) / 100),
            "calls": safe_float(row["Calls"]),
            "refund": safe_float(row["Refund"]),
            "cost_per_sale": safe_float(row["Cost per sale"]),
            "cost_per_call": safe_float(row["Cost per call"]),
            "leads": safe_float(row["Leads"]),
            "new_leads": safe_float(row["New Leads"]),
            "cost_per_lead": safe_float(row["Cost per lead"]),
            "cost_per_new_lead": safe_float(row["Cost per new lead"]),
            "cost_per_unique_sale": safe_float(row["Cost per unique sale"]),
            "unique_sales": safe_float(row["Unique Sales"]),
            "average_over_value": safe_float(row["Average Order Value"]),
            "account": id,
            "_batched_at": NOW.isoformat(timespec="seconds"),
        }
        for row in [{k: transform_null(v) for k, v in row.items()} for row in rows]
        if row["Source"] != "Total"
    ]


def get_data(
    session: requests.Session,
    intercepted_request: Request,
    id: str,
) -> list[dict]:
    url, modified_headers, modified_body = modifiy_request(intercepted_request, id)
    report_key = post_modified_request(
        session,
        url,
        modified_headers,
        modified_body,
    )
    polled_key = poll_request(session, url, modified_headers, report_key)
    data = get_csv(session, url, modified_headers, polled_key)
    return transform(data, id)


def get(client):
    intercepted_request = get_report_request(client["user"], client["pwd"])
    with requests.Session() as session:
        data = [
            get_data(session, intercepted_request, account["id"])
            for account in client["accounts"]
        ]
    return [i for j in data for i in j]
