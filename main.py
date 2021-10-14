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

HYROS_ID = 180016

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

BQ_CLIENT = bigquery.Client()
DATASET = "SBLA_Hyros"
TABLE = "AdAttributionScrape"

NOW = datetime.utcnow()

ACCOUNTS = [
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
]


def get_report_request():
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
    username.send_keys(os.getenv("USERNAME"))
    password = driver.find_element_by_name("password")
    password.send_keys(os.getenv("HYROS_PWD"))
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
        "/html/body/div[3]/div/div/div[1]/report-selection-directive/div[3]/div/div[1]/div[2]/p[1]"
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


def modifiy_request(request, id):
    end = datetime.utcnow()
    start = end - timedelta(days=30)
    headers = request.headers
    body = json.loads(request.body.decode())
    modified_body = {
        "start": start.strftime("%d-%m-%Y"),
        "end": end.strftime("%d-%m-%Y"),
        "customerIds": [id],
        "timeGroupingOption": "DAY",
        "groupAConfiguration": {
            "start": start.strftime("%d-%m-%Y"),
            "end": end.strftime("%d-%m-%Y"),
            "productTags": body["groupAConfiguration"].get("productTags"),
            "productCategories": body["groupAConfiguration"].get("productCategories"),
            "leadTags": body["groupAConfiguration"].get("leadTags"),
            "notLeadTags": body["groupAConfiguration"].get("notLeadTags"),
            "ignoreRecurringSales": body["groupAConfiguration"].get(
                "ignoreRecurringSales"
            ),
            "excludeHardCosts": body["groupAConfiguration"].get("excludeHardCosts"),
            "productCategoryIds": body["groupAConfiguration"].get("productCategoryIds"),
        }
        if body.get("groupAConfiguration", {})
        else {},
        "groupBConfiguration": {
            "start": start.strftime("%d-%m-%Y"),
            "end": end.strftime("%d-%m-%Y"),
            "productTags": body["groupBConfiguration"].get("productTags"),
            "productCategories": body["groupBConfiguration"].get("productCategories"),
            "leadTags": body["groupBConfiguration"].get("leadTags"),
            "notLeadTags": body["groupBConfiguration"].get("notLeadTags"),
            "ignoreRecurringSales": body["groupBConfiguration"].get(
                "ignoreRecurringSales"
            ),
            "excludeHardCosts": body["groupBConfiguration"].get("excludeHardCosts"),
            "productCategoryIds": body["groupBConfiguration"].get("productCategoryIds"),
        }
        if body.get("groupBConfiguration", {})
        else {},
        "days": body.get("days"),
        "sourceLinkName": body.get("sourceLinkName"),
        "leadTags": body.get("leadTags"),
        "notLeadTags": body.get("notLeadTags"),
        "productTags": body.get("productTags"),
        "percentage": body.get("percentage"),
        "firstSourceLinkPercentage": body.get("firstSourceLinkPercentage"),
        "dayOfAttribution": body.get("dayOfAttribution"),
        "filterNoSale": body.get("filterNoSale"),
        "scientificDaysRange,": body.get("scientificDaysRange,"),
        "emailSourceOptions": body.get("emailSourceOptions"),
        "optimizeReport": body.get("optimizeReport"),
        "excludeHardCosts": body.get("excludeHardCosts"),
        "ignoreRecurringSales": body.get("ignoreRecurringSales"),
        "groups": body.get("groups"),
        "compareTotalSales": body.get("compareTotalSales"),
        "compareTotalSalesBy": body.get("compareTotalSalesBy"),
        "origin": body.get("origin"),
        "financialStatsType": body.get("financialStatsType"),
        "sourceCategoryIds": body.get("sourceCategoryIds"),
        "trafficSourceIds": body.get("trafficSourceIds"),
        "goalIds": body.get("goalIds"),
        "productCategoryIds": body.get("productCategoryIds"),
        "sourceLinkFilterCriteria": body.get("sourceLinkFilterCriteria"),
        "qReportName": body.get("qReportName"),
        "financialDashboardType": body.get("financialDashboardType"),
        "segment": body.get("segment"),
    }
    modified_headers = {
        "accept": "application/json",
        "userloginid": headers["userloginid"],
        "authtoken": headers["authtoken"],
    }
    return request.url, modified_headers, modified_body


def post_modified_request(session, url, headers, body):
    with session.post(
        url,
        headers=headers,
        json=body,
    ) as r:
        res = r.json()
    return res["key"]


def poll_request(session, url, headers, key, attempt=0):
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


def get_csv(session, url, headers, key):
    with session.post(
        f"{url.replace('stats', 'export-report')}/{key}",
        headers={
            **headers,
            "excludeinactive": "true",
        },
        json=[
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
    ) as r:
        res = r.content
    decoded_content = res.decode("utf-8")
    csv_lines = decoded_content.splitlines()
    cr = csv.DictReader(
        csv_lines[2:],
        fieldnames=csv_lines[0].split(","),
    )
    return [row for row in cr]


def transform(rows, id):
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

    return [
        {
            "source": transform_date(row["Source"]),
            "clicks": round(float(row["Clicks"]), 6),
            "cost": round(float(row["Cost"]), 6),
            "total_revenue": round(float(row["Total Revenue"]), 6),
            "revenue": round(float(row["Revenue"]), 6),
            "recurring_revenue": round(float(row["Recurring revenue"]), 6),
            "profit": round(float(row["Profit"]), 6),
            "reported": round(float(row["Reported"]), 6),
            "reported_vs_revenue": round(float(row["Reported VS Revenue"]), 6),
            "sales": round(float(row["Sales"]), 6),
            "roi": round(float(row["ROI"]) / 100, 6),
            "roas": round(float(row["ROAS"]) / 100, 6),
            "calls": round(float(row["Calls"]), 6),
            "refund": round(float(row["Refund"]), 6),
            "cost_per_sale": round(float(row["Cost per sale"]), 6),
            "cost_per_call": round(float(row["Cost per call"]), 6),
            "leads": round(float(row["Leads"]), 6),
            "new_leads": round(float(row["New Leads"]), 6),
            "cost_per_lead": round(float(row["Cost per lead"]), 6),
            "cost_per_new_lead": round(float(row["Cost per new lead"]), 6),
            "cost_per_unique_sale": round(float(row["Cost per unique sale"]), 6),
            "unique_sales": round(float(row["Unique Sales"]), 6),
            "average_over_value": round(float(row["Average Order Value"]), 6),
            "account": id,
            "_batched_at": NOW.isoformat(timespec="seconds"),
        }
        for row in rows
    ]


def get_data(session, intercepted_request, id):
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


def load(rows):
    """Load data to stage table

    Args:
        rows (list): List of row

    Returns:
        int: Output rows
    """
    output_rows = (
        BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_{TABLE}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=[
                    {"name": "source", "type": "DATE"},
                    {"name": "clicks", "type": "NUMERIC"},
                    {"name": "cost", "type": "NUMERIC"},
                    {"name": "total_revenue", "type": "NUMERIC"},
                    {"name": "revenue", "type": "NUMERIC"},
                    {"name": "recurring_revenue", "type": "NUMERIC"},
                    {"name": "profit", "type": "NUMERIC"},
                    {"name": "reported", "type": "NUMERIC"},
                    {"name": "reported_vs_revenue", "type": "NUMERIC"},
                    {"name": "sales", "type": "NUMERIC"},
                    {"name": "roi", "type": "NUMERIC"},
                    {"name": "roas", "type": "NUMERIC"},
                    {"name": "calls", "type": "NUMERIC"},
                    {"name": "refund", "type": "NUMERIC"},
                    {"name": "cost_per_sale", "type": "NUMERIC"},
                    {"name": "cost_per_call", "type": "NUMERIC"},
                    {"name": "leads", "type": "NUMERIC"},
                    {"name": "new_leads", "type": "NUMERIC"},
                    {"name": "cost_per_lead", "type": "NUMERIC"},
                    {"name": "cost_per_new_lead", "type": "NUMERIC"},
                    {"name": "cost_per_unique_sale", "type": "NUMERIC"},
                    {"name": "unique_sales", "type": "NUMERIC"},
                    {"name": "average_over_value", "type": "NUMERIC"},
                    {"name": "account", "type": "STRING"},
                    {"name": "_batched_at", "type": "TIMESTAMP"},
                ],
            ),
        )
        .result()
        .output_rows
    )
    update()
    return output_rows


def update():
    """Update the main table"""

    query = f"""
    CREATE OR REPLACE TABLE {DATASET}.{TABLE} AS
    SELECT * EXCEPT (row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source, account ORDER BY _batched_at)
            AS row_num
        FROM {DATASET}._stage_{TABLE}
    ) WHERE row_num = 1"""
    BQ_CLIENT.query(query).result()


def main(request):
    intercepted_request = get_report_request()
    with requests.Session() as session:
        data = [
            get_data(session, intercepted_request, account["id"])
            for account in ACCOUNTS
        ]
    rows = [i for j in data for i in j]
    response = {
        "table": TABLE,
        "num_processed": len(rows),
    }
    response["output_rows"] = load(rows)
    print(response)
    return response
