import os
import json
from datetime import datetime, timedelta
import asyncio
from abc import ABCMeta, abstractmethod

import aiohttp
import requests
from google.cloud import bigquery
import jinja2

NOW = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
DATE_FORMAT = "%Y-%m-%d"

BASE_URL = "https://api.hyros.com/v1/api/v1.0"
HEADERS = {
    "Content-Type": "application/json",
    "API-key": os.getenv("API_KEY"),
}

BQ_CLIENT = bigquery.Client()
DATASET = "SBLA_Hyros"

TEMPLATE_LOADER = jinja2.FileSystemLoader("./templates")
TEMPLATE_ENV = jinja2.Environment(loader=TEMPLATE_LOADER)


class AdAttribution(metaclass=ABCMeta):
    def __init__(self, start, end):
        self.keys, self.schema = self.get_config()
        self.start, self.end = self._get_date_range(start, end)

    @staticmethod
    def factory(table, start, end):
        args = (start, end)
        if table == "facebook":
            return FacebooAdAttribution(*args)
        elif table == "google":
            return GoogleAdAttribution(*args)
        else:
            raise NotImplementedError(table)

    @property
    @abstractmethod
    def id_template(self):
        pass

    @property
    @abstractmethod
    def level(self):
        pass

    @property
    @abstractmethod
    def table(self):
        pass

    def get_config(self):
        with open("configs/AdAttribution.json", "r") as f:
            config = json.load(f)
        return config["keys"], config["schema"]

    def _get_date_range(self, start, end):
        if start and end:
            if isinstance(start, datetime):
                _start, _end = start, end
            else:
                _start, _end = [datetime.strptime(i, DATE_FORMAT) for i in (start, end)]
        else:
            _start, _end = [NOW - timedelta(days=30), NOW]
        return _start, _end

    def get_ids(self):
        template = TEMPLATE_ENV.get_template(f"{self.id_template}.sql.j2")
        rendered_query = template.render()
        rows = BQ_CLIENT.query(rendered_query).result()
        rows = [dict(row.items()) for row in rows]
        return [row["id"] for row in rows]

    def get(self, session):
        date_range = [
            self.start + timedelta(i)
            for i in range(int((self.end - self.start).days) + 1)
        ]
        ids = self.get_ids()
        rows = self._get_one(session, ids, self.start, self.end)
        rows
        return rows
    
    def _get_one(self, session, ids, date, date_end):
        start = date.isoformat(timespec='seconds')
        end = (date + timedelta(days=1)).isoformat(timespec='seconds')
        with session.get(
            f"{BASE_URL}/attribution",
            params={
            "attributionModel": "last_click",
            "startDate": start,
            "endDate": end,
            "level": self.level,
            "fields": ",".join(
                [
                    "sales",
                    "revenue",
                    "calls",
                    "total_revenue",
                    "recurring_revenue",
                    "refund",
                    "unique_sales",
                ]
            ),
            "ids": ",".join([str(i) for i in ids]),
            "currency": "usd",
            "dayOfAttribution": json.dumps(False),
        },
        headers=HEADERS
        ) as r:
            r.raise_for_status()
            res = r.json()
        results = [
                {
                    **result,
                    "start_time": start,
                    "end_time": end,
                }
                for result in res['result']
            ]
        return results + self._get_one(session, ids, date + timedelta(days=1), date_end) if date < date_end else results

    def _transform(self, rows):
        return [
            {
                **row,
                "_batched_at": NOW.isoformat(timespec="seconds"),
            }
            for row in rows
        ]

    def _load(self, rows):
        output_rows = BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_AdAttribution_{self.table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=self.schema,
            ),
        ).result().output_rows

    def update(self):
        template = TEMPLATE_ENV.get_template("update_from_stage.sql.j2")
        rendered_query = template.render(
            dataset=DATASET,
            table=f"AdAttribution_{self.table}",
            p_key=",".join(self.keys["p_key"]),
            incre_key=self.keys["incre_key"],
        )
        BQ_CLIENT.query(rendered_query)

    def run(self):
        with requests.Session() as session:
            rows = self.get(session)
        responses = {
            "table": self.table,
            "start": self.start,
            "end": self.end,
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self.transform(rows)
            loads = self.load(rows)
            self.update()
            responses["output_rows"] = loads.output_rows
        return responses


class FacebooAdAttribution(AdAttribution):
    def __init__(self, start, end):
        super().__init__(start, end)

    @property
    def id_template(self):
        return "get_facebook_ids"

    @property
    def level(self):
        return "facebook_adset"

    @property
    def table(self):
        return "FacebookAdSet"


class GoogleAdAttribution(AdAttribution):
    def __init__(self, start, end):
        super().__init__(start, end)

    @property
    def id_template(self):
        return "get_google_ids"

    @property
    def level(self):
        return "google_campaign"

    @property
    def table(self):
        return "GoogleCampaign"
