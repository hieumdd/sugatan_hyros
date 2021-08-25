import os
import json
from datetime import datetime, timedelta
import asyncio
from abc import ABCMeta, abstractmethod

import aiohttp
from google.cloud import bigquery
import jinja2

NOW = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
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
        self.start, self.end = self.get_time_range(start, end)

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

    def get_time_range(self, _start, _end):
        if _start and _end:
            start, end = tuple(
                datetime.strptime(i, DATE_FORMAT) for i in (_start, _end)
            )
        else:
            end = NOW
            template = TEMPLATE_ENV.get_template("read_max_incremental.sql.j2")
            rendered_query = template.render(
                dataset=DATASET,
                table=f"AdAttribution_{self.table}",
                incre_key=self.keys["time_key"],
            )
            rows = BQ_CLIENT.query(rendered_query).result()
            row = [dict(row.items()) for row in rows][0]
            start = row["incre"].replace(tzinfo=None) - timedelta(days=10)
        return start, end

    def get_ids(self):
        template = TEMPLATE_ENV.get_template(f"{self.id_template}.sql.j2")
        rendered_query = template.render()
        rows = BQ_CLIENT.query(rendered_query).result()
        rows = [dict(row.items()) for row in rows]
        return [row["id"] for row in rows]

    def get(self):
        dt_range = []
        _start = self.start
        while _start < self.end:
            dt_range.append(_start)
            _start += timedelta(hours=1)
        rows = asyncio.run(self._get(dt_range))
        return [i for j in rows for i in j]

    async def _get(self, dt_range):
        connector = aiohttp.TCPConnector(limit=50)
        timeout = aiohttp.ClientTimeout(total=540)
        ids = self.get_ids()
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
        ) as sessions:
            tasks = [
                asyncio.create_task(self._get_one(sessions, ids, dt)) for dt in dt_range
            ]
            return await asyncio.gather(*tasks)

    async def _get_one(self, sessions, ids, dt):
        start = dt.isoformat()
        end = (dt + timedelta(hours=1)).isoformat()
        url = f"{BASE_URL}/attribution"
        params = {
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
        }
        try:
            async with sessions.get(url, params=params, headers=HEADERS) as r:
                r.raise_for_status()
                res = await r.json()
            results = res["result"]
            results = [
                {
                    **result,
                    "start_time": start,
                    "end_time": end,
                }
                for result in results
            ]
        except Exception as e:
            print(e)
            raise e
        return results

    def transform(self, rows):
        rows = [
            {
                **row,
                "_batched_at": NOW.isoformat(timespec="seconds"),
            }
            for row in rows
        ]
        return rows

    def load(self, rows):
        return BQ_CLIENT.load_table_from_json(
            rows,
            f"{DATASET}._stage_AdAttribution_{self.table}",
            job_config=bigquery.LoadJobConfig(
                create_disposition="CREATE_IF_NEEDED",
                write_disposition="WRITE_APPEND",
                schema=self.schema,
            ),
        ).result()

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
        rows = self.get()
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
