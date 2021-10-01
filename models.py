import os
import json
from datetime import datetime, timedelta
from abc import ABCMeta, abstractmethod

import requests
from google.cloud import bigquery

NOW = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
DATE_FORMAT = "%Y-%m-%d"

BASE_URL = "https://api.hyros.com/v1/api/v1.0"
HEADERS = {
    "Content-Type": "application/json",
    "API-key": os.getenv("API_KEY"),
}

BQ_CLIENT = bigquery.Client()
DATASET = "SBLA_Hyros"


class AdAttribution(metaclass=ABCMeta):
    keys = {
        "p_key": ["id", "start_time", "end_time"],
        "incre_key": "_batched_at",
        "time_key": "start_time",
    }
    schema = [
        {"name": "id", "type": "STRING"},
        {"name": "sales", "type": "FLOAT"},
        {"name": "calls", "type": "FLOAT"},
        {"name": "unique_sales", "type": "FLOAT"},
        {"name": "refund", "type": "FLOAT"},
        {"name": "revenue", "type": "FLOAT"},
        {"name": "recurring_revenue", "type": "FLOAT"},
        {"name": "total_revenue", "type": "FLOAT"},
        {"name": "start_time", "type": "TIMESTAMP"},
        {"name": "end_time", "type": "TIMESTAMP"},
        {"name": "_batched_at", "type": "TIMESTAMP"},
    ]

    @property
    @abstractmethod
    def level(self):
        pass

    @property
    @abstractmethod
    def id_query(self):
        pass

    def __init__(self, start, end):
        self.table = self.__class__.__name__
        self.start, self.end = (
            [datetime.strptime(i, DATE_FORMAT) for i in (start, end)]
            if start and end
            else [NOW - timedelta(days=30), NOW]
        )

    def _get(self, session):
        ids_results = BQ_CLIENT.query(self.id_query).result()
        ids = [dict(row.items())["id"] for row in ids_results]
        return self._get_one(session, ids, self.start, self.end)

    def _get_one(self, session, ids, date, date_end):
        start = date.isoformat(timespec="seconds")
        end = (date + timedelta(days=1)).isoformat(timespec="seconds")
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
            headers=HEADERS,
        ) as r:
            r.raise_for_status()
            res = r.json()
        results = [
            {
                **result,
                "start_time": start,
                "end_time": end,
            }
            for result in res["result"]
        ]
        return (
            results + self._get_one(session, ids, date + timedelta(days=1), date_end)
            if date < date_end
            else results
        )

    def _transform(self, rows):
        return [
            {
                **row,
                "_batched_at": NOW.isoformat(timespec="seconds"),
            }
            for row in rows
        ]

    def _load(self, rows):
        output_rows = (
            BQ_CLIENT.load_table_from_json(
                rows,
                f"{DATASET}.AdAttribution_{self.table}",
                job_config=bigquery.LoadJobConfig(
                    create_disposition="CREATE_IF_NEEDED",
                    write_disposition="WRITE_APPEND",
                    schema=self.schema,
                ),
            )
            .result()
            .output_rows
        )
        self._update()
        return output_rows

    def _update(self):
        query = f"""
        CREATE OR REPLACE TABLE `{DATASET}`.`AdAttribution_{self.table}` AS
        SELECT * EXCEPT (`row_num`) FROM
        (
            SELECT
                *,
                ROW_NUMBER() over (
                    PARTITION BY {','.join(self.keys['p_key'])}
                    ORDER BY {self.keys['incre_key']} DESC) AS `row_num`
                FROM
                    `{DATASET}`.`AdAttribution_{self.table}`
            )
        WHERE
            `row_num` = 1
        """
        BQ_CLIENT.query(query).result()

    def run(self):
        with requests.Session() as session:
            rows = self._get(session)
        responses = {
            "table": self.table,
            "start": self.start.strftime(DATE_FORMAT),
            "end": self.end.strftime(DATE_FORMAT),
            "num_processed": len(rows),
        }
        if len(rows) > 0:
            rows = self._transform(rows)
            responses["output_rows"] = self._load(rows)
        return responses


class FacebookAdset(AdAttribution):
    level = "facebook_adset"
    id_query = """
    SELECT
        DISTINCT `ad_group_id` AS `id`
    FROM
        `SBLA_7c1v`.`FBADS_AD_*`
    WHERE
        creative_url_tags = 'fbc_id={{adset.id}}&h_ad_id={{ad.id}}'
        AND `date` >= date_add(CURRENT_DATE(), INTERVAL -30 DAY)
        AND `cost` > 0
        AND `impressions` > 0
    """


class GoogleCampaign(AdAttribution):
    level = "google_campaign"
    id_query = """
        SELECT
        DISTINCT s.`CampaignId` AS `id`
    FROM
        `SBLA_GAds`.`AdGroup_4175347744` s
        INNER JOIN `SBLA_GAds`.`AdGroupStats_4175347744` d
        ON s.`CampaignId` = d.`CampaignId`
        AND s.`AdGroupId` = d.`AdGroupId`
    WHERE
        `TrackingUrlTemplate` = '{lpurl}?gc_id={campaignid}&h_ad_id={creative}'
        AND `Date` >= date_add(CURRENT_DATE(), INTERVAL -2 DAY)
        AND `Cost` > 0
        AND `Clicks` > 0
    """
