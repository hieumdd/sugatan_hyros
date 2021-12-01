from google.cloud import bigquery

BQ_CLIENT = bigquery.Client()
TABLE = "AdAttributionScrape"


def load(dataset: str, rows: list[dict]) -> int:
    """Load data to stage table

    Args:
        rows (list): List of row

    Returns:
        int: Output rows
    """
    output_rows = (
        BQ_CLIENT.load_table_from_json(
            rows,
            f"{dataset}._stage_{TABLE}",
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
    update(dataset)
    return output_rows


def update(dataset: str) -> None:
    BQ_CLIENT.query(
        f"""
    CREATE OR REPLACE TABLE {dataset}.{TABLE} AS
    SELECT * EXCEPT (row_num)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY source, account ORDER BY _batched_at DESC)
            AS row_num
        FROM {dataset}._stage_{TABLE}
    ) WHERE row_num = 1"""
    ).result()
