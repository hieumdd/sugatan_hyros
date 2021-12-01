from libs.hyros import get
from libs.bigquery import load
from controller.tasks import AuthHyrosClient


def run(client: AuthHyrosClient) -> dict:
    rows = get(client)
    return {
        "num_processed": len(rows),
        "output_rows": load(f"{client['name']}_Hyros", rows),
    }
