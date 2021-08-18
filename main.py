import json
import base64

from models import AdAttribution

def main(request):
    request_json = request.get_json(silent=True)
    message = request_json["message"]
    data_bytes = message["data"]
    data = json.loads(base64.b64decode(data_bytes).decode("utf-8"))
    print(data)

    if 'broadcast' in data:
        results = None
    elif 'table' in data:
        job = AdAttribution.factory(
            data['table'],
            data.get('start'),
            data.get('end'),
        )
        results = job.run()
    
    responses = {
        "pipelines": "Hyros",
        "results": results,
    }
    print(responses)
    return responses
