from models import FacebookAdset, GoogleCampaign


def main(request):
    data = request.get_json(silent=True)
    print(data)

    if "tasks" in data:
        results = [
            i(
                data.get("start"),
                data.get("end"),
            ).run()
            for i in [FacebookAdset, GoogleCampaign]
        ]
    elif "table" in data:
        if data["table"] == "FacebookAdset":
            job = FacebookAdset
        elif data["table"] == "GoogleCampaign":
            job = GoogleCampaign
        else:
            raise ValueError(data["table"])
        results = [
            job(
                data.get("start"),
                data.get("end"),
            ).run()
        ]
    response = {
        "results": results,
    }
    print(response)
    return response
