import json
from urllib import request
from urllib.error import HTTPError

import pandas as pd
import awswrangler as wr


def lambda_handler(event: dict, context: dict = None) -> dict:

    date = event["date"]

    try:
        url = "https://raw.githubusercontent.com/alexlitel/congresstweets/master/data/{date}.json".format(date=date)
        response = request.urlopen(url=url).read().decode()
        data = json.loads(response)
        data = pd.DataFrame(data)
        wr.s3.to_json(df=data, path="s3://mba-data-lake-raw/{date}/tweets.json".format(date=date))
    except HTTPError as exc:
        print(exc)
        return dict(date=date, status=False)
    except Exception as exc:
        raise exc

    try:
        url = "https://raw.githubusercontent.com/alexlitel/congresstweets-automator/master/data/users.json"
        response = request.urlopen(url=url).read().decode()
        data = json.loads(response)
        data = pd.DataFrame(data)
        wr.s3.to_json(df=data, path="s3://mba-data-lake-raw/{date}/users.json".format(date=date))
    except HTTPError as exc:
        print(exc)
        return dict(date=date, status=False)
    except Exception as exc:
        raise exc

    return dict(date=date, status=True)
