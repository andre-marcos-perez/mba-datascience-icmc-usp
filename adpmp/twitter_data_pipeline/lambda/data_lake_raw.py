import json
from urllib import request
from urllib.error import HTTPError

import pandas as pd
import awswrangler as wr


def lambda_handler(event: dict, context: dict = None) -> dict:

    """
    Extracts data from data source (congresstweets and congresstweets-automator projects hosted on GitHub) and loads on
    AWS s3 bucket mba-data-lake-raw.

    :param event: a dict with the execution date (str)
    :param context: a dict with runtime infrastructure data
    :return: a dict with the execution date (str) and a status flag (bool) indication whether the execution was successful

    :see: https://raw.githubusercontent.com/alexlitel/congresstweets/master/
    :see: https://raw.githubusercontent.com/alexlitel/congresstweets-automator/master
    """

    # Parse input

    date = event["date"].split(sep="T")[0]

    # Extract and Load

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

    # Extract and Load

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

    # Return result

    return dict(date=date, status=True)
