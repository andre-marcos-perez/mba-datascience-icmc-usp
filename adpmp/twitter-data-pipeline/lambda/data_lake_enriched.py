import random

import pandas as pd
import awswrangler as wr


def lambda_handler(event: dict, context: dict = None) -> dict:
    """
    Extracts data from AWS s3 bucket mba-data-lake-raw and wrangles it to loads on AWS s3 bucket mba-data-lake-enriched.

    :param event: a dict with the execution date (str)
    :param context: a dict with runtime infrastructure data
    :return: a dict with the execution date (str) and a status flag (bool) indication whether the execution was successful
    """

    # Parse input

    date = event["date"].split(sep="T")[0]

    # Extract

    try:
        users = wr.s3.read_json(path="s3://mba-data-lake-raw/{date}/users.json".format(date=date))
        tweets = wr.s3.read_json(path="s3://mba-data-lake-raw/{date}/tweets.json".format(date=date))
    except Exception as exc:
        raise exc

    # Transform

    tweets["sentiment"] = tweets["text"].apply(get_sentiment)

    users = users.explode(column="accounts").reset_index(drop=True).drop(columns=["id"])
    users = pd.concat([users.drop(columns=["accounts"]), users["accounts"].apply(pd.Series)["screen_name"]], axis=1)

    dataset = tweets.merge(users, on=["screen_name"], how="inner")
    dataset["date"] = date

    # Load

    wr.s3.to_parquet(
        df=dataset,
        path="s3://mba-data-lake-enriched/",
        dataset=True,
        compression="snappy",
        partition_cols=["date"],
        mode="overwrite_partitions",
        dtype={
            "id": "string",
            "screen_name": "string",
            "user_id": "string",
            "time": "timestamp",
            "link": "string",
            "text": "string",
            "source": "string",
            "sentiment": "string",
            "name": "string",
            "chamber": "string",
            "type": "string",
            "party": "string",
            "state": "string",
            "date": "date"
        }
    )

    # Return result

    return dict(date=date, status=True)


def get_sentiment(text: str) -> str:

    """
    A mock function to return the polarity of a text

    :param text: a string to calculate the polarity with
    :return: a string with the polarity classification (negative, neutral or positive)

    :see: https://en.wikipedia.org/wiki/Sentiment_analysis
    """

    polarity = random.uniform(-1, 1)
    if polarity == 0:
        return "neutral"
    else:
        return "positive" if polarity > 0 else "negative"
