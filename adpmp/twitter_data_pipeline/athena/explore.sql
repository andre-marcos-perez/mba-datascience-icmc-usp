# -- Get the number of tweets per party on a specific date

SELECT party, COUNT(party) AS amount_of_tweets
FROM mba_data_lake_enriched
WHERE "date" = CAST('2020-10-10' AS DATE)
GROUP BY party;

# -- Get the number of tweets per sentiment per party on a specific date

SELECT party, sentiment, COUNT(sentiment) AS amount_of_sentiments
FROM mba_data_lake_enriched
WHERE "date" = CAST('2020-10-10' AS DATE)
GROUP BY party, sentiment
ORDER BY party, sentiment;