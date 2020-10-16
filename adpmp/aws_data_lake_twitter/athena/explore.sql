select party, count(party) as amount_of_tweets from mba_data_lake_enriched where "date" = CAST('2020-10-10' AS DATE) group by party;
select party, sentiment, count(sentiment) as amount_of_sentiments from mba_data_lake_enriched where "date" = CAST('2020-10-10' AS DATE) group by party, sentiment order by party, sentiment;