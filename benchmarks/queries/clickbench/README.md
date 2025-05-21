# ClickBench queries

This directory contains queries for the ClickBench benchmark https://benchmark.clickhouse.com/

ClickBench is focused on aggregation and filtering performance (though it has no Joins)

## Files:

- `queries.sql` - Actual ClickBench queries, downloaded from the [ClickBench repository]
- `extended.sql` - "Extended" DataFusion specific queries.

[clickbench repository]: https://github.com/ClickHouse/ClickBench/blob/main/datafusion/queries.sql

## "Extended" Queries

The "extended" queries are not part of the official ClickBench benchmark.
Instead they are used to test other DataFusion features that are not covered by
the standard benchmark. Each description below is for the corresponding line in
`extended.sql` (line 1 is `Q0`, line 2 is `Q1`, etc.)

### Q0: Data Exploration

**Question**: "How many distinct searches, mobile phones, and mobile phone models are there in the dataset?"

**Important Query Properties**: multiple `COUNT DISTINCT`s, with low and high cardinality
distinct string columns.

```sql
SELECT COUNT(DISTINCT "SearchPhrase"), COUNT(DISTINCT "MobilePhone"), COUNT(DISTINCT "MobilePhoneModel")
FROM hits;
```

### Q1: Data Exploration

**Question**: "How many distinct "hit color", "browser country" and "language" are there in the dataset?"

**Important Query Properties**: multiple `COUNT DISTINCT`s. All three are small strings (length either 1 or 2).

```sql
SELECT COUNT(DISTINCT "HitColor"), COUNT(DISTINCT "BrowserCountry"), COUNT(DISTINCT "BrowserLanguage")
FROM hits;
```

### Q2: Top 10 analysis

**Question**: "Find the top 10 "browser country" by number of distinct "social network"s,
including the distinct counts of "hit color", "browser language",
and "social action"."

**Important Query Properties**: GROUP BY short, string, multiple `COUNT DISTINCT`s. There are several small strings (length either 1 or 2).

```sql
SELECT "BrowserCountry",  COUNT(DISTINCT "SocialNetwork"), COUNT(DISTINCT "HitColor"), COUNT(DISTINCT "BrowserLanguage"), COUNT(DISTINCT "SocialAction")
FROM hits
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
```

### Q3: What is the income distribution for users in specific regions

**Question**: "What regions and social networks have the highest variance of parameter price?"

**Important Query Properties**: STDDEV and VAR aggregation functions, GROUP BY multiple small ints

```sql
SELECT "SocialSourceNetworkID", "RegionID", COUNT(*), AVG("Age"), AVG("ParamPrice"), STDDEV("ParamPrice") as s, VAR("ParamPrice")
FROM 'hits.parquet'
GROUP BY "SocialSourceNetworkID", "RegionID"
HAVING s IS NOT NULL
ORDER BY s DESC
LIMIT 10;
```

### Q4: Response start time distribution analysis (median)

**Question**: Find the WatchIDs with the highest median "ResponseStartTiming" without Java enabled

**Important Query Properties**: MEDIAN, functions, high cardinality grouping that skips intermediate aggregation

Note this query is somewhat synthetic as "WatchID" is almost unique (there are a few duplicates)

```sql
SELECT "ClientIP", "WatchID",  COUNT(*) c, MIN("ResponseStartTiming") tmin, MEDIAN("ResponseStartTiming") tmed, MAX("ResponseStartTiming") tmax
FROM 'hits.parquet'
WHERE "JavaEnable" = 0 -- filters to 32M of 100M rows
GROUP BY  "ClientIP", "WatchID"
HAVING c > 1
ORDER BY tmed DESC
LIMIT 10;
```

Results look like

```
+-------------+---------------------+---+------+------+------+
| ClientIP    | WatchID             | c | tmin | tmed | tmax |
+-------------+---------------------+---+------+------+------+
| 1611957945  | 6655575552203051303 | 2 | 0    | 0    | 0    |
| -1402644643 | 8566928176839891583 | 2 | 0    | 0    | 0    |
+-------------+---------------------+---+------+------+------+
```

### Q5: Response start time distribution analysis (p95)

**Question**: Find the WatchIDs with the highest p95 "ResponseStartTiming" without Java enabled

**Important Query Properties**: APPROX_PERCENTILE_CONT, functions, high cardinality grouping that skips intermediate aggregation

Note this query is somewhat synthetic as "WatchID" is almost unique (there are a few duplicates)

```sql
SELECT "ClientIP", "WatchID",  COUNT(*) c, MIN("ResponseStartTiming") tmin, APPROX_PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY "ResponseStartTiming") tp95, MAX("ResponseStartTiming") tmax
FROM 'hits.parquet'
WHERE "JavaEnable" = 0 -- filters to 32M of 100M rows
GROUP BY  "ClientIP", "WatchID"
HAVING c > 1
ORDER BY tp95 DESC
LIMIT 10;
```

Results look like

```
+-------------+---------------------+---+------+------+------+
| ClientIP    | WatchID             | c | tmin | tp95 | tmax |
+-------------+---------------------+---+------+------+------+
| 1611957945  | 6655575552203051303 | 2 | 0    | 0    | 0    |
| -1402644643 | 8566928176839891583 | 2 | 0    | 0    | 0    |
+-------------+---------------------+---+------+------+------+
```

### Q6: How many social shares meet complex multi-stage filtering criteria?

**Question**: What is the count of sharing actions from iPhone mobile users on specific social networks, within common timezones, participating in seasonal campaigns, with high screen resolutions and closely matched UTM parameters?
**Important Query Properties**: Simple filter with high-selectivity, Costly string matching, A large number of filters with high overhead are positioned relatively later in the process

```sql
SELECT COUNT(*) AS ShareCount
FROM hits
WHERE
	-- Stage 1: High-selectivity filters (fast)
    "IsMobile" = 1 -- Filter mobile users
    AND "MobilePhoneModel" LIKE 'iPhone%' -- Match iPhone models
    AND "SocialAction" = 'share' -- Identify social sharing actions

	-- Stage 2: Moderate filters (cheap)
    AND "SocialSourceNetworkID" IN (5, 12) -- Filter specific social networks
    AND "ClientTimeZone" BETWEEN -5 AND 5 -- Restrict to common timezones

	-- Stage 3: Heavy computations (expensive)
    AND regexp_match("Referer", '\/campaign\/(spring|summer)_promo') IS NOT NULL -- Find campaign-specific referrers
    AND CASE
        WHEN split_part(split_part("URL", 'resolution=', 2), '&', 1) ~ '^\d+$'
        THEN split_part(split_part("URL", 'resolution=', 2), '&', 1)::INT
        ELSE 0
    END > 1920 -- Extract and validate resolution parameter
    AND levenshtein(CAST("UTMSource" AS STRING), CAST("UTMCampaign" AS STRING)) < 3 -- Verify UTM parameter similarity
```

Result is empty,Since it has already been filtered by `"SocialAction" = 'share'`.

### Q7: Device Resolution and Refresh Behavior Analysis

**Question**: Identify the top 10 WatchIDs with the highest resolution range (min/max "ResolutionWidth") and total refresh count ("IsRefresh") in descending WatchID order

**Important Query Properties**: Primitive aggregation functions, group by single primitive column, high cardinality grouping

```sql
SELECT "WatchID", MIN("ResolutionWidth") as wmin, MAX("ResolutionWidth") as wmax, SUM("IsRefresh") as srefresh
FROM hits
GROUP BY "WatchID"
ORDER BY "WatchID" DESC
LIMIT 10;
```

Results look like

```
+---------------------+------+------+----------+
| WatchID             | wmin | wmax | srefresh |
+---------------------+------+------+----------+
| 9223372033328793741 | 1368 | 1368 | 0        |
| 9223371941779979288 | 1479 | 1479 | 0        |
| 9223371906781104763 | 1638 | 1638 | 0        |
| 9223371803397398692 | 1990 | 1990 | 0        |
| 9223371799215233959 | 1638 | 1638 | 0        |
| 9223371785975219972 | 0    | 0    | 0        |
| 9223371776706839366 | 1368 | 1368 | 0        |
| 9223371740707848038 | 1750 | 1750 | 0        |
| 9223371715190479830 | 1368 | 1368 | 0        |
| 9223371620124912624 | 1828 | 1828 | 0        |
+---------------------+------+------+----------+
```

### Q8: Average Latency and Response Time Analysis

**Question**: Which combinations of operating system, region, and user agent exhibit the highest average latency? For each of these combinations, also report the average response time.

**Important Query Properties**: Multiple average of Duration, high cardinality grouping

```sql
SELECT "RegionID", "UserAgent", "OS", AVG(to_timestamp("ResponseEndTiming")-to_timestamp("ResponseStartTiming")) as avg_response_time, AVG(to_timestamp("ResponseEndTiming")-to_timestamp("ConnectTiming")) as avg_latency
FROM hits
GROUP BY "RegionID", "UserAgent", "OS"
ORDER BY avg_latency DESC
LIMIT 10;
```

Results look like

```
+----------+-----------+-----+------------------------------------------+------------------------------------------+
| RegionID | UserAgent | OS  | avg_response_time                        | avg_latency                              |
+----------+-----------+-----+------------------------------------------+------------------------------------------+
| 22934    | 5         | 126 | 0 days 8 hours 20 mins 0.000000000 secs  | 0 days 8 hours 20 mins 0.000000000 secs  |
| 22735    | 82        | 74  | 0 days 8 hours 20 mins 0.000000000 secs  | 0 days 8 hours 20 mins 0.000000000 secs  |
| 21687    | 32        | 49  | 0 days 8 hours 20 mins 0.000000000 secs  | 0 days 8 hours 20 mins 0.000000000 secs  |
| 18518    | 82        | 77  | 0 days 8 hours 20 mins 0.000000000 secs  | 0 days 8 hours 20 mins 0.000000000 secs  |
| 14006    | 7         | 126 | 0 days 7 hours 58 mins 20.000000000 secs | 0 days 8 hours 20 mins 0.000000000 secs  |
| 9803     | 82        | 77  | 0 days 8 hours 20 mins 0.000000000 secs  | 0 days 8 hours 20 mins 0.000000000 secs  |
| 107108   | 82        | 77  | 0 days 8 hours 20 mins 0.000000000 secs  | 0 days 8 hours 20 mins 0.000000000 secs  |
| 111626   | 7         | 44  | 0 days 7 hours 23 mins 12.500000000 secs | 0 days 8 hours 0 mins 47.000000000 secs  |
| 17716    | 56        | 44  | 0 days 6 hours 48 mins 44.500000000 secs | 0 days 7 hours 35 mins 47.000000000 secs |
| 13631    | 82        | 45  | 0 days 7 hours 23 mins 1.000000000 secs  | 0 days 7 hours 23 mins 1.000000000 secs  |
+----------+-----------+-----+------------------------------------------+------------------------------------------+
10 row(s) fetched.
Elapsed 30.195 seconds.
```

## Data Notes

Here are some interesting statistics about the data used in the queries
Max length of `"SearchPhrase"` is 1113 characters

```sql
> select min(length("SearchPhrase")) as "SearchPhrase_len_min", max(length("SearchPhrase")) "SearchPhrase_len_max" from 'hits.parquet' limit 10;
+----------------------+----------------------+
| SearchPhrase_len_min | SearchPhrase_len_max |
+----------------------+----------------------+
| 0                    | 1113                 |
+----------------------+----------------------+
```

Here is the schema of the data

```sql
> describe 'hits.parquet';
+-----------------------+-----------+-------------+
| column_name           | data_type | is_nullable |
+-----------------------+-----------+-------------+
| WatchID               | Int64     | NO          |
| JavaEnable            | Int16     | NO          |
| Title                 | Utf8      | NO          |
| GoodEvent             | Int16     | NO          |
| EventTime             | Int64     | NO          |
| EventDate             | UInt16    | NO          |
| CounterID             | Int32     | NO          |
| ClientIP              | Int32     | NO          |
| RegionID              | Int32     | NO          |
| UserID                | Int64     | NO          |
| CounterClass          | Int16     | NO          |
| OS                    | Int16     | NO          |
| UserAgent             | Int16     | NO          |
| URL                   | Utf8      | NO          |
| Referer               | Utf8      | NO          |
| IsRefresh             | Int16     | NO          |
| RefererCategoryID     | Int16     | NO          |
| RefererRegionID       | Int32     | NO          |
| URLCategoryID         | Int16     | NO          |
| URLRegionID           | Int32     | NO          |
| ResolutionWidth       | Int16     | NO          |
| ResolutionHeight      | Int16     | NO          |
| ResolutionDepth       | Int16     | NO          |
| FlashMajor            | Int16     | NO          |
| FlashMinor            | Int16     | NO          |
| FlashMinor2           | Utf8      | NO          |
| NetMajor              | Int16     | NO          |
| NetMinor              | Int16     | NO          |
| UserAgentMajor        | Int16     | NO          |
| UserAgentMinor        | Utf8      | NO          |
| CookieEnable          | Int16     | NO          |
| JavascriptEnable      | Int16     | NO          |
| IsMobile              | Int16     | NO          |
| MobilePhone           | Int16     | NO          |
| MobilePhoneModel      | Utf8      | NO          |
| Params                | Utf8      | NO          |
| IPNetworkID           | Int32     | NO          |
| TraficSourceID        | Int16     | NO          |
| SearchEngineID        | Int16     | NO          |
| SearchPhrase          | Utf8      | NO          |
| AdvEngineID           | Int16     | NO          |
| IsArtifical           | Int16     | NO          |
| WindowClientWidth     | Int16     | NO          |
| WindowClientHeight    | Int16     | NO          |
| ClientTimeZone        | Int16     | NO          |
| ClientEventTime       | Int64     | NO          |
| SilverlightVersion1   | Int16     | NO          |
| SilverlightVersion2   | Int16     | NO          |
| SilverlightVersion3   | Int32     | NO          |
| SilverlightVersion4   | Int16     | NO          |
| PageCharset           | Utf8      | NO          |
| CodeVersion           | Int32     | NO          |
| IsLink                | Int16     | NO          |
| IsDownload            | Int16     | NO          |
| IsNotBounce           | Int16     | NO          |
| FUniqID               | Int64     | NO          |
| OriginalURL           | Utf8      | NO          |
| HID                   | Int32     | NO          |
| IsOldCounter          | Int16     | NO          |
| IsEvent               | Int16     | NO          |
| IsParameter           | Int16     | NO          |
| DontCountHits         | Int16     | NO          |
| WithHash              | Int16     | NO          |
| HitColor              | Utf8      | NO          |
| LocalEventTime        | Int64     | NO          |
| Age                   | Int16     | NO          |
| Sex                   | Int16     | NO          |
| Income                | Int16     | NO          |
| Interests             | Int16     | NO          |
| Robotness             | Int16     | NO          |
| RemoteIP              | Int32     | NO          |
| WindowName            | Int32     | NO          |
| OpenerName            | Int32     | NO          |
| HistoryLength         | Int16     | NO          |
| BrowserLanguage       | Utf8      | NO          |
| BrowserCountry        | Utf8      | NO          |
| SocialNetwork         | Utf8      | NO          |
| SocialAction          | Utf8      | NO          |
| HTTPError             | Int16     | NO          |
| SendTiming            | Int32     | NO          |
| DNSTiming             | Int32     | NO          |
| ConnectTiming         | Int32     | NO          |
| ResponseStartTiming   | Int32     | NO          |
| ResponseEndTiming     | Int32     | NO          |
| FetchTiming           | Int32     | NO          |
| SocialSourceNetworkID | Int16     | NO          |
| SocialSourcePage      | Utf8      | NO          |
| ParamPrice            | Int64     | NO          |
| ParamOrderID          | Utf8      | NO          |
| ParamCurrency         | Utf8      | NO          |
| ParamCurrencyID       | Int16     | NO          |
| OpenstatServiceName   | Utf8      | NO          |
| OpenstatCampaignID    | Utf8      | NO          |
| OpenstatAdID          | Utf8      | NO          |
| OpenstatSourceID      | Utf8      | NO          |
| UTMSource             | Utf8      | NO          |
| UTMMedium             | Utf8      | NO          |
| UTMCampaign           | Utf8      | NO          |
| UTMContent            | Utf8      | NO          |
| UTMTerm               | Utf8      | NO          |
| FromTag               | Utf8      | NO          |
| HasGCLID              | Int16     | NO          |
| RefererHash           | Int64     | NO          |
| URLHash               | Int64     | NO          |
| CLID                  | Int32     | NO          |
+-----------------------+-----------+-------------+
105 rows in set. Query took 0.034 seconds.

```
