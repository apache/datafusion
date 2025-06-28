SELECT "UserID", extract(minute FROM to_timestamp_seconds("EventTime")) AS m, "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", m, "SearchPhrase" ORDER BY COUNT(*) DESC LIMIT 10;
