SELECT "UserID", concat("SearchPhrase", 'helloworld21') as s, COUNT(*) FROM hits GROUP BY "UserID", s LIMIT 10;
SELECT "UserID", "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", "SearchPhrase" LIMIT 10;
