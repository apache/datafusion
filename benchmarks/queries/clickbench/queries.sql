SELECT "UserID", "SearchPhrase", COUNT(*) FROM hits GROUP BY "UserID", "SearchPhrase" ORDER BY COUNT(*) DESC LIMIT 10;
SELECT "UserID", concat("SearchPhrase", repeat('hello', 20)) as s, COUNT(*) FROM hits GROUP BY "UserID", s ORDER BY COUNT(*) DESC LIMIT 10;
