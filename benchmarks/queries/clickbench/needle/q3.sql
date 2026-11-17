-- Point lookup of a value stored in 302 of the 325 row groups of hits_partitioned
SELECT "UserID", "EventTime", "URL", "Title" FROM hits WHERE "SearchPhrase" = 'смотреть онлайн';
