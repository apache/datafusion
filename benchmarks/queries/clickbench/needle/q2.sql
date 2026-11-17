-- Point lookup of a value stored in 1 of the 325 row groups of hits_partitioned, on a high cardinality column
SELECT "UserID", "EventTime", "URL", "Title" FROM hits WHERE "SearchPhrase" = 'модулятор 3 воина мирные';
