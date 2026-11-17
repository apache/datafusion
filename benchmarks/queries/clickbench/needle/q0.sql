-- Point lookup of a value stored in 1 of the 325 row groups of hits_partitioned
SELECT "UserID", "EventTime", "URL", "Title" FROM hits WHERE "MobilePhoneModel" = 'ST23i';
