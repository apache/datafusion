-- IN list of values stored in 5 of the 325 row groups of hits_partitioned
SELECT "UserID", "EventTime", "URL", "Title" FROM hits WHERE "MobilePhoneModel" IN ('ST23i', 'MediaPad7Lite', 'MT27i', 'X2-00', 'SK17i');
