SELECT "WatchID", MIN("ResolutionWidth") as wmin, MAX("ResolutionWidth") as wmax, SUM("IsRefresh") as srefresh FROM hits GROUP BY "WatchID" ORDER BY "WatchID" DESC LIMIT 10;
