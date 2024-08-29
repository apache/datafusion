SELECT COUNT(DISTINCT "SearchPhrase"), COUNT(DISTINCT "MobilePhone"), COUNT(DISTINCT "MobilePhoneModel") FROM hits;
SELECT COUNT(DISTINCT "HitColor"), COUNT(DISTINCT "BrowserCountry"), COUNT(DISTINCT "BrowserLanguage")  FROM hits;
SELECT "BrowserCountry",  COUNT(DISTINCT "SocialNetwork"), COUNT(DISTINCT "HitColor"), COUNT(DISTINCT "BrowserLanguage"), COUNT(DISTINCT "SocialAction") FROM hits GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
SELECT "SocialSourceNetworkID", "RegionID", COUNT(*), AVG("Age"), AVG("ParamPrice"), STDDEV("ParamPrice") as s, VAR("ParamPrice")  FROM hits GROUP BY "SocialSourceNetworkID", "RegionID" HAVING s IS NOT NULL ORDER BY s DESC LIMIT 10;
