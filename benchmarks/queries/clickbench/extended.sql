SELECT COUNT(DISTINCT "SearchPhrase"), COUNT(DISTINCT "MobilePhone"), COUNT(DISTINCT "MobilePhoneModel") FROM hits;
SELECT COUNT(DISTINCT "HitColor"), COUNT(DISTINCT "BrowserCountry"), COUNT(DISTINCT "BrowserLanguage")  FROM hits;
SELECT "BrowserCountry",  COUNT(DISTINCT "SocialNetwork"), COUNT(DISTINCT "HitColor"), COUNT(DISTINCT "BrowserLanguage"), COUNT(DISTINCT "SocialAction") FROM hits GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
