SELECT "BrowserCountry",  COUNT(DISTINCT "SocialNetwork"), COUNT(DISTINCT "HitColor"), COUNT(DISTINCT "BrowserLanguage"), COUNT(DISTINCT "SocialAction") FROM hits GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
