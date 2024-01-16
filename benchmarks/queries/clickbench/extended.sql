SELECT COUNT(DISTINCT "SearchPhrase"), COUNT(DISTINCT "MobilePhone"), COUNT(DISTINCT "MobilePhoneModel") FROM hits;
SELECT COUNT(DISTINCT "HitColor"), COUNT(DISTINCT "BrowserCountry"), COUNT(DISTINCT "BrowserLanguage")  FROM hits;
SELECT COUNT(DISTINCT "HitColor"), COUNT(DISTINCT "BrowserCountry"), COUNT(DISTINCT "BrowserLanguage"), COUNT(DISTINCT "URL")  FROM hits;