-- Must set for ClickBench hits_partitioned dataset. See https://github.com/apache/datafusion/issues/16591
-- set datafusion.execution.parquet.binary_as_string = true

SELECT COUNT(*) AS ShareCount FROM hits WHERE "IsMobile" = 1 AND "MobilePhoneModel" LIKE 'iPhone%' AND "SocialAction" = 'share' AND "SocialSourceNetworkID" IN (5, 12) AND "ClientTimeZone" BETWEEN -5 AND 5 AND regexp_match("Referer", '\/campaign\/(spring|summer)_promo') IS NOT NULL AND CASE WHEN split_part(split_part("URL", 'resolution=', 2), '&', 1) ~ '^\d+$' THEN split_part(split_part("URL", 'resolution=', 2), '&', 1)::INT ELSE 0 END > 1920 AND levenshtein(CAST("UTMSource" AS STRING), CAST("UTMCampaign" AS STRING)) < 3;
