-- SQLBench-DS query 55 derived from TPC-DS query 55 under the terms of the TPC Fair Use Policy.
-- TPC-DS queries are Copyright 2021 Transaction Processing Performance Council.
-- This query was generated at scale factor 1.
select  i_brand_id brand_id, i_brand brand,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item
 where d_date_sk = ss_sold_date_sk
 	and ss_item_sk = i_item_sk
 	and i_manager_id=20
 	and d_moy=12
 	and d_year=1998
 group by i_brand, i_brand_id
 order by ext_price desc, i_brand_id
 LIMIT 100 ;

