-- SQLBench-DS query 32 derived from TPC-DS query 32 under the terms of the TPC Fair Use Policy.
-- TPC-DS queries are Copyright 2021 Transaction Processing Performance Council.
-- This query was generated at scale factor 1.
select  sum(cs_ext_discount_amt)  as "excess discount amount" 
from 
   catalog_sales 
   ,item 
   ,date_dim
where
i_manufact_id = 283
and i_item_sk = cs_item_sk 
and d_date between '1999-02-22' and 
        (cast('1999-02-22' as date) + 90 days)
and d_date_sk = cs_sold_date_sk 
and cs_ext_discount_amt  
     > ( 
         select 
            1.3 * avg(cs_ext_discount_amt) 
         from 
            catalog_sales 
           ,date_dim
         where 
              cs_item_sk = i_item_sk 
          and d_date between '1999-02-22' and
                             (cast('1999-02-22' as date) + 90 days)
          and d_date_sk = cs_sold_date_sk 
      ) 
 LIMIT 100;

