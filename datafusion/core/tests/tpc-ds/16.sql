
SELECT
    count(DISTINCT cs_order_number) AS order_count,
    sum(cs_ext_ship_cost) AS total_shipping_cost,
    sum(cs_net_profit) AS total_net_profit
FROM catalog_sales cs1, date_dim, customer_address, call_center
WHERE
    d_date BETWEEN cast('2002-2-01' AS date) AND (
        cast('2002-2-01' AS date) + INTERVAL '60 days'
    )
    AND cs1.cs_ship_date_sk = d_date_sk
    AND cs1.cs_ship_addr_sk = ca_address_sk
    AND ca_state = 'GA'
    AND cs1.cs_call_center_sk = cc_call_center_sk
    AND cc_county IN (
        'Williamson County',
        'Daviess County',
        'Franklin Parish',
        'Barrow County',
        'Luce County',
        'Fairfield County'
    )
    AND exists (
        SELECT *
        FROM catalog_sales cs2
        WHERE
            cs1.cs_order_number = cs2.cs_order_number
            AND cs1.cs_warehouse_sk <> cs2.cs_warehouse_sk
    )
    AND NOT exists (
        SELECT *
        FROM catalog_returns cr1
        WHERE cs1.cs_order_number = cr1.cr_order_number
    )
ORDER BY count(DISTINCT cs_order_number)
LIMIT 100
;
