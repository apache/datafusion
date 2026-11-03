-- Full schema's are required to define primary keys (to match the rust native version)
CREATE EXTERNAL TABLE call_center
(
    cc_call_center_sk INT,
    cc_call_center_id VARCHAR,
    cc_rec_start_date DATE,
    cc_rec_end_date DATE,
    cc_closed_date_sk INT,
    cc_open_date_sk INT,
    cc_name VARCHAR,
    cc_class VARCHAR,
    cc_employees INT,
    cc_sq_ft INT,
    cc_hours VARCHAR,
    cc_manager VARCHAR,
    cc_mkt_id INT,
    cc_mkt_class VARCHAR,
    cc_mkt_desc VARCHAR,
    cc_market_manager VARCHAR,
    cc_division INT,
    cc_division_name VARCHAR,
    cc_company INT,
    cc_company_name VARCHAR,
    cc_street_number VARCHAR,
    cc_street_name VARCHAR,
    cc_street_type VARCHAR,
    cc_suite_number VARCHAR,
    cc_city VARCHAR,
    cc_county VARCHAR,
    cc_state VARCHAR,
    cc_zip VARCHAR,
    cc_country VARCHAR,
    cc_gmt_offset DECIMAL(5, 2),
    cc_tax_percentage DECIMAL(5, 2),
    PRIMARY KEY (cc_call_center_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/call_center.parquet';

CREATE EXTERNAL TABLE catalog_page
(
    cp_catalog_page_sk INT,
    cp_catalog_page_id VARCHAR,
    cp_start_date_sk INT,
    cp_end_date_sk INT,
    cp_department VARCHAR,
    cp_catalog_number INT,
    cp_catalog_page_number INT,
    cp_description VARCHAR,
    cp_type VARCHAR,
    PRIMARY KEY (cp_catalog_page_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/catalog_page.parquet';

CREATE EXTERNAL TABLE catalog_returns
(
    cr_returned_date_sk INT,
    cr_returned_time_sk INT,
    cr_item_sk INT,
    cr_refunded_customer_sk INT,
    cr_refunded_cdemo_sk INT,
    cr_refunded_hdemo_sk INT,
    cr_refunded_addr_sk INT,
    cr_returning_customer_sk INT,
    cr_returning_cdemo_sk INT,
    cr_returning_hdemo_sk INT,
    cr_returning_addr_sk INT,
    cr_call_center_sk INT,
    cr_catalog_page_sk INT,
    cr_ship_mode_sk INT,
    cr_warehouse_sk INT,
    cr_reason_sk INT,
    cr_order_number INT,
    cr_return_quantity INT,
    cr_return_amount DECIMAL(7, 2),
    cr_return_tax DECIMAL(7, 2),
    cr_return_amt_inc_tax DECIMAL(7, 2),
    cr_fee DECIMAL(7, 2),
    cr_return_ship_cost DECIMAL(7, 2),
    cr_refunded_cash DECIMAL(7, 2),
    cr_reversed_charge DECIMAL(7, 2),
    cr_store_credit DECIMAL(7, 2),
    cr_net_loss DECIMAL(7, 2),
    PRIMARY KEY (cr_item_sk, cr_order_number)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/catalog_returns.parquet';

CREATE EXTERNAL TABLE catalog_sales
(
    cs_sold_date_sk INT,
    cs_sold_time_sk INT,
    cs_ship_date_sk INT,
    cs_bill_customer_sk INT,
    cs_bill_cdemo_sk INT,
    cs_bill_hdemo_sk INT,
    cs_bill_addr_sk INT,
    cs_ship_customer_sk INT,
    cs_ship_cdemo_sk INT,
    cs_ship_hdemo_sk INT,
    cs_ship_addr_sk INT,
    cs_call_center_sk INT,
    cs_catalog_page_sk INT,
    cs_ship_mode_sk INT,
    cs_warehouse_sk INT,
    cs_item_sk INT,
    cs_promo_sk INT,
    cs_order_number INT,
    cs_quantity INT,
    cs_wholesale_cost DECIMAL(7, 2),
    cs_list_price DECIMAL(7, 2),
    cs_sales_price DECIMAL(7, 2),
    cs_ext_discount_amt DECIMAL(7, 2),
    cs_ext_sales_price DECIMAL(7, 2),
    cs_ext_wholesale_cost DECIMAL(7, 2),
    cs_ext_list_price DECIMAL(7, 2),
    cs_ext_tax DECIMAL(7, 2),
    cs_coupon_amt DECIMAL(7, 2),
    cs_ext_ship_cost DECIMAL(7, 2),
    cs_net_paid DECIMAL(7, 2),
    cs_net_paid_inc_tax DECIMAL(7, 2),
    cs_net_paid_inc_ship DECIMAL(7, 2),
    cs_net_paid_inc_ship_tax DECIMAL(7, 2),
    cs_net_profit DECIMAL(7, 2),
    PRIMARY KEY (cs_item_sk, cs_order_number)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/catalog_sales.parquet';

CREATE EXTERNAL TABLE customer
(
    c_customer_sk INT,
    c_customer_id VARCHAR,
    c_current_cdemo_sk INT,
    c_current_hdemo_sk INT,
    c_current_addr_sk INT,
    c_first_shipto_date_sk INT,
    c_first_sales_date_sk INT,
    c_salutation VARCHAR,
    c_first_name VARCHAR,
    c_last_name VARCHAR,
    c_preferred_cust_flag VARCHAR,
    c_birth_day INT,
    c_birth_month INT,
    c_birth_year INT,
    c_birth_country VARCHAR,
    c_login VARCHAR,
    c_email_address VARCHAR,
    c_last_review_date_sk VARCHAR,
    PRIMARY KEY (c_customer_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/customer.parquet';

CREATE EXTERNAL TABLE customer_address
(
    ca_address_sk INT,
    ca_address_id VARCHAR,
    ca_street_number VARCHAR,
    ca_street_name VARCHAR,
    ca_street_type VARCHAR,
    ca_suite_number VARCHAR,
    ca_city VARCHAR,
    ca_county VARCHAR,
    ca_state VARCHAR,
    ca_zip VARCHAR,
    ca_country VARCHAR,
    ca_gmt_offset DECIMAL(5, 2),
    ca_location_type VARCHAR,
    PRIMARY KEY (ca_address_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/customer_address.parquet';

CREATE EXTERNAL TABLE customer_demographics
(
    cd_demo_sk INT,
    cd_gender VARCHAR,
    cd_marital_status VARCHAR,
    cd_education_status VARCHAR,
    cd_purchase_estimate INT,
    cd_credit_rating VARCHAR,
    cd_dep_count INT,
    cd_dep_employed_count INT,
    cd_dep_college_count INT,
    PRIMARY KEY (cd_demo_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/customer_demographics.parquet';

CREATE EXTERNAL TABLE date_dim
(
    d_date_sk INT,
    d_date_id VARCHAR,
    d_date DATE,
    d_month_seq INT,
    d_week_seq INT,
    d_quarter_seq INT,
    d_year INT,
    d_dow INT,
    d_moy INT,
    d_dom INT,
    d_qoy INT,
    d_fy_year INT,
    d_fy_quarter_seq INT,
    d_fy_week_seq INT,
    d_day_name VARCHAR,
    d_quarter_name VARCHAR,
    d_holiday VARCHAR,
    d_weekend VARCHAR,
    d_following_holiday VARCHAR,
    d_first_dom INT,
    d_last_dom INT,
    d_same_day_ly INT,
    d_same_day_lq INT,
    d_current_day VARCHAR,
    d_current_week VARCHAR,
    d_current_month VARCHAR,
    d_current_quarter VARCHAR,
    d_current_year VARCHAR,
    PRIMARY KEY (d_date_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/date_dim.parquet';

CREATE EXTERNAL TABLE household_demographics
(
    hd_demo_sk INT,
    hd_income_band_sk INT,
    hd_buy_potential VARCHAR,
    hd_dep_count INT,
    hd_vehicle_count INT,
    PRIMARY KEY (hd_demo_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/household_demographics.parquet';

CREATE EXTERNAL TABLE income_band
(
    ib_income_band_sk INT,
    ib_lower_bound INT,
    ib_upper_bound INT,
    PRIMARY KEY (ib_income_band_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/income_band.parquet';

CREATE EXTERNAL TABLE inventory
(
    inv_date_sk INT,
    inv_item_sk INT,
    inv_warehouse_sk INT,
    inv_quantity_on_hand INT,
    PRIMARY KEY (inv_date_sk, inv_item_sk, inv_warehouse_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/inventory.parquet';

CREATE EXTERNAL TABLE item
(
    i_item_sk INT,
    i_item_id VARCHAR,
    i_rec_start_date DATE,
    i_rec_end_date DATE,
    i_item_desc VARCHAR,
    i_current_price DECIMAL(7, 2),
    i_wholesale_cost DECIMAL(7, 2),
    i_brand_id INT,
    i_brand VARCHAR,
    i_class_id INT,
    i_class VARCHAR,
    i_category_id INT,
    i_category VARCHAR,
    i_manufact_id INT,
    i_manufact VARCHAR,
    i_size VARCHAR,
    i_formulation VARCHAR,
    i_color VARCHAR,
    i_units VARCHAR,
    i_container VARCHAR,
    i_manager_id INT,
    i_product_name VARCHAR,
    PRIMARY KEY (i_item_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/item.parquet';

CREATE EXTERNAL TABLE promotion
(
    p_promo_sk INT,
    p_promo_id VARCHAR,
    p_start_date_sk INT,
    p_end_date_sk INT,
    p_item_sk INT,
    p_cost DECIMAL(15, 2),
    p_response_target INT,
    p_promo_name VARCHAR,
    p_channel_dmail VARCHAR,
    p_channel_email VARCHAR,
    p_channel_catalog VARCHAR,
    p_channel_tv VARCHAR,
    p_channel_radio VARCHAR,
    p_channel_press VARCHAR,
    p_channel_event VARCHAR,
    p_channel_demo VARCHAR,
    p_channel_details VARCHAR,
    p_purpose VARCHAR,
    p_discount_active VARCHAR,
    PRIMARY KEY (p_promo_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/promotion.parquet';

CREATE EXTERNAL TABLE reason
(
    r_reason_sk INT,
    r_reason_id VARCHAR,
    r_reason_desc VARCHAR,
    PRIMARY KEY (r_reason_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/reason.parquet';

CREATE EXTERNAL TABLE ship_mode
(
    sm_ship_mode_sk INT,
    sm_ship_mode_id VARCHAR,
    sm_type VARCHAR,
    sm_code VARCHAR,
    sm_carrier VARCHAR,
    sm_contract VARCHAR,
    PRIMARY KEY (sm_ship_mode_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/ship_mode.parquet';

CREATE EXTERNAL TABLE store
(
    s_store_sk INT,
    s_store_id VARCHAR,
    s_rec_start_date DATE,
    s_rec_end_date DATE,
    s_closed_date_sk INT,
    s_store_name VARCHAR,
    s_number_employees INT,
    s_floor_space INT,
    s_hours VARCHAR,
    s_manager VARCHAR,
    s_market_id INT,
    s_geography_class VARCHAR,
    s_market_desc VARCHAR,
    s_market_manager VARCHAR,
    s_division_id INT,
    s_division_name VARCHAR,
    s_company_id INT,
    s_company_name VARCHAR,
    s_street_number VARCHAR,
    s_street_name VARCHAR,
    s_street_type VARCHAR,
    s_suite_number VARCHAR,
    s_city VARCHAR,
    s_county VARCHAR,
    s_state VARCHAR,
    s_zip VARCHAR,
    s_country VARCHAR,
    s_gmt_offset DECIMAL(5, 2),
    s_tax_precentage DECIMAL(5, 2),
    PRIMARY KEY (s_store_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/store.parquet';

CREATE EXTERNAL TABLE store_returns
(
    sr_returned_date_sk INT,
    sr_return_time_sk INT,
    sr_item_sk INT,
    sr_customer_sk INT,
    sr_cdemo_sk INT,
    sr_hdemo_sk INT,
    sr_addr_sk INT,
    sr_store_sk INT,
    sr_reason_sk INT,
    sr_ticket_number INT,
    sr_return_quantity INT,
    sr_return_amt DECIMAL(7, 2),
    sr_return_tax DECIMAL(7, 2),
    sr_return_amt_inc_tax DECIMAL(7, 2),
    sr_fee DECIMAL(7, 2),
    sr_return_ship_cost DECIMAL(7, 2),
    sr_refunded_cash DECIMAL(7, 2),
    sr_reversed_charge DECIMAL(7, 2),
    sr_store_credit DECIMAL(7, 2),
    sr_net_loss DECIMAL(7, 2),
    PRIMARY KEY (sr_item_sk, sr_ticket_number)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/store_returns.parquet';

CREATE EXTERNAL TABLE store_sales
(
    ss_sold_date_sk INT,
    ss_sold_time_sk INT,
    ss_item_sk INT,
    ss_customer_sk INT,
    ss_cdemo_sk INT,
    ss_hdemo_sk INT,
    ss_addr_sk INT,
    ss_store_sk INT,
    ss_promo_sk INT,
    ss_ticket_number INT,
    ss_quantity INT,
    ss_wholesale_cost DECIMAL(7, 2),
    ss_list_price DECIMAL(7, 2),
    ss_sales_price DECIMAL(7, 2),
    ss_ext_discount_amt DECIMAL(7, 2),
    ss_ext_sales_price DECIMAL(7, 2),
    ss_ext_wholesale_cost DECIMAL(7, 2),
    ss_ext_list_price DECIMAL(7, 2),
    ss_ext_tax DECIMAL(7, 2),
    ss_coupon_amt DECIMAL(7, 2),
    ss_net_paid DECIMAL(7, 2),
    ss_net_paid_inc_tax DECIMAL(7, 2),
    ss_net_profit DECIMAL(7, 2),
    PRIMARY KEY (ss_item_sk, ss_ticket_number)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/store_sales.parquet';

CREATE EXTERNAL TABLE time_dim
(
    t_time_sk INT,
    t_time_id VARCHAR,
    t_time INT,
    t_hour INT,
    t_minute INT,
    t_second INT,
    t_am_pm VARCHAR,
    t_shift VARCHAR,
    t_sub_shift VARCHAR,
    t_meal_time VARCHAR,
    PRIMARY KEY (t_time_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/time_dim.parquet';

CREATE EXTERNAL TABLE warehouse
(
    w_warehouse_sk INT,
    w_warehouse_id VARCHAR,
    w_warehouse_name VARCHAR,
    w_warehouse_sq_ft INT,
    w_street_number VARCHAR,
    w_street_name VARCHAR,
    w_street_type VARCHAR,
    w_suite_number VARCHAR,
    w_city VARCHAR,
    w_county VARCHAR,
    w_state VARCHAR,
    w_zip VARCHAR,
    w_country VARCHAR,
    w_gmt_offset DECIMAL(5, 2),
    PRIMARY KEY (w_warehouse_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/warehouse.parquet';

CREATE EXTERNAL TABLE web_page
(
    wp_web_page_sk INT,
    wp_web_page_id VARCHAR,
    wp_rec_start_date DATE,
    wp_rec_end_date DATE,
    wp_creation_date_sk INT,
    wp_access_date_sk INT,
    wp_autogen_flag VARCHAR,
    wp_customer_sk INT,
    wp_url VARCHAR,
    wp_type VARCHAR,
    wp_char_count INT,
    wp_link_count INT,
    wp_image_count INT,
    wp_max_ad_count INT,
    PRIMARY KEY (wp_web_page_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/web_page.parquet';

CREATE EXTERNAL TABLE web_returns
(
    wr_returned_date_sk INT,
    wr_returned_time_sk INT,
    wr_item_sk INT,
    wr_refunded_customer_sk INT,
    wr_refunded_cdemo_sk INT,
    wr_refunded_hdemo_sk INT,
    wr_refunded_addr_sk INT,
    wr_returning_customer_sk INT,
    wr_returning_cdemo_sk INT,
    wr_returning_hdemo_sk INT,
    wr_returning_addr_sk INT,
    wr_web_page_sk INT,
    wr_reason_sk INT,
    wr_order_number INT,
    wr_return_quantity INT,
    wr_return_amt DECIMAL(7, 2),
    wr_return_tax DECIMAL(7, 2),
    wr_return_amt_inc_tax DECIMAL(7, 2),
    wr_fee DECIMAL(7, 2),
    wr_return_ship_cost DECIMAL(7, 2),
    wr_refunded_cash DECIMAL(7, 2),
    wr_reversed_charge DECIMAL(7, 2),
    wr_account_credit DECIMAL(7, 2),
    wr_net_loss DECIMAL(7, 2),
    PRIMARY KEY (wr_item_sk, wr_order_number)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/web_returns.parquet';

CREATE EXTERNAL TABLE web_sales
(
    ws_sold_date_sk INT,
    ws_sold_time_sk INT,
    ws_ship_date_sk INT,
    ws_item_sk INT,
    ws_bill_customer_sk INT,
    ws_bill_cdemo_sk INT,
    ws_bill_hdemo_sk INT,
    ws_bill_addr_sk INT,
    ws_ship_customer_sk INT,
    ws_ship_cdemo_sk INT,
    ws_ship_hdemo_sk INT,
    ws_ship_addr_sk INT,
    ws_web_page_sk INT,
    ws_web_site_sk INT,
    ws_ship_mode_sk INT,
    ws_warehouse_sk INT,
    ws_promo_sk INT,
    ws_order_number INT,
    ws_quantity INT,
    ws_wholesale_cost DECIMAL(7, 2),
    ws_list_price DECIMAL(7, 2),
    ws_sales_price DECIMAL(7, 2),
    ws_ext_discount_amt DECIMAL(7, 2),
    ws_ext_sales_price DECIMAL(7, 2),
    ws_ext_wholesale_cost DECIMAL(7, 2),
    ws_ext_list_price DECIMAL(7, 2),
    ws_ext_tax DECIMAL(7, 2),
    ws_coupon_amt DECIMAL(7, 2),
    ws_ext_ship_cost DECIMAL(7, 2),
    ws_net_paid DECIMAL(7, 2),
    ws_net_paid_inc_tax DECIMAL(7, 2),
    ws_net_paid_inc_ship DECIMAL(7, 2),
    ws_net_paid_inc_ship_tax DECIMAL(7, 2),
    ws_net_profit DECIMAL(7, 2),
    PRIMARY KEY (ws_item_sk, ws_order_number)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/web_sales.parquet';

CREATE EXTERNAL TABLE web_site
(
    web_site_sk INT,
    web_site_id VARCHAR,
    web_rec_start_date DATE,
    web_rec_end_date DATE,
    web_name VARCHAR,
    web_open_date_sk INT,
    web_close_date_sk INT,
    web_class VARCHAR,
    web_manager VARCHAR,
    web_mkt_id INT,
    web_mkt_class VARCHAR,
    web_mkt_desc VARCHAR,
    web_market_manager VARCHAR,
    web_company_id INT,
    web_company_name VARCHAR,
    web_street_number VARCHAR,
    web_street_name VARCHAR,
    web_street_type VARCHAR,
    web_suite_number VARCHAR,
    web_city VARCHAR,
    web_county VARCHAR,
    web_state VARCHAR,
    web_zip VARCHAR,
    web_country VARCHAR,
    web_gmt_offset DECIMAL(5, 2),
    web_tax_percentage DECIMAL(5, 2),
    PRIMARY KEY (web_site_sk)
) STORED AS PARQUET LOCATION '${DATA_DIR:-data}/tpcds_sf${BENCH_SIZE:-1}/web_site.parquet';
