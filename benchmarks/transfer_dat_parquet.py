#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import pyarrow as pa
import pyarrow.parquet as pq
import chardet
from datetime import datetime
import pandas as pd
import re
from decimal import Decimal, getcontext
import argparse


# Define TPC-DS table schemas with column names
TABLE_SCHEMAS = {
    "catalog_sales": {
        "columns": [
            "cs_sold_date_sk", "cs_sold_time_sk", "cs_ship_date_sk", "cs_bill_customer_sk",
            "cs_bill_cdemo_sk", "cs_bill_hdemo_sk", "cs_bill_addr_sk", "cs_ship_customer_sk",
            "cs_ship_cdemo_sk", "cs_ship_hdemo_sk", "cs_ship_addr_sk", "cs_call_center_sk",
            "cs_catalog_page_sk", "cs_ship_mode_sk", "cs_warehouse_sk", "cs_item_sk",
            "cs_promo_sk", "cs_order_number", "cs_quantity", "cs_wholesale_cost",
            "cs_list_price", "cs_sales_price", "cs_ext_discount_amt", "cs_ext_sales_price",
            "cs_ext_wholesale_cost", "cs_ext_list_price", "cs_ext_tax", "cs_coupon_amt",
            "cs_ext_ship_cost", "cs_net_paid", "cs_net_paid_inc_tax",
            "cs_net_paid_inc_ship", "cs_net_paid_inc_ship_tax", "cs_net_profit"
        ],
        "dtype": {
            "cs_sold_date_sk": "int32",
            "cs_sold_time_sk": "int32",
            "cs_ship_date_sk": "int32",
            "cs_bill_customer_sk": "int32",
            "cs_bill_cdemo_sk": "int32",
            "cs_bill_hdemo_sk": "int32",
            "cs_bill_addr_sk": "int32",
            "cs_ship_customer_sk": "int32",
            "cs_ship_cdemo_sk": "int32",
            "cs_ship_hdemo_sk": "int32",
            "cs_ship_addr_sk": "int32",
            "cs_call_center_sk": "int32",
            "cs_catalog_page_sk": "int32",
            "cs_ship_mode_sk": "int32",
            "cs_warehouse_sk": "int32",
            "cs_item_sk": "int32",
            "cs_promo_sk": "int32",
            "cs_order_number": "int64",
            "cs_quantity": "int32",
            "cs_wholesale_cost": "float64",
            "cs_list_price": "float64",
            "cs_sales_price": "float64",
            "cs_ext_discount_amt": "float64",
            "cs_ext_sales_price": "float64",
            "cs_ext_wholesale_cost": "float64",
            "cs_ext_list_price": "float64",
            "cs_ext_tax": "float64",
            "cs_coupon_amt": "float64",
            "cs_ext_ship_cost": "float64",
            "cs_net_paid": "float64",
            "cs_net_paid_inc_tax": "float64",
            "cs_net_paid_inc_ship": "float64",
            "cs_net_paid_inc_ship_tax": "float64",
            "cs_net_profit": "float64",
        }
    },
    "catalog_returns": {
        "columns": [
            "cr_returned_date_sk", "cr_returned_time_sk", "cr_item_sk",
            "cr_refunded_customer_sk", "cr_refunded_cdemo_sk", "cr_refunded_hdemo_sk",
            "cr_refunded_addr_sk", "cr_returning_customer_sk", "cr_returning_cdemo_sk",
            "cr_returning_hdemo_sk", "cr_returning_addr_sk", "cr_call_center_sk",
            "cr_catalog_page_sk", "cr_ship_mode_sk", "cr_warehouse_sk", "cr_reason_sk",
            "cr_order_number", "cr_return_quantity", "cr_return_amount",
            "cr_return_tax", "cr_return_amt_inc_tax", "cr_fee", "cr_return_ship_cost",
            "cr_refunded_cash", "cr_reversed_charge", "cr_store_credit", "cr_net_loss"
        ],
        "dtype": {
            "cr_returned_date_sk": "int32",
            "cr_returned_time_sk": "int32",
            "cr_item_sk": "int32",
            "cr_refunded_customer_sk": "int32",
            "cr_refunded_cdemo_sk": "int32",
            "cr_refunded_hdemo_sk": "int32",
            "cr_refunded_addr_sk": "int32",
            "cr_returning_customer_sk": "int32",
            "cr_returning_cdemo_sk": "int32",
            "cr_returning_hdemo_sk": "int32",
            "cr_returning_addr_sk": "int32",
            "cr_call_center_sk": "int32",
            "cr_catalog_page_sk": "int32",
            "cr_ship_mode_sk": "int32",
            "cr_warehouse_sk": "int32",
            "cr_reason_sk": "int32",
            "cr_order_number": "int64",
            "cr_return_quantity": "int32",
            "cr_return_amount": "float64",
            "cr_return_tax": "float64",
            "cr_return_amt_inc_tax": "float64",
            "cr_fee": "float64",
            "cr_return_ship_cost": "float64",
            "cr_refunded_cash": "float64",
            "cr_reversed_charge": "float64",
            "cr_store_credit": "float64",
            "cr_net_loss": "float64",
        }
    },
    "web_returns": {
    "columns": [
        "wr_returned_date_sk", "wr_returned_time_sk", "wr_item_sk",
        "wr_refunded_customer_sk", "wr_refunded_cdemo_sk", "wr_refunded_hdemo_sk",
        "wr_refunded_addr_sk", "wr_returning_customer_sk", "wr_returning_cdemo_sk",
        "wr_returning_hdemo_sk", "wr_returning_addr_sk", "wr_web_page_sk",
        "wr_reason_sk", "wr_order_number", "wr_return_quantity", "wr_return_amt",
        "wr_return_tax", "wr_return_amt_inc_tax", "wr_fee", "wr_return_ship_cost",
        "wr_refunded_cash", "wr_reversed_charge", "wr_account_credit", "wr_net_loss"
    ],
    "dtype": {
        "wr_returned_date_sk": "int32",
        "wr_returned_time_sk": "int32",
        "wr_item_sk": "int32",
        "wr_refunded_customer_sk": "int32",
        "wr_refunded_cdemo_sk": "int32",
        "wr_refunded_hdemo_sk": "int32",
        "wr_refunded_addr_sk": "int32",
        "wr_returning_customer_sk": "int32",
        "wr_returning_cdemo_sk": "int32",
        "wr_returning_hdemo_sk": "int32",
        "wr_returning_addr_sk": "int32",
        "wr_web_page_sk": "int32",
        "wr_reason_sk": "int32",
        "wr_order_number": "int64",
        "wr_return_quantity": "int32",
        "wr_return_amt": "float64",
        "wr_return_tax": "float64",
        "wr_return_amt_inc_tax": "float64",
        "wr_fee": "float64",
        "wr_return_ship_cost": "float64",
        "wr_refunded_cash": "float64",
        "wr_reversed_charge": "float64",
        "wr_account_credit": "float64",
        "wr_net_loss": "float64",
    }
    },
    "call_center": {
        "columns": [
            "cc_call_center_sk", "cc_call_center_id", "cc_rec_start_date", "cc_rec_end_date",
            "cc_closed_date_sk", "cc_open_date_sk", "cc_name", "cc_class", "cc_employees",
            "cc_sq_ft", "cc_hours", "cc_manager", "cc_mkt_id", "cc_mkt_class", "cc_mkt_desc",
            "cc_market_manager", "cc_division", "cc_division_name", "cc_company",
            "cc_company_name", "cc_street_number", "cc_street_name", "cc_street_type",
            "cc_suite_number", "cc_city", "cc_county", "cc_state", "cc_zip", "cc_country",
            "cc_gmt_offset", "cc_tax_percentage"
        ],
        "dtype": {
            "cc_call_center_sk": "int32",
            "cc_call_center_id": "string",
            "cc_rec_start_date": "datetime64[ns]",
            "cc_rec_end_date": "datetime64[ns]",
            "cc_closed_date_sk": "int32",
            "cc_open_date_sk": "int32",
            "cc_name": "string",
            "cc_class": "string",
            "cc_employees": "int32",
            "cc_sq_ft": "int32",
            "cc_hours": "string",
            "cc_manager": "string",
            "cc_mkt_id": "int32",
            "cc_mkt_class": "string",
            "cc_mkt_desc": "string",
            "cc_market_manager": "string",
            "cc_division": "int32",
            "cc_division_name": "string",
            "cc_company": "int32",
            "cc_company_name": "string",
            "cc_street_number": "string",
            "cc_street_name": "string",
            "cc_street_type": "string",
            "cc_suite_number": "string",
            "cc_city": "string",
            "cc_county": "string",
            "cc_state": "string",
            "cc_zip": "string",
            "cc_country": "string",
            "cc_gmt_offset": "float64",
            "cc_tax_percentage": "float64",
        }
    },
    "catalog_page": {
        "columns": [
            "cp_catalog_page_sk", "cp_catalog_page_id", "cp_start_date_sk",
            "cp_end_date_sk", "cp_department", "cp_catalog_number",
            "cp_catalog_page_number", "cp_description", "cp_type"
        ],
        "dtype": {
            "cp_catalog_page_sk": "int32",
            "cp_catalog_page_id": "string",
            "cp_start_date_sk": "int32",
            "cp_end_date_sk": "int32",
            "cp_department": "string",
            "cp_catalog_number": "int32",
            "cp_catalog_page_number": "int32",
            "cp_description": "string",
            "cp_type": "string",
        }
    },
    "customer": {
        "columns": [
            "c_customer_sk", "c_customer_id", "c_current_cdemo_sk", "c_current_hdemo_sk",
            "c_current_addr_sk", "c_first_shipto_date_sk", "c_first_sales_date_sk",
            "c_salutation", "c_first_name", "c_last_name", "c_preferred_cust_flag",
            "c_birth_day", "c_birth_month", "c_birth_year", "c_birth_country",
            "c_login", "c_email_address", "c_last_review_date_sk"
        ],
        "dtype": {
            "c_customer_sk": "int32",
            "c_customer_id": "string",
            "c_current_cdemo_sk": "int32",
            "c_current_hdemo_sk": "int32",
            "c_current_addr_sk": "int32",
            "c_first_shipto_date_sk": "int32",
            "c_first_sales_date_sk": "int32",
            "c_salutation": "string",
            "c_first_name": "string",
            "c_last_name": "string",
            "c_preferred_cust_flag": "string",
            "c_birth_day": "int32",
            "c_birth_month": "int32",
            "c_birth_year": "int32",
            "c_birth_country": "string",
            "c_login": "string",
            "c_email_address": "string",
            "c_last_review_date_sk": "string",
        }
    },
    "customer_address": {
        "columns": [
            "ca_address_sk", "ca_address_id", "ca_street_number", "ca_street_name",
            "ca_street_type", "ca_suite_number", "ca_city", "ca_county", "ca_state",
            "ca_zip", "ca_country", "ca_gmt_offset", "ca_location_type"
        ],
        "dtype": {
            "ca_address_sk": "string",
            "ca_address_id": "string",
            "ca_street_number": "string",
            "ca_street_name": "string",
            "ca_street_type": "string",
            "ca_suite_number": "string",
            "ca_city": "string",
            "ca_county": "string",
            "ca_state": "string",
            "ca_zip": "string",
            "ca_country": "string",
            "ca_gmt_offset": "float64",
            "ca_location_type": "string",
        }
    },
    "customer_demographics": {
        "columns": [
            "cd_demo_sk", "cd_gender", "cd_marital_status", "cd_education_status",
            "cd_purchase_estimate", "cd_credit_rating", "cd_dep_count",
            "cd_dep_employed_count", "cd_dep_college_count"
        ],
        "dtype": {
            "cd_demo_sk": "int32",
            "cd_gender": "string",
            "cd_marital_status": "string",
            "cd_education_status": "string",
            "cd_purchase_estimate": "int32",
            "cd_credit_rating": "string",
            "cd_dep_count": "int32",
            "cd_dep_employed_count": "int32",
            "cd_dep_college_count": "int32",
        }
    },
    "date_dim": {
        "columns": [
            "d_date_sk", "d_date_id", "d_date", "d_month_seq", "d_week_seq",
            "d_quarter_seq", "d_year", "d_dow", "d_moy", "d_dom", "d_qoy",
            "d_fy_year", "d_fy_quarter_seq", "d_fy_week_seq", "d_day_name",
            "d_quarter_name", "d_holiday", "d_weekend", "d_following_holiday",
            "d_first_dom", "d_last_dom", "d_same_day_ly", "d_same_day_lq",
            "d_current_day", "d_current_week", "d_current_month",
            "d_current_quarter", "d_current_year"
        ],
        "dtype": {
            "d_date_sk": "int32",
            "d_date_id": "string",
            "d_date": "datetime64[ns]",
            "d_month_seq": "int32",
            "d_week_seq": "int32",
            "d_quarter_seq": "int32",
            "d_year": "int32",
            "d_dow": "int32",
            "d_moy": "int32",
            "d_dom": "int32",
            "d_qoy": "int32",
            "d_fy_year": "int32",
            "d_fy_quarter_seq": "int32",
            "d_fy_week_seq": "int32",
            "d_day_name": "string",
            "d_quarter_name": "string",
            "d_holiday": "string",
            "d_weekend": "string",
            "d_following_holiday": "string",
            "d_first_dom": "int32",
            "d_last_dom": "int32",
            "d_same_day_ly": "int32",
            "d_same_day_lq": "int32",
            "d_current_day": "string",
            "d_current_week": "string",
            "d_current_month": "string",
            "d_current_quarter": "string",
            "d_current_year": "string",
        }
    },
    "household_demographics": {
        "columns": [
            "hd_demo_sk", "hd_income_band_sk", "hd_buy_potential",
            "hd_dep_count", "hd_vehicle_count"
        ],
        "dtype": {
            "hd_demo_sk": "Int64",
            "hd_income_band_sk": "Int64",
            "hd_buy_potential": "string",
            "hd_dep_count": "Int64",
            "hd_vehicle_count": "Int64"
        }
    },
    "item": {
        "columns": [
            "i_item_sk", "i_item_id", "i_rec_start_date", "i_rec_end_date",
            "i_item_desc", "i_current_price", "i_wholesale_cost", "i_brand_id",
            "i_brand", "i_class_id", "i_class", "i_category_id", "i_category",
            "i_manufact_id", "i_manufact", "i_size", "i_formulation", "i_color",
            "i_units", "i_container", "i_manager_id", "i_product_name"
        ],
        "dtype": {
            "i_item_sk": "Int64",
            "i_item_id": "string",
            "i_rec_start_date": "datetime64[ns]",
            "i_rec_end_date": "datetime64[ns]",
            "i_item_desc": "string",
            "i_current_price": "float64",
            "i_wholesale_cost": "float64",
            "i_brand_id": "Int64",
            "i_brand": "string",
            "i_class_id": "Int64",
            "i_class": "string",
            "i_category_id": "Int64",
            "i_category": "string",
            "i_manufact_id": "Int64",
            "i_manufact": "string",
            "i_size": "string",
            "i_formulation": "string",
            "i_color": "string",
            "i_units": "string",
            "i_container": "string",
            "i_manager_id": "Int64",
            "i_product_name": "string"
        }
    },
    "promotion": {
        "columns": [
            "p_promo_sk", "p_promo_id", "p_start_date_sk", "p_end_date_sk",
            "p_item_sk", "p_cost", "p_response_target", "p_promo_name",
            "p_channel_dmail", "p_channel_email", "p_channel_catalog", "p_channel_tv",
            "p_channel_radio", "p_channel_press", "p_channel_event", "p_channel_demo",
            "p_channel_details", "p_purpose", "p_discount_active"
        ],
        "dtype": {
            "p_promo_sk": "Int64",
            "p_promo_id": "string",
            "p_start_date_sk": "Int64",
            "p_end_date_sk": "Int64",
            "p_item_sk": "Int64",
            "p_cost": "float64",
            "p_response_target": "Int64",
            "p_promo_name": "string",
            "p_channel_dmail": "string",
            "p_channel_email": "string",
            "p_channel_catalog": "string",
            "p_channel_tv": "string",
            "p_channel_radio": "string",
            "p_channel_press": "string",
            "p_channel_event": "string",
            "p_channel_demo": "string",
            "p_channel_details": "string",
            "p_purpose": "string",
            "p_discount_active": "string"
        }
    },
     "web_sales": {
        "columns": [
            "ws_sold_date_sk", "ws_sold_time_sk", "ws_ship_date_sk", "ws_item_sk",
            "ws_bill_customer_sk", "ws_bill_cdemo_sk", "ws_bill_hdemo_sk", "ws_bill_addr_sk",
            "ws_ship_customer_sk", "ws_ship_cdemo_sk", "ws_ship_hdemo_sk", "ws_ship_addr_sk",
            "ws_web_page_sk", "ws_web_site_sk", "ws_ship_mode_sk", "ws_warehouse_sk",
            "ws_promo_sk", "ws_order_number", "ws_quantity", "ws_wholesale_cost",
            "ws_list_price", "ws_sales_price", "ws_ext_discount_amt", "ws_ext_sales_price",
            "ws_ext_wholesale_cost", "ws_ext_list_price", "ws_ext_tax", "ws_coupon_amt",
            "ws_ext_ship_cost", "ws_net_paid", "ws_net_paid_inc_tax", "ws_net_paid_inc_ship",
            "ws_net_paid_inc_ship_tax", "ws_net_profit"
        ],
        "dtype": {
            "ws_sold_date_sk": "Int64",
            "ws_sold_time_sk": "Int64",
            "ws_ship_date_sk": "Int64",
            "ws_item_sk": "Int64",
            "ws_bill_customer_sk": "Int64",
            "ws_bill_cdemo_sk": "Int64",
            "ws_bill_hdemo_sk": "Int64",
            "ws_bill_addr_sk": "Int64",
            "ws_ship_customer_sk": "Int64",
            "ws_ship_cdemo_sk": "Int64",
            "ws_ship_hdemo_sk": "Int64",
            "ws_ship_addr_sk": "Int64",
            "ws_web_page_sk": "Int64",
            "ws_web_site_sk": "Int64",
            "ws_ship_mode_sk": "Int64",
            "ws_warehouse_sk": "Int64",
            "ws_promo_sk": "Int64",
            "ws_order_number": "Int64",
            "ws_quantity": "Int64",
            "ws_wholesale_cost": "float64",
            "ws_list_price": "float64",
            "ws_sales_price": "float64",
            "ws_ext_discount_amt": "float64",
            "ws_ext_sales_price": "float64",
            "ws_ext_wholesale_cost": "float64",
            "ws_ext_list_price": "float64",
            "ws_ext_tax": "float64",
            "ws_coupon_amt": "float64",
            "ws_ext_ship_cost": "float64",
            "ws_net_paid": "float64",
            "ws_net_paid_inc_tax": "float64",
            "ws_net_paid_inc_ship": "float64",
            "ws_net_paid_inc_ship_tax": "float64",
            "ws_net_profit": "float64"
        }
    },
    "store": {
        "columns": [
            "s_store_sk", "s_store_id", "s_rec_start_date", "s_rec_end_date", 
            "s_closed_date_sk", "s_store_name", "s_number_employees", 
            "s_floor_space", "s_hours", "s_manager", "s_market_id", 
            "s_geography_class", "s_market_desc", "s_market_manager", 
            "s_division_id", "s_division_name", "s_company_id", "s_company_name", 
            "s_street_number", "s_street_name", "s_street_type", "s_suite_number", 
            "s_city", "s_county", "s_state", "s_zip", "s_country", 
            "s_gmt_offset", "s_tax_precentage"
        ],
        "dtype": {
            "s_store_sk": "Int64",
            "s_store_id": "string",
            "s_rec_start_date": "datetime64[ns]",
            "s_rec_end_date": "datetime64[ns]",
            "s_closed_date_sk": "Int64",
            "s_store_name": "string",
            "s_number_employees": "Int64",
            "s_floor_space": "Int64",
            "s_hours": "string",
            "s_manager": "string",
            "s_market_id": "Int64",
            "s_geography_class": "string",
            "s_market_desc": "string",
            "s_market_manager": "string",
            "s_division_id": "Int64",
            "s_division_name": "string",
            "s_company_id": "Int64",
            "s_company_name": "string",
            "s_street_number": "string",
            "s_street_name": "string",
            "s_street_type": "string",
            "s_suite_number": "string",
            "s_city": "string",
            "s_county": "string",
            "s_state": "string",
            "s_zip": "string",
            "s_country": "string",
            "s_gmt_offset": "float64",
            "s_tax_precentage": "float64"
        }
    },
    "inventory": {
        "columns": [
            "inv_date_sk", "inv_item_sk", "inv_warehouse_sk", "inv_quantity_on_hand"
        ],
        "dtype": {
            "inv_date_sk": "Int64",
            "inv_item_sk": "Int64",
            "inv_warehouse_sk": "Int64",
            "inv_quantity_on_hand": "Int64"
        }
    },
    "ship_mode": {
        "columns": [
            "sm_ship_mode_sk", "sm_ship_mode_id", "sm_type", "sm_code", 
            "sm_carrier", "sm_contract"
        ],
        "dtype": {
            "sm_ship_mode_sk": "Int64",
            "sm_ship_mode_id": "string",
            "sm_type": "string",
            "sm_code": "string",
            "sm_carrier": "string",
            "sm_contract": "string"
        }
    },
    "income_band": {
        "columns": [
            "ib_income_band_sk", "ib_lower_bound", "ib_upper_bound"
        ],
        "dtype": {
            "ib_income_band_sk": "Int64",
            "ib_lower_bound": "Int64",
            "ib_upper_bound": "Int64"
        }
    },
    "reason": {
        "columns": [
            "r_reason_sk", "r_reason_id", "r_reason_desc"
        ],
        "dtype": {
            "r_reason_sk": "Int64",
            "r_reason_id": "string",
            "r_reason_desc": "string"
        }
    },
    "time_dim": {
        "columns": [
            "t_time_sk", "t_time_id", "t_time", "t_hour", "t_minute", 
            "t_second", "t_am_pm", "t_shift", "t_sub_shift", "t_meal_time"
        ],
        "dtype": {
            "t_time_sk": "Int64",
            "t_time_id": "string",
            "t_time": "Int64",
            "t_hour": "Int64",
            "t_minute": "Int64",
            "t_second": "Int64",
            "t_am_pm": "string",
            "t_shift": "string",
            "t_sub_shift": "string",
            "t_meal_time": "string"
        }
    },
    "web_page": {
        "columns": [
            "wp_web_page_sk", "wp_web_page_id", "wp_rec_start_date", "wp_rec_end_date",
            "wp_creation_date_sk", "wp_access_date_sk", "wp_autogen_flag", 
            "wp_customer_sk", "wp_url", "wp_type", 
            "wp_char_count", "wp_link_count", "wp_image_count", "wp_max_ad_count"
        ],
        "dtype": {
            "wp_web_page_sk": "int32",
            "wp_web_page_id": "string",
            "wp_rec_start_date": "date32",
            "wp_rec_end_date": "date32",
            "wp_creation_date_sk": "int32",
            "wp_access_date_sk": "int32",
            "wp_autogen_flag": "string",
            "wp_customer_sk": "int32",
            "wp_url": "string",
            "wp_type": "string",
            "wp_char_count": "int32",
            "wp_link_count": "int32",
            "wp_image_count": "int32",
            "wp_max_ad_count": "int32"
        }
    },
    "web_site": {
        "columns": [
            "web_site_sk", "web_site_id", "web_rec_start_date", "web_rec_end_date",
            "web_name", "web_open_date_sk", "web_close_date_sk", "web_class",
            "web_manager", "web_mkt_id", "web_mkt_class", "web_mkt_desc",
            "web_market_manager", "web_company_id", "web_company_name",
            "web_street_number", "web_street_name", "web_street_type",
            "web_suite_number", "web_city", "web_county", "web_state",
            "web_zip", "web_country", "web_gmt_offset", "web_tax_percentage"
        ],
        "dtype": {
            "web_site_sk": "int32",
            "web_site_id": "string",
            "web_rec_start_date": "date32",
            "web_rec_end_date": "date32",
            "web_name": "string",
            "web_open_date_sk": "int32",
            "web_close_date_sk": "int32",
            "web_class": "string",
            "web_manager": "string",
            "web_mkt_id": "int32",
            "web_mkt_class": "string",
            "web_mkt_desc": "string",
            "web_market_manager": "string",
            "web_company_id": "int32",
            "web_company_name": "string",
            "web_street_number": "string",
            "web_street_name": "string",
            "web_street_type": "string",
            "web_suite_number": "string",
            "web_city": "string",
            "web_county": "string",
            "web_state": "string",
            "web_zip": "string",
            "web_country": "string",
            "web_gmt_offset": "decimal(5, 2)",
            "web_tax_percentage": "decimal(5, 2)"
        }
    },
    "store_sales": {
        "columns": [
            "ss_sold_date_sk", "ss_sold_time_sk", "ss_item_sk", "ss_customer_sk",
            "ss_cdemo_sk", "ss_hdemo_sk", "ss_addr_sk", "ss_store_sk", "ss_promo_sk",
            "ss_ticket_number", "ss_quantity", "ss_wholesale_cost", "ss_list_price",
            "ss_sales_price", "ss_ext_discount_amt", "ss_ext_sales_price",
            "ss_ext_wholesale_cost", "ss_ext_list_price", "ss_ext_tax", "ss_coupon_amt",
            "ss_net_paid", "ss_net_paid_inc_tax", "ss_net_profit"
        ],
        "dtype": {
            "ss_sold_date_sk": "int32",
            "ss_sold_time_sk": "int32",
            "ss_item_sk": "int32",
            "ss_customer_sk": "int32",
            "ss_cdemo_sk": "int32",
            "ss_hdemo_sk": "int32",
            "ss_addr_sk": "int32",
            "ss_store_sk": "int32",
            "ss_promo_sk": "int32",
            "ss_ticket_number": "int64",
            "ss_quantity": "int32",
            "ss_wholesale_cost": "decimal(7, 2)",
            "ss_list_price": "decimal(7, 2)",
            "ss_sales_price": "decimal(7, 2)",
            "ss_ext_discount_amt": "decimal(7, 2)",
            "ss_ext_sales_price": "decimal(7, 2)",
            "ss_ext_wholesale_cost": "decimal(7, 2)",
            "ss_ext_list_price": "decimal(7, 2)",
            "ss_ext_tax": "decimal(7, 2)",
            "ss_coupon_amt": "decimal(7, 2)",
            "ss_net_paid": "decimal(7, 2)",
            "ss_net_paid_inc_tax": "decimal(7, 2)",
            "ss_net_profit": "decimal(7, 2)"
        }
    },
    "store_returns": {
        "columns": [
            "sr_returned_date_sk", "sr_return_time_sk", "sr_item_sk", "sr_customer_sk",
            "sr_cdemo_sk", "sr_hdemo_sk", "sr_addr_sk", "sr_store_sk", "sr_reason_sk",
            "sr_ticket_number", "sr_return_quantity", "sr_return_amt", "sr_return_tax",
            "sr_return_amt_inc_tax", "sr_fee", "sr_return_ship_cost", "sr_refunded_cash",
            "sr_reversed_charge", "sr_store_credit", "sr_net_loss"
        ],
        "dtype": {
            "sr_returned_date_sk": "int32",
            "sr_return_time_sk": "int32",
            "sr_item_sk": "int32",
            "sr_customer_sk": "int32",
            "sr_cdemo_sk": "int32",
            "sr_hdemo_sk": "int32",
            "sr_addr_sk": "int32",
            "sr_store_sk": "int32",
            "sr_reason_sk": "int32",
            "sr_ticket_number": "int64",
            "sr_return_quantity": "int32",
            "sr_return_amt": "decimal(7, 2)",
            "sr_return_tax": "decimal(7, 2)",
            "sr_return_amt_inc_tax": "decimal(7, 2)",
            "sr_fee": "decimal(7, 2)",
            "sr_return_ship_cost": "decimal(7, 2)",
            "sr_refunded_cash": "decimal(7, 2)",
            "sr_reversed_charge": "decimal(7, 2)",
            "sr_store_credit": "decimal(7, 2)",
            "sr_net_loss": "decimal(7, 2)"
        }
    },
    "warehouse": {
        "columns": [
            "w_warehouse_sk", "w_warehouse_id", "w_warehouse_name",
            "w_warehouse_sq_ft", "w_street_number", "w_street_name",
            "w_street_type", "w_suite_number", "w_city", "w_county",
            "w_state", "w_zip", "w_country", "w_gmt_offset"
        ],
        "dtype": {
            "w_warehouse_sk": "int32",
            "w_warehouse_id": "string",
            "w_warehouse_name": "string",
            "w_warehouse_sq_ft": "int32",
            "w_street_number": "string",
            "w_street_name": "string",
            "w_street_type": "string",
            "w_suite_number": "string",
            "w_city": "string",
            "w_county": "string",
            "w_state": "string",
            "w_zip": "string",
            "w_country": "string",
            "w_gmt_offset": "decimal(5, 2)"
        }
    },
}


def detect_encoding(file_path, default_encoding="utf-8"):
    """
    Detect the encoding of a file using chardet. Falls back to a default encoding if detection fails.
    """
    try:
        with open(file_path, 'rb') as f:
            result = chardet.detect(f.read())
            return result['encoding'] or default_encoding
    except Exception as e:
        print(f"Error detecting encoding for {file_path}: {e}")
        return default_encoding


def convert_and_delete_dat_files(data_dir, default_value=0):
    """
    Convert .dat files in the specified directory to .parquet format and delete the original .dat files.

    Args:
        data_dir (str): The directory containing the .dat files.
        default_value (int, optional): The default value to fill missing data. Defaults to 0.
    """
    if not os.path.exists(data_dir):
        print(f"Error: The directory {data_dir} does not exist.")
        return

    files_processed = False
    for file_name in os.listdir(data_dir):
        if file_name.endswith(".dat"):
            files_processed = True
            base_name = os.path.basename(file_name)  # Get the file name without the directory path
            table_name = os.path.splitext(base_name)[0]  # Extract table name by removing file extension
            input_file = os.path.join(data_dir, file_name)
            output_file = os.path.join(data_dir, f"{table_name}.parquet")

            print(f"Processing: {input_file} -> {output_file}")

            if table_name in TABLE_SCHEMAS:
                schema = TABLE_SCHEMAS[table_name]
                columns = schema.get("columns")
                dtype = schema.get("dtype", {})
            else:
                print(f"Warning: Table {table_name} is not defined in the schema. Skipping.")
                continue

            try:
                # Detect encoding and read the .dat file
                encoding = detect_encoding(input_file)
                df = pd.read_csv(
                    input_file,
                    sep="|",
                    names=columns,
                    header=None,
                    engine="python",
                    skipfooter=1,
                    na_values=["NULL", ""],
                    dtype="object",  # Read as objects (strings) to avoid type issues during parsing
                    encoding=encoding,  # Use detected encoding
                )

                # Replace missing values
                df = df.fillna(default_value)

                # Convert column types based on schema
                for col, col_type in dtype.items():
                    try:
                        if col_type == "Int64" or col_type == "int64":
                            df[col] = pd.to_numeric(df[col], errors="coerce")
                            df[col] = df[col].fillna(default_value).astype(int).astype("Int64")
                        elif col_type == "int32":
                            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int).astype('int32')
                        elif col_type == "float64":
                            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(default_value).astype("float64")
                        elif col_type == "datetime64[ns]":
                            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors="coerce").fillna(pd.Timestamp("1970-01-01"))
                        elif col_type == "date32":
                            df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors="coerce").dt.date.fillna(pd.Timestamp("1970-01-01").date())
                        elif col_type == "string":
                            df[col] = df[col].fillna("").astype(str)
                        elif re.match(r"decimal\(\d+, \d+\)", col_type):
                            match = re.match(r"decimal\((\d+), (\d+)\)", col_type)
                            precision = int(match.group(1))  # Extract precision
                            scale = int(match.group(2))      # Extract scale
                            getcontext().prec = precision
                            df[col] = df[col].apply(
                                lambda x: Decimal(str(x)).quantize(Decimal(f"1.{'0' * scale}")) 
                                if pd.notnull(x) and str(x).replace(".", "", 1).isdigit() 
                                else Decimal(f"0.{'0' * scale}")
                            )
                    except Exception as e:
                        print(f"Error converting column {col}: {e}")
                        continue

                # Convert the DataFrame to Parquet format
                table = pa.Table.from_pandas(df)
                pq.write_table(table, output_file)

                print(f"Conversion completed: {output_file}")

                # Delete the original .dat file
                os.remove(input_file)
                print(f"Original file deleted: {input_file}")

            except Exception as e:
                print(f"Error processing {table_name}: {e}")

    if not files_processed:
        print(f"No `.dat` files found in the directory {data_dir}.")


if __name__ == "__main__":
    # Use argparse to handle command-line arguments
    parser = argparse.ArgumentParser(description="Convert .dat files to .parquet format.")
    parser.add_argument(
        "--dir",
        required=True,
        help="Path to the directory containing .dat files."
    )
    args = parser.parse_args()

    # Set the directory from command-line arguments
    data_dir = args.dir

    # Check if the directory exists
    if not os.path.exists(data_dir):
        print(f"Error: The specified directory '{data_dir}' does not exist.")
    else:
        pd.set_option('future.no_silent_downcasting', True)
        convert_and_delete_dat_files(data_dir)