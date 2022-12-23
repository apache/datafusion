// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::Result;
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::fs;
use std::sync::Arc;

#[tokio::test]
async fn q1() -> Result<()> {
    regression_test(1).await
}

#[tokio::test]
async fn q2() -> Result<()> {
    regression_test(2).await
}

#[tokio::test]
async fn q3() -> Result<()> {
    regression_test(3).await
}

#[ignore] // https://github.com/apache/arrow-datafusion/issues/4065
#[tokio::test]
async fn q4() -> Result<()> {
    regression_test(4).await
}

#[tokio::test]
async fn q5() -> Result<()> {
    regression_test(5).await
}

#[tokio::test]
async fn q6() -> Result<()> {
    regression_test(6).await
}

#[tokio::test]
async fn q7() -> Result<()> {
    regression_test(7).await
}

#[tokio::test]
async fn q8() -> Result<()> {
    regression_test(8).await
}

#[tokio::test]
async fn q9() -> Result<()> {
    regression_test(9).await
}

#[tokio::test]
async fn q10() -> Result<()> {
    regression_test(10).await
}

#[tokio::test]
async fn q11() -> Result<()> {
    regression_test(11).await
}

#[tokio::test]
async fn q12() -> Result<()> {
    regression_test(12).await
}

#[tokio::test]
async fn q13() -> Result<()> {
    regression_test(13).await
}

#[tokio::test]
async fn q14() -> Result<()> {
    regression_test(14).await
}

#[tokio::test]
async fn q15() -> Result<()> {
    regression_test(15).await
}

#[tokio::test]
async fn q16() -> Result<()> {
    regression_test(16).await
}

#[tokio::test]
async fn q17() -> Result<()> {
    regression_test(17).await
}

#[tokio::test]
async fn q18() -> Result<()> {
    regression_test(18).await
}

#[tokio::test]
async fn q19() -> Result<()> {
    regression_test(19).await
}

#[tokio::test]
async fn q20() -> Result<()> {
    regression_test(20).await
}

#[tokio::test]
async fn q21() -> Result<()> {
    regression_test(21).await
}

#[tokio::test]
async fn q22() -> Result<()> {
    regression_test(22).await
}

#[tokio::test]
async fn q23() -> Result<()> {
    regression_test(23).await
}

#[tokio::test]
async fn q24() -> Result<()> {
    regression_test(24).await
}

#[tokio::test]
async fn q25() -> Result<()> {
    regression_test(25).await
}

#[tokio::test]
async fn q26() -> Result<()> {
    regression_test(26).await
}

#[tokio::test]
async fn q27() -> Result<()> {
    regression_test(27).await
}

#[tokio::test]
async fn q28() -> Result<()> {
    regression_test(28).await
}

#[tokio::test]
async fn q29() -> Result<()> {
    regression_test(29).await
}

#[tokio::test]
async fn q30() -> Result<()> {
    regression_test(30).await
}

#[tokio::test]
async fn q31() -> Result<()> {
    regression_test(31).await
}

#[tokio::test]
async fn q32() -> Result<()> {
    regression_test(32).await
}

#[tokio::test]
async fn q33() -> Result<()> {
    regression_test(33).await
}

#[tokio::test]
async fn q34() -> Result<()> {
    regression_test(34).await
}

#[tokio::test]
async fn q35() -> Result<()> {
    regression_test(35).await
}

#[tokio::test]
async fn q36() -> Result<()> {
    regression_test(36).await
}

#[tokio::test]
async fn q37() -> Result<()> {
    regression_test(37).await
}

#[tokio::test]
async fn q38() -> Result<()> {
    regression_test(38).await
}

#[tokio::test]
async fn q39() -> Result<()> {
    regression_test(39).await
}

#[tokio::test]
async fn q40() -> Result<()> {
    regression_test(40).await
}

#[tokio::test]
async fn q41() -> Result<()> {
    regression_test(41).await
}

#[tokio::test]
async fn q42() -> Result<()> {
    regression_test(42).await
}

#[tokio::test]
async fn q43() -> Result<()> {
    regression_test(43).await
}

#[tokio::test]
async fn q44() -> Result<()> {
    regression_test(44).await
}

#[tokio::test]
async fn q45() -> Result<()> {
    regression_test(45).await
}

#[tokio::test]
async fn q46() -> Result<()> {
    regression_test(46).await
}

#[tokio::test]
async fn q47() -> Result<()> {
    regression_test(47).await
}

#[tokio::test]
async fn q48() -> Result<()> {
    regression_test(48).await
}

#[tokio::test]
async fn q49() -> Result<()> {
    regression_test(49).await
}

#[tokio::test]
async fn q50() -> Result<()> {
    regression_test(50).await
}

#[tokio::test]
async fn q51() -> Result<()> {
    regression_test(51).await
}

#[tokio::test]
async fn q52() -> Result<()> {
    regression_test(52).await
}

#[tokio::test]
async fn q53() -> Result<()> {
    regression_test(53).await
}

#[tokio::test]
async fn q54() -> Result<()> {
    regression_test(54).await
}

#[tokio::test]
async fn q55() -> Result<()> {
    regression_test(55).await
}

#[tokio::test]
async fn q56() -> Result<()> {
    regression_test(56).await
}

#[tokio::test]
async fn q57() -> Result<()> {
    regression_test(57).await
}

#[tokio::test]
async fn q58() -> Result<()> {
    regression_test(58).await
}

#[tokio::test]
async fn q59() -> Result<()> {
    regression_test(59).await
}

#[tokio::test]
async fn q60() -> Result<()> {
    regression_test(60).await
}

#[tokio::test]
async fn q61() -> Result<()> {
    regression_test(61).await
}

#[tokio::test]
async fn q62() -> Result<()> {
    regression_test(62).await
}

#[tokio::test]
async fn q63() -> Result<()> {
    regression_test(63).await
}

#[ignore] // https://github.com/apache/arrow-datafusion/issues/4065
#[tokio::test]
async fn q64() -> Result<()> {
    regression_test(64).await
}

#[tokio::test]
async fn q65() -> Result<()> {
    regression_test(65).await
}

#[tokio::test]
async fn q66() -> Result<()> {
    regression_test(66).await
}

#[tokio::test]
async fn q67() -> Result<()> {
    regression_test(67).await
}

#[tokio::test]
async fn q68() -> Result<()> {
    regression_test(68).await
}

#[tokio::test]
async fn q69() -> Result<()> {
    regression_test(69).await
}

#[tokio::test]
async fn q70() -> Result<()> {
    regression_test(70).await
}

#[tokio::test]
async fn q71() -> Result<()> {
    regression_test(71).await
}

#[tokio::test]
async fn q72() -> Result<()> {
    regression_test(72).await
}

#[tokio::test]
async fn q73() -> Result<()> {
    regression_test(73).await
}

#[tokio::test]
async fn q74() -> Result<()> {
    regression_test(74).await
}

#[tokio::test]
async fn q75() -> Result<()> {
    regression_test(75).await
}

#[tokio::test]
async fn q76() -> Result<()> {
    regression_test(76).await
}

#[tokio::test]
async fn q77() -> Result<()> {
    regression_test(77).await
}

#[tokio::test]
async fn q78() -> Result<()> {
    regression_test(78).await
}

#[tokio::test]
async fn q79() -> Result<()> {
    regression_test(79).await
}

#[tokio::test]
async fn q80() -> Result<()> {
    regression_test(80).await
}

#[tokio::test]
async fn q81() -> Result<()> {
    regression_test(81).await
}

#[tokio::test]
async fn q82() -> Result<()> {
    regression_test(82).await
}

#[tokio::test]
async fn q83() -> Result<()> {
    regression_test(83).await
}

#[tokio::test]
async fn q84() -> Result<()> {
    regression_test(84).await
}

#[tokio::test]
async fn q85() -> Result<()> {
    regression_test(85).await
}

#[tokio::test]
async fn q86() -> Result<()> {
    regression_test(86).await
}

#[tokio::test]
async fn q87() -> Result<()> {
    regression_test(87).await
}

#[tokio::test]
async fn q88() -> Result<()> {
    regression_test(88).await
}

#[tokio::test]
async fn q89() -> Result<()> {
    regression_test(89).await
}

#[tokio::test]
async fn q90() -> Result<()> {
    regression_test(90).await
}

#[tokio::test]
async fn q91() -> Result<()> {
    regression_test(91).await
}

#[tokio::test]
async fn q92() -> Result<()> {
    regression_test(92).await
}

#[tokio::test]
async fn q93() -> Result<()> {
    regression_test(93).await
}

#[tokio::test]
async fn q94() -> Result<()> {
    regression_test(94).await
}

#[tokio::test]
async fn q95() -> Result<()> {
    regression_test(95).await
}

#[tokio::test]
async fn q96() -> Result<()> {
    regression_test(96).await
}

#[tokio::test]
async fn q97() -> Result<()> {
    regression_test(97).await
}

#[tokio::test]
async fn q98() -> Result<()> {
    regression_test(98).await
}

#[tokio::test]
async fn q99() -> Result<()> {
    regression_test(99).await
}

async fn regression_test(query_no: u8) -> Result<()> {
    let filename = format!("tests/tpc-ds/{query_no}.sql");
    let sql = fs::read_to_string(filename).expect("Could not read query");

    let config = SessionConfig::default();
    let ctx = SessionContext::with_config(config);
    let tables = get_table_definitions();
    for table in &tables {
        ctx.register_table(
            table.name.as_str(),
            Arc::new(MemTable::try_new(Arc::new(table.schema.clone()), vec![])?),
        )?;
    }

    // some queries have multiple statements
    let sql = sql
        .split(';')
        .filter(|s| !s.trim().is_empty())
        .collect::<Vec<_>>();

    let debug = true;
    for sql in &sql {
        if debug {
            println!("Query {}: {}", query_no, sql);
        }

        // query parsing and planning using default DataFusion optimizer rules
        let df = ctx.sql(sql).await?;
        let plan = df.into_optimized_plan()?;
        let formatted_query_plan = format!("{}", plan.display_indent());

        if debug {
            println!("{}", formatted_query_plan);
        }

        // let path = format!("tests/expected/{}.txt", query_no);
        // let expected_plan_path = Path::new(&path);
        // match fs::read_to_string(expected_plan_path) {
        //     Ok(expected) => assert_eq!(expected, formatted_query_plan),
        //     Err(_) => {
        //         println!(
        //             "Writing new expected output to {}",
        //             expected_plan_path.display()
        //         );
        //         fs::write(expected_plan_path, &formatted_query_plan)?
        //     }
        // }
    }

    Ok(())
}

pub struct TableDef {
    pub name: String,
    pub schema: Schema,
}

impl TableDef {
    fn new(name: &str, schema: Schema) -> Self {
        Self {
            name: name.to_string(),
            schema,
        }
    }
}

pub fn get_table_definitions() -> Vec<TableDef> {
    vec![
        TableDef::new(
            "catalog_sales",
            Schema::new(vec![
                Field::new("cs_sold_date_sk", DataType::Int32, false),
                Field::new("cs_sold_time_sk", DataType::Int32, false),
                Field::new("cs_ship_date_sk", DataType::Int32, false),
                Field::new("cs_bill_customer_sk", DataType::Int32, false),
                Field::new("cs_bill_cdemo_sk", DataType::Int32, false),
                Field::new("cs_bill_hdemo_sk", DataType::Int32, false),
                Field::new("cs_bill_addr_sk", DataType::Int32, false),
                Field::new("cs_ship_customer_sk", DataType::Int32, false),
                Field::new("cs_ship_cdemo_sk", DataType::Int32, false),
                Field::new("cs_ship_hdemo_sk", DataType::Int32, false),
                Field::new("cs_ship_addr_sk", DataType::Int32, false),
                Field::new("cs_call_center_sk", DataType::Int32, false),
                Field::new("cs_catalog_page_sk", DataType::Int32, false),
                Field::new("cs_ship_mode_sk", DataType::Int32, false),
                Field::new("cs_warehouse_sk", DataType::Int32, false),
                Field::new("cs_item_sk", DataType::Int32, false),
                Field::new("cs_promo_sk", DataType::Int32, false),
                Field::new("cs_order_number", DataType::Int64, false),
                Field::new("cs_quantity", DataType::Int32, false),
                Field::new("cs_wholesale_cost", DataType::Decimal128(7, 2), false),
                Field::new("cs_list_price", DataType::Decimal128(7, 2), false),
                Field::new("cs_sales_price", DataType::Decimal128(7, 2), false),
                Field::new("cs_ext_discount_amt", DataType::Decimal128(7, 2), false),
                Field::new("cs_ext_sales_price", DataType::Decimal128(7, 2), false),
                Field::new("cs_ext_wholesale_cost", DataType::Decimal128(7, 2), false),
                Field::new("cs_ext_list_price", DataType::Decimal128(7, 2), false),
                Field::new("cs_ext_tax", DataType::Decimal128(7, 2), false),
                Field::new("cs_coupon_amt", DataType::Decimal128(7, 2), false),
                Field::new("cs_ext_ship_cost", DataType::Decimal128(7, 2), false),
                Field::new("cs_net_paid", DataType::Decimal128(7, 2), false),
                Field::new("cs_net_paid_inc_tax", DataType::Decimal128(7, 2), false),
                Field::new("cs_net_paid_inc_ship", DataType::Decimal128(7, 2), false),
                Field::new(
                    "cs_net_paid_inc_ship_tax",
                    DataType::Decimal128(7, 2),
                    false,
                ),
                Field::new("cs_net_profit", DataType::Decimal128(7, 2), false),
            ]),
        ),
        TableDef::new(
            "catalog_returns",
            Schema::new(vec![
                Field::new("cr_returned_date_sk", DataType::Int32, false),
                Field::new("cr_returned_time_sk", DataType::Int32, false),
                Field::new("cr_item_sk", DataType::Int32, false),
                Field::new("cr_refunded_customer_sk", DataType::Int32, false),
                Field::new("cr_refunded_cdemo_sk", DataType::Int32, false),
                Field::new("cr_refunded_hdemo_sk", DataType::Int32, false),
                Field::new("cr_refunded_addr_sk", DataType::Int32, false),
                Field::new("cr_returning_customer_sk", DataType::Int32, false),
                Field::new("cr_returning_cdemo_sk", DataType::Int32, false),
                Field::new("cr_returning_hdemo_sk", DataType::Int32, false),
                Field::new("cr_returning_addr_sk", DataType::Int32, false),
                Field::new("cr_call_center_sk", DataType::Int32, false),
                Field::new("cr_catalog_page_sk", DataType::Int32, false),
                Field::new("cr_ship_mode_sk", DataType::Int32, false),
                Field::new("cr_warehouse_sk", DataType::Int32, false),
                Field::new("cr_reason_sk", DataType::Int32, false),
                Field::new("cr_order_number", DataType::Int64, false),
                Field::new("cr_return_quantity", DataType::Int32, false),
                Field::new("cr_return_amount", DataType::Decimal128(7, 2), false),
                Field::new("cr_return_tax", DataType::Decimal128(7, 2), false),
                Field::new("cr_return_amt_inc_tax", DataType::Decimal128(7, 2), false),
                Field::new("cr_fee", DataType::Decimal128(7, 2), false),
                Field::new("cr_return_ship_cost", DataType::Decimal128(7, 2), false),
                Field::new("cr_refunded_cash", DataType::Decimal128(7, 2), false),
                Field::new("cr_reversed_charge", DataType::Decimal128(7, 2), false),
                Field::new("cr_store_credit", DataType::Decimal128(7, 2), false),
                Field::new("cr_net_loss", DataType::Decimal128(7, 2), false),
            ]),
        ),
        TableDef::new(
            "inventory",
            Schema::new(vec![
                Field::new("inv_date_sk", DataType::Int32, false),
                Field::new("inv_item_sk", DataType::Int32, false),
                Field::new("inv_warehouse_sk", DataType::Int32, false),
                Field::new("inv_quantity_on_hand", DataType::Int32, false),
            ]),
        ),
        TableDef::new(
            "store_sales",
            Schema::new(vec![
                Field::new("ss_sold_date_sk", DataType::Int32, false),
                Field::new("ss_sold_time_sk", DataType::Int32, false),
                Field::new("ss_item_sk", DataType::Int32, false),
                Field::new("ss_customer_sk", DataType::Int32, false),
                Field::new("ss_cdemo_sk", DataType::Int32, false),
                Field::new("ss_hdemo_sk", DataType::Int32, false),
                Field::new("ss_addr_sk", DataType::Int32, false),
                Field::new("ss_store_sk", DataType::Int32, false),
                Field::new("ss_promo_sk", DataType::Int32, false),
                Field::new("ss_ticket_number", DataType::Int64, false),
                Field::new("ss_quantity", DataType::Int32, false),
                Field::new("ss_wholesale_cost", DataType::Decimal128(7, 2), false),
                Field::new("ss_list_price", DataType::Decimal128(7, 2), false),
                Field::new("ss_sales_price", DataType::Decimal128(7, 2), false),
                Field::new("ss_ext_discount_amt", DataType::Decimal128(7, 2), false),
                Field::new("ss_ext_sales_price", DataType::Decimal128(7, 2), false),
                Field::new("ss_ext_wholesale_cost", DataType::Decimal128(7, 2), false),
                Field::new("ss_ext_list_price", DataType::Decimal128(7, 2), false),
                Field::new("ss_ext_tax", DataType::Decimal128(7, 2), false),
                Field::new("ss_coupon_amt", DataType::Decimal128(7, 2), false),
                Field::new("ss_net_paid", DataType::Decimal128(7, 2), false),
                Field::new("ss_net_paid_inc_tax", DataType::Decimal128(7, 2), false),
                Field::new("ss_net_profit", DataType::Decimal128(7, 2), false),
            ]),
        ),
        TableDef::new(
            "store_returns",
            Schema::new(vec![
                Field::new("sr_returned_date_sk", DataType::Int32, false),
                Field::new("sr_return_time_sk", DataType::Int32, false),
                Field::new("sr_item_sk", DataType::Int32, false),
                Field::new("sr_customer_sk", DataType::Int32, false),
                Field::new("sr_cdemo_sk", DataType::Int32, false),
                Field::new("sr_hdemo_sk", DataType::Int32, false),
                Field::new("sr_addr_sk", DataType::Int32, false),
                Field::new("sr_store_sk", DataType::Int32, false),
                Field::new("sr_reason_sk", DataType::Int32, false),
                Field::new("sr_ticket_number", DataType::Int64, false),
                Field::new("sr_return_quantity", DataType::Int32, false),
                Field::new("sr_return_amt", DataType::Decimal128(7, 2), false),
                Field::new("sr_return_tax", DataType::Decimal128(7, 2), false),
                Field::new("sr_return_amt_inc_tax", DataType::Decimal128(7, 2), false),
                Field::new("sr_fee", DataType::Decimal128(7, 2), false),
                Field::new("sr_return_ship_cost", DataType::Decimal128(7, 2), false),
                Field::new("sr_refunded_cash", DataType::Decimal128(7, 2), false),
                Field::new("sr_reversed_charge", DataType::Decimal128(7, 2), false),
                Field::new("sr_store_credit", DataType::Decimal128(7, 2), false),
                Field::new("sr_net_loss", DataType::Decimal128(7, 2), false),
            ]),
        ),
        TableDef::new(
            "web_sales",
            Schema::new(vec![
                Field::new("ws_sold_date_sk", DataType::Int32, false),
                Field::new("ws_sold_time_sk", DataType::Int32, false),
                Field::new("ws_ship_date_sk", DataType::Int32, false),
                Field::new("ws_item_sk", DataType::Int32, false),
                Field::new("ws_bill_customer_sk", DataType::Int32, false),
                Field::new("ws_bill_cdemo_sk", DataType::Int32, false),
                Field::new("ws_bill_hdemo_sk", DataType::Int32, false),
                Field::new("ws_bill_addr_sk", DataType::Int32, false),
                Field::new("ws_ship_customer_sk", DataType::Int32, false),
                Field::new("ws_ship_cdemo_sk", DataType::Int32, false),
                Field::new("ws_ship_hdemo_sk", DataType::Int32, false),
                Field::new("ws_ship_addr_sk", DataType::Int32, false),
                Field::new("ws_web_page_sk", DataType::Int32, false),
                Field::new("ws_web_site_sk", DataType::Int32, false),
                Field::new("ws_ship_mode_sk", DataType::Int32, false),
                Field::new("ws_warehouse_sk", DataType::Int32, false),
                Field::new("ws_promo_sk", DataType::Int32, false),
                Field::new("ws_order_number", DataType::Int64, false),
                Field::new("ws_quantity", DataType::Int32, false),
                Field::new("ws_wholesale_cost", DataType::Decimal128(7, 2), false),
                Field::new("ws_list_price", DataType::Decimal128(7, 2), false),
                Field::new("ws_sales_price", DataType::Decimal128(7, 2), false),
                Field::new("ws_ext_discount_amt", DataType::Decimal128(7, 2), false),
                Field::new("ws_ext_sales_price", DataType::Decimal128(7, 2), false),
                Field::new("ws_ext_wholesale_cost", DataType::Decimal128(7, 2), false),
                Field::new("ws_ext_list_price", DataType::Decimal128(7, 2), false),
                Field::new("ws_ext_tax", DataType::Decimal128(7, 2), false),
                Field::new("ws_coupon_amt", DataType::Decimal128(7, 2), false),
                Field::new("ws_ext_ship_cost", DataType::Decimal128(7, 2), false),
                Field::new("ws_net_paid", DataType::Decimal128(7, 2), false),
                Field::new("ws_net_paid_inc_tax", DataType::Decimal128(7, 2), false),
                Field::new("ws_net_paid_inc_ship", DataType::Decimal128(7, 2), false),
                Field::new(
                    "ws_net_paid_inc_ship_tax",
                    DataType::Decimal128(7, 2),
                    false,
                ),
                Field::new("ws_net_profit", DataType::Decimal128(7, 2), false),
            ]),
        ),
        TableDef::new(
            "web_returns",
            Schema::new(vec![
                Field::new("wr_returned_date_sk", DataType::Int32, false),
                Field::new("wr_returned_time_sk", DataType::Int32, false),
                Field::new("wr_item_sk", DataType::Int32, false),
                Field::new("wr_refunded_customer_sk", DataType::Int32, false),
                Field::new("wr_refunded_cdemo_sk", DataType::Int32, false),
                Field::new("wr_refunded_hdemo_sk", DataType::Int32, false),
                Field::new("wr_refunded_addr_sk", DataType::Int32, false),
                Field::new("wr_returning_customer_sk", DataType::Int32, false),
                Field::new("wr_returning_cdemo_sk", DataType::Int32, false),
                Field::new("wr_returning_hdemo_sk", DataType::Int32, false),
                Field::new("wr_returning_addr_sk", DataType::Int32, false),
                Field::new("wr_web_page_sk", DataType::Int32, false),
                Field::new("wr_reason_sk", DataType::Int32, false),
                Field::new("wr_order_number", DataType::Int64, false),
                Field::new("wr_return_quantity", DataType::Int32, false),
                Field::new("wr_return_amt", DataType::Decimal128(7, 2), false),
                Field::new("wr_return_tax", DataType::Decimal128(7, 2), false),
                Field::new("wr_return_amt_inc_tax", DataType::Decimal128(7, 2), false),
                Field::new("wr_fee", DataType::Decimal128(7, 2), false),
                Field::new("wr_return_ship_cost", DataType::Decimal128(7, 2), false),
                Field::new("wr_refunded_cash", DataType::Decimal128(7, 2), false),
                Field::new("wr_reversed_charge", DataType::Decimal128(7, 2), false),
                Field::new("wr_account_credit", DataType::Decimal128(7, 2), false),
                Field::new("wr_net_loss", DataType::Decimal128(7, 2), false),
            ]),
        ),
        TableDef::new(
            "call_center",
            Schema::new(vec![
                Field::new("cc_call_center_sk", DataType::Int32, false),
                Field::new("cc_call_center_id", DataType::Utf8, false),
                Field::new("cc_rec_start_date", DataType::Date32, false),
                Field::new("cc_rec_end_date", DataType::Date32, false),
                Field::new("cc_closed_date_sk", DataType::Int32, false),
                Field::new("cc_open_date_sk", DataType::Int32, false),
                Field::new("cc_name", DataType::Utf8, false),
                Field::new("cc_class", DataType::Utf8, false),
                Field::new("cc_employees", DataType::Int32, false),
                Field::new("cc_sq_ft", DataType::Int32, false),
                Field::new("cc_hours", DataType::Utf8, false),
                Field::new("cc_manager", DataType::Utf8, false),
                Field::new("cc_mkt_id", DataType::Int32, false),
                Field::new("cc_mkt_class", DataType::Utf8, false),
                Field::new("cc_mkt_desc", DataType::Utf8, false),
                Field::new("cc_market_manager", DataType::Utf8, false),
                Field::new("cc_division", DataType::Int32, false),
                Field::new("cc_division_name", DataType::Utf8, false),
                Field::new("cc_company", DataType::Int32, false),
                Field::new("cc_company_name", DataType::Utf8, false),
                Field::new("cc_street_number", DataType::Utf8, false),
                Field::new("cc_street_name", DataType::Utf8, false),
                Field::new("cc_street_type", DataType::Utf8, false),
                Field::new("cc_suite_number", DataType::Utf8, false),
                Field::new("cc_city", DataType::Utf8, false),
                Field::new("cc_county", DataType::Utf8, false),
                Field::new("cc_state", DataType::Utf8, false),
                Field::new("cc_zip", DataType::Utf8, false),
                Field::new("cc_country", DataType::Utf8, false),
                Field::new("cc_gmt_offset", DataType::Decimal128(5, 2), false),
                Field::new("cc_tax_percentage", DataType::Decimal128(5, 2), false),
            ]),
        ),
        TableDef::new(
            "catalog_page",
            Schema::new(vec![
                Field::new("cp_catalog_page_sk", DataType::Int32, false),
                Field::new("cp_catalog_page_id", DataType::Utf8, false),
                Field::new("cp_start_date_sk", DataType::Int32, false),
                Field::new("cp_end_date_sk", DataType::Int32, false),
                Field::new("cp_department", DataType::Utf8, false),
                Field::new("cp_catalog_number", DataType::Int32, false),
                Field::new("cp_catalog_page_number", DataType::Int32, false),
                Field::new("cp_description", DataType::Utf8, false),
                Field::new("cp_type", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "customer",
            Schema::new(vec![
                Field::new("c_customer_sk", DataType::Int32, false),
                Field::new("c_customer_id", DataType::Utf8, false),
                Field::new("c_current_cdemo_sk", DataType::Int32, false),
                Field::new("c_current_hdemo_sk", DataType::Int32, false),
                Field::new("c_current_addr_sk", DataType::Int32, false),
                Field::new("c_first_shipto_date_sk", DataType::Int32, false),
                Field::new("c_first_sales_date_sk", DataType::Int32, false),
                Field::new("c_salutation", DataType::Utf8, false),
                Field::new("c_first_name", DataType::Utf8, false),
                Field::new("c_last_name", DataType::Utf8, false),
                Field::new("c_preferred_cust_flag", DataType::Utf8, false),
                Field::new("c_birth_day", DataType::Int32, false),
                Field::new("c_birth_month", DataType::Int32, false),
                Field::new("c_birth_year", DataType::Int32, false),
                Field::new("c_birth_country", DataType::Utf8, false),
                Field::new("c_login", DataType::Utf8, false),
                Field::new("c_email_address", DataType::Utf8, false),
                Field::new("c_last_review_date_sk", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "customer_address",
            Schema::new(vec![
                Field::new("ca_address_sk", DataType::Int32, false),
                Field::new("ca_address_id", DataType::Utf8, false),
                Field::new("ca_street_number", DataType::Utf8, false),
                Field::new("ca_street_name", DataType::Utf8, false),
                Field::new("ca_street_type", DataType::Utf8, false),
                Field::new("ca_suite_number", DataType::Utf8, false),
                Field::new("ca_city", DataType::Utf8, false),
                Field::new("ca_county", DataType::Utf8, false),
                Field::new("ca_state", DataType::Utf8, false),
                Field::new("ca_zip", DataType::Utf8, false),
                Field::new("ca_country", DataType::Utf8, false),
                Field::new("ca_gmt_offset", DataType::Decimal128(5, 2), false),
                Field::new("ca_location_type", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "customer_demographics",
            Schema::new(vec![
                Field::new("cd_demo_sk", DataType::Int32, false),
                Field::new("cd_gender", DataType::Utf8, false),
                Field::new("cd_marital_status", DataType::Utf8, false),
                Field::new("cd_education_status", DataType::Utf8, false),
                Field::new("cd_purchase_estimate", DataType::Int32, false),
                Field::new("cd_credit_rating", DataType::Utf8, false),
                Field::new("cd_dep_count", DataType::Int32, false),
                Field::new("cd_dep_employed_count", DataType::Int32, false),
                Field::new("cd_dep_college_count", DataType::Int32, false),
            ]),
        ),
        TableDef::new(
            "date_dim",
            Schema::new(vec![
                Field::new("d_date_sk", DataType::Int32, false),
                Field::new("d_date_id", DataType::Utf8, false),
                Field::new("d_date", DataType::Date32, false),
                Field::new("d_month_seq", DataType::Int32, false),
                Field::new("d_week_seq", DataType::Int32, false),
                Field::new("d_quarter_seq", DataType::Int32, false),
                Field::new("d_year", DataType::Int32, false),
                Field::new("d_dow", DataType::Int32, false),
                Field::new("d_moy", DataType::Int32, false),
                Field::new("d_dom", DataType::Int32, false),
                Field::new("d_qoy", DataType::Int32, false),
                Field::new("d_fy_year", DataType::Int32, false),
                Field::new("d_fy_quarter_seq", DataType::Int32, false),
                Field::new("d_fy_week_seq", DataType::Int32, false),
                Field::new("d_day_name", DataType::Utf8, false),
                Field::new("d_quarter_name", DataType::Utf8, false),
                Field::new("d_holiday", DataType::Utf8, false),
                Field::new("d_weekend", DataType::Utf8, false),
                Field::new("d_following_holiday", DataType::Utf8, false),
                Field::new("d_first_dom", DataType::Int32, false),
                Field::new("d_last_dom", DataType::Int32, false),
                Field::new("d_same_day_ly", DataType::Int32, false),
                Field::new("d_same_day_lq", DataType::Int32, false),
                Field::new("d_current_day", DataType::Utf8, false),
                Field::new("d_current_week", DataType::Utf8, false),
                Field::new("d_current_month", DataType::Utf8, false),
                Field::new("d_current_quarter", DataType::Utf8, false),
                Field::new("d_current_year", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "household_demographics",
            Schema::new(vec![
                Field::new("hd_demo_sk", DataType::Int32, false),
                Field::new("hd_income_band_sk", DataType::Int32, false),
                Field::new("hd_buy_potential", DataType::Utf8, false),
                Field::new("hd_dep_count", DataType::Int32, false),
                Field::new("hd_vehicle_count", DataType::Int32, false),
            ]),
        ),
        TableDef::new(
            "income_band",
            Schema::new(vec![
                Field::new("ib_income_band_sk", DataType::Int32, false),
                Field::new("ib_lower_bound", DataType::Int32, false),
                Field::new("ib_upper_bound", DataType::Int32, false),
            ]),
        ),
        TableDef::new(
            "item",
            Schema::new(vec![
                Field::new("i_item_sk", DataType::Int32, false),
                Field::new("i_item_id", DataType::Utf8, false),
                Field::new("i_rec_start_date", DataType::Date32, false),
                Field::new("i_rec_end_date", DataType::Date32, false),
                Field::new("i_item_desc", DataType::Utf8, false),
                Field::new("i_current_price", DataType::Decimal128(7, 2), false),
                Field::new("i_wholesale_cost", DataType::Decimal128(7, 2), false),
                Field::new("i_brand_id", DataType::Int32, false),
                Field::new("i_brand", DataType::Utf8, false),
                Field::new("i_class_id", DataType::Int32, false),
                Field::new("i_class", DataType::Utf8, false),
                Field::new("i_category_id", DataType::Int32, false),
                Field::new("i_category", DataType::Utf8, false),
                Field::new("i_manufact_id", DataType::Int32, false),
                Field::new("i_manufact", DataType::Utf8, false),
                Field::new("i_size", DataType::Utf8, false),
                Field::new("i_formulation", DataType::Utf8, false),
                Field::new("i_color", DataType::Utf8, false),
                Field::new("i_units", DataType::Utf8, false),
                Field::new("i_container", DataType::Utf8, false),
                Field::new("i_manager_id", DataType::Int32, false),
                Field::new("i_product_name", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "promotion",
            Schema::new(vec![
                Field::new("p_promo_sk", DataType::Int32, false),
                Field::new("p_promo_id", DataType::Utf8, false),
                Field::new("p_start_date_sk", DataType::Int32, false),
                Field::new("p_end_date_sk", DataType::Int32, false),
                Field::new("p_item_sk", DataType::Int32, false),
                Field::new("p_cost", DataType::Decimal128(15, 2), false),
                Field::new("p_response_target", DataType::Int32, false),
                Field::new("p_promo_name", DataType::Utf8, false),
                Field::new("p_channel_dmail", DataType::Utf8, false),
                Field::new("p_channel_email", DataType::Utf8, false),
                Field::new("p_channel_catalog", DataType::Utf8, false),
                Field::new("p_channel_tv", DataType::Utf8, false),
                Field::new("p_channel_radio", DataType::Utf8, false),
                Field::new("p_channel_press", DataType::Utf8, false),
                Field::new("p_channel_event", DataType::Utf8, false),
                Field::new("p_channel_demo", DataType::Utf8, false),
                Field::new("p_channel_details", DataType::Utf8, false),
                Field::new("p_purpose", DataType::Utf8, false),
                Field::new("p_discount_active", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "reason",
            Schema::new(vec![
                Field::new("r_reason_sk", DataType::Int32, false),
                Field::new("r_reason_id", DataType::Utf8, false),
                Field::new("r_reason_desc", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "ship_mode",
            //),
            Schema::new(vec![
                Field::new("sm_ship_mode_sk", DataType::Int32, false),
                Field::new("sm_ship_mode_id", DataType::Utf8, false),
                Field::new("sm_type", DataType::Utf8, false),
                Field::new("sm_code", DataType::Utf8, false),
                Field::new("sm_carrier", DataType::Utf8, false),
                Field::new("sm_contract", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "store",
            Schema::new(vec![
                Field::new("s_store_sk", DataType::Int32, false),
                Field::new("s_store_id", DataType::Utf8, false),
                Field::new("s_rec_start_date", DataType::Date32, false),
                Field::new("s_rec_end_date", DataType::Date32, false),
                Field::new("s_closed_date_sk", DataType::Int32, false),
                Field::new("s_store_name", DataType::Utf8, false),
                Field::new("s_number_employees", DataType::Int32, false),
                Field::new("s_floor_space", DataType::Int32, false),
                Field::new("s_hours", DataType::Utf8, false),
                Field::new("s_manager", DataType::Utf8, false),
                Field::new("s_market_id", DataType::Int32, false),
                Field::new("s_geography_class", DataType::Utf8, false),
                Field::new("s_market_desc", DataType::Utf8, false),
                Field::new("s_market_manager", DataType::Utf8, false),
                Field::new("s_division_id", DataType::Int32, false),
                Field::new("s_division_name", DataType::Utf8, false),
                Field::new("s_company_id", DataType::Int32, false),
                Field::new("s_company_name", DataType::Utf8, false),
                Field::new("s_street_number", DataType::Utf8, false),
                Field::new("s_street_name", DataType::Utf8, false),
                Field::new("s_street_type", DataType::Utf8, false),
                Field::new("s_suite_number", DataType::Utf8, false),
                Field::new("s_city", DataType::Utf8, false),
                Field::new("s_county", DataType::Utf8, false),
                Field::new("s_state", DataType::Utf8, false),
                Field::new("s_zip", DataType::Utf8, false),
                Field::new("s_country", DataType::Utf8, false),
                Field::new("s_gmt_offset", DataType::Decimal128(5, 2), false),
                Field::new("s_tax_precentage", DataType::Decimal128(5, 2), false),
            ]),
        ),
        TableDef::new(
            "time_dim",
            Schema::new(vec![
                Field::new("t_time_sk", DataType::Int32, false),
                Field::new("t_time_id", DataType::Utf8, false),
                Field::new("t_time", DataType::Int32, false),
                Field::new("t_hour", DataType::Int32, false),
                Field::new("t_minute", DataType::Int32, false),
                Field::new("t_second", DataType::Int32, false),
                Field::new("t_am_pm", DataType::Utf8, false),
                Field::new("t_shift", DataType::Utf8, false),
                Field::new("t_sub_shift", DataType::Utf8, false),
                Field::new("t_meal_time", DataType::Utf8, false),
            ]),
        ),
        TableDef::new(
            "warehouse",
            //),
            Schema::new(vec![
                Field::new("w_warehouse_sk", DataType::Int32, false),
                Field::new("w_warehouse_id", DataType::Utf8, false),
                Field::new("w_warehouse_name", DataType::Utf8, false),
                Field::new("w_warehouse_sq_ft", DataType::Int32, false),
                Field::new("w_street_number", DataType::Utf8, false),
                Field::new("w_street_name", DataType::Utf8, false),
                Field::new("w_street_type", DataType::Utf8, false),
                Field::new("w_suite_number", DataType::Utf8, false),
                Field::new("w_city", DataType::Utf8, false),
                Field::new("w_county", DataType::Utf8, false),
                Field::new("w_state", DataType::Utf8, false),
                Field::new("w_zip", DataType::Utf8, false),
                Field::new("w_country", DataType::Utf8, false),
                Field::new("w_gmt_offset", DataType::Decimal128(5, 2), false),
            ]),
        ),
        TableDef::new(
            "web_page",
            Schema::new(vec![
                Field::new("wp_web_page_sk", DataType::Int32, false),
                Field::new("wp_web_page_id", DataType::Utf8, false),
                Field::new("wp_rec_start_date", DataType::Date32, false),
                Field::new("wp_rec_end_date", DataType::Date32, false),
                Field::new("wp_creation_date_sk", DataType::Int32, false),
                Field::new("wp_access_date_sk", DataType::Int32, false),
                Field::new("wp_autogen_flag", DataType::Utf8, false),
                Field::new("wp_customer_sk", DataType::Int32, false),
                Field::new("wp_url", DataType::Utf8, false),
                Field::new("wp_type", DataType::Utf8, false),
                Field::new("wp_char_count", DataType::Int32, false),
                Field::new("wp_link_count", DataType::Int32, false),
                Field::new("wp_image_count", DataType::Int32, false),
                Field::new("wp_max_ad_count", DataType::Int32, false),
            ]),
        ),
        TableDef::new(
            "web_site",
            Schema::new(vec![
                Field::new("web_site_sk", DataType::Int32, false),
                Field::new("web_site_id", DataType::Utf8, false),
                Field::new("web_rec_start_date", DataType::Date32, false),
                Field::new("web_rec_end_date", DataType::Date32, false),
                Field::new("web_name", DataType::Utf8, false),
                Field::new("web_open_date_sk", DataType::Int32, false),
                Field::new("web_close_date_sk", DataType::Int32, false),
                Field::new("web_class", DataType::Utf8, false),
                Field::new("web_manager", DataType::Utf8, false),
                Field::new("web_mkt_id", DataType::Int32, false),
                Field::new("web_mkt_class", DataType::Utf8, false),
                Field::new("web_mkt_desc", DataType::Utf8, false),
                Field::new("web_market_manager", DataType::Utf8, false),
                Field::new("web_company_id", DataType::Int32, false),
                Field::new("web_company_name", DataType::Utf8, false),
                Field::new("web_street_number", DataType::Utf8, false),
                Field::new("web_street_name", DataType::Utf8, false),
                Field::new("web_street_type", DataType::Utf8, false),
                Field::new("web_suite_number", DataType::Utf8, false),
                Field::new("web_city", DataType::Utf8, false),
                Field::new("web_county", DataType::Utf8, false),
                Field::new("web_state", DataType::Utf8, false),
                Field::new("web_zip", DataType::Utf8, false),
                Field::new("web_country", DataType::Utf8, false),
                Field::new("web_gmt_offset", DataType::Decimal128(5, 2), false),
                Field::new("web_tax_percentage", DataType::Decimal128(5, 2), false),
            ]),
        ),
    ]
}
