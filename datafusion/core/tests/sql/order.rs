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

use super::*;
use datafusion::datasource::datasource::TableProviderFactory;
use datafusion::datasource::listing::ListingTable;
use datafusion::datasource::listing_table_factory::ListingTableFactory;
use test_utils::{batches_to_vec, partitions_to_sorted_vec};

#[tokio::test]
async fn sort_with_lots_of_repetition_values() -> Result<()> {
    let ctx = SessionContext::new();
    let filename = "tests/parquet/data/repeat_much.snappy.parquet";

    ctx.register_parquet("rep", filename, ParquetReadOptions::default())
        .await?;
    let sql = "select a from rep order by a";
    let actual = execute_to_batches(&ctx, sql).await;
    let actual = batches_to_vec(&actual);

    let sql1 = "select a from rep";
    let expected = execute_to_batches(&ctx, sql1).await;
    let expected = partitions_to_sorted_vec(&[expected]);

    assert_eq!(actual.len(), expected.len());
    for i in 0..actual.len() {
        assert_eq!(actual[i], expected[i]);
    }
    Ok(())
}

#[tokio::test]
async fn create_external_table_with_ddl_ordered() -> Result<()> {
    let ctx = SessionContext::new();
    let sql = "CREATE EXTERNAL TABLE dt (a_id integer, a_str string, a_bool boolean) STORED AS CSV ORDER BY (a_id ASC) LOCATION 'file://path/to/table';";
    if let LogicalPlan::CreateExternalTable(cmd) =
        ctx.state().create_logical_plan(sql).await?
    {
        let listing_table_factory = Arc::new(ListingTableFactory::new());
        let table_dyn = listing_table_factory.create(&ctx.state(), &cmd).await?;
        let table = table_dyn.as_any().downcast_ref::<ListingTable>().unwrap();
        assert_eq!(
            &cmd.ordered_exprs,
            table.options().file_sort_order.as_ref().unwrap()
        )
    } else {
        panic!("Wrong command")
    }
    Ok(())
}
