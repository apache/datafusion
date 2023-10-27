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

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result;
use datafusion::logical_expr::builder::LogicalTableSource;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::prelude::*;
use std::sync::Arc;

#[test]
fn plan_builder_1() -> Result<()> {
    // create a logical table source
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
    ]);
    let table_source = LogicalTableSource::new(SchemaRef::new(schema));

    // optional projection
    let projection = None;

    // create a LogicalPlanBuilder for a table scan
    let builder = LogicalPlanBuilder::scan("person", Arc::new(table_source), projection)?;

    // perform a filter operation and build the plan
    let plan = builder.filter(col("id").gt(lit(500)))?.build()?;

    // print the plan
    println!("{}", plan.display_indent_schema());

    Ok(())
}
