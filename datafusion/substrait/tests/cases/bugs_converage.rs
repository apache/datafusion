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

//! Tests for bugs in substrait

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Result;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use std::fs::File;
    use std::io::BufReader;
    use std::sync::Arc;
    use substrait::proto::Plan;
    #[tokio::test]
    async fn extra_projection_with_input() -> Result<()> {
        let ctx = SessionContext::new();
        let schema = Schema::new(vec![
            Field::new("user_id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("paid_for_service", DataType::Boolean, false),
        ]);
        let memory_table = MemTable::try_new(schema.into(), vec![vec![]]).unwrap();
        ctx.register_table("users", Arc::new(memory_table))?;
        let path = "tests/testdata/extra_projection_with_input.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;
        let plan_str = format!("{}", plan);
        assert_eq!(plan_str, "Projection: users.user_id, users.name, users.paid_for_service, row_number() ORDER BY [users.name ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS row_number\
        \n  WindowAggr: windowExpr=[[row_number() ORDER BY [users.name ASC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]\
        \n    TableScan: users projection=[user_id, name, paid_for_service]");
        Ok(())
    }
}
