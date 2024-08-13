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

//! Tests for Function Compatibility

#[cfg(test)]
mod tests {
    use datafusion::common::Result;
    use datafusion::prelude::{CsvReadOptions, SessionContext};
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use std::fs::File;
    use std::io::BufReader;
    use substrait::proto::Plan;

    #[tokio::test]
    async fn contains_function_test() -> Result<()> {
        let ctx = create_context().await?;

        let path = "tests/testdata/contains_plan.substrait.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;

        let plan_str = format!("{}", plan);

        assert_eq!(
            plan_str,
            "Projection: nation.b AS n_name\
            \n  Filter: contains(nation.b, Utf8(\"IA\"))\
            \n    TableScan: nation projection=[a, b, c, d, e, f]"
        );
        Ok(())
    }

    async fn create_context() -> datafusion::common::Result<SessionContext> {
        let ctx = SessionContext::new();
        ctx.register_csv("nation", "tests/testdata/data.csv", CsvReadOptions::new())
            .await?;
        Ok(ctx)
    }
}
