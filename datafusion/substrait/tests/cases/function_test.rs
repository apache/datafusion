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
    use crate::utils::test::TestSchemaCollector;
    use datafusion::common::Result;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use std::fs::File;
    use std::io::BufReader;
    use substrait::proto::Plan;

    #[tokio::test]
    async fn contains_function_test() -> Result<()> {
        let path = "tests/testdata/contains_plan.substrait.json";

        let proto_plan = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let ctx = TestSchemaCollector::generate_context_from_plan(&proto_plan);
        let plan = from_substrait_plan(&ctx, &proto_plan).await?;

        let plan_str = format!("{}", plan);

        assert_eq!(
            plan_str,
            "Projection: nation.n_name\
            \n  Filter: contains(nation.n_name, Utf8(\"IA\"))\
            \n    TableScan: nation projection=[n_nationkey, n_name, n_regionkey, n_comment]"
        );
        Ok(())
    }
}
