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

//! Tests for reading substrait plans produced by other systems

#[cfg(test)]
mod tests {
    use datafusion::common::Result;
    use datafusion::prelude::{CsvReadOptions, SessionContext};
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use std::fs::File;
    use std::io::BufReader;
    use substrait::proto::Plan;

    #[tokio::test]
    async fn function_compound_signature() -> Result<()> {
        // DataFusion currently produces Substrait that refers to functions only by their name.
        // However, the Substrait spec requires that functions be identified by their compound signature.
        // This test confirms that DataFusion is able to consume plans following the spec, even though
        // we don't yet produce such plans.
        // Once we start producing plans with compound signatures, this test can be replaced by the roundtrip tests.

        let ctx = create_context().await?;

        // File generated with substrait-java's Isthmus:
        // ./isthmus-cli/build/graal/isthmus "select not d from data" -c "create table data (d boolean)"
        let path = "tests/testdata/select_not_bool.substrait.json";
        let proto = serde_json::from_reader::<_, Plan>(BufReader::new(
            File::open(path).expect("file not found"),
        ))
        .expect("failed to parse json");

        let plan = from_substrait_plan(&ctx, &proto).await?;

        assert_eq!(
            format!("{:?}", plan),
            "Projection: NOT DATA.a\
            \n  TableScan: DATA projection=[a, b, c, d, e, f]"
        );
        Ok(())
    }

    async fn create_context() -> datafusion::common::Result<SessionContext> {
        let ctx = SessionContext::new();
        ctx.register_csv("DATA", "tests/testdata/data.csv", CsvReadOptions::new())
            .await?;
        Ok(ctx)
    }
}
