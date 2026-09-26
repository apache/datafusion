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

//! Writes the Substrait plan that `java-interop` reads.
//!
//! The Java side converts the plan with substrait-java, runs it in Spark and
//! compares the rows, which is the only way to catch a field that DataFusion
//! writes but never reads back, such as `AggregationPhase`. Producing the plan
//! needs no JVM, so it lives here; the test is ignored by default because the
//! file is only useful to that Java project.
//!
//! ```shell
//! cargo test -p datafusion-substrait --test substrait_integration -- --ignored write_java_interop_plan
//! mvn -f datafusion/substrait/java-interop/pom.xml test
//! ```

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::Result;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use prost::Message;
    use std::path::PathBuf;
    use std::sync::Arc;

    /// The query the Java side runs. `t` holds 1, 2 and 3 there, so the
    /// expected rows are 3, 6 and 2.0.
    const SQL: &str = "SELECT count(i), sum(i), avg(i) FROM t";

    /// Where the plan is written, overridable with `SUBSTRAIT_INTEROP_PLAN`.
    fn plan_path() -> PathBuf {
        match std::env::var_os("SUBSTRAIT_INTEROP_PLAN") {
            Some(path) => PathBuf::from(path),
            None => PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("java-interop/target/aggregate_plan.bin"),
        }
    }

    #[tokio::test]
    #[ignore = "writes a file for the java-interop project"]
    async fn write_java_interop_plan() -> Result<()> {
        let ctx = SessionContext::new();
        ctx.register_table(
            "t",
            Arc::new(EmptyTable::new(Arc::new(Schema::new(vec![Field::new(
                "i",
                DataType::Int64,
                true,
            )])))),
        )?;

        let plan = ctx.sql(SQL).await?.into_optimized_plan()?;
        let proto = to_substrait_plan(&plan, &ctx.state())?;

        let path = plan_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&path, proto.encode_to_vec())?;
        println!("wrote {}", path.display());
        Ok(())
    }
}
