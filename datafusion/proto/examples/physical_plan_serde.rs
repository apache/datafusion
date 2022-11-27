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

use datafusion::prelude::*;
use datafusion_common::Result;
use datafusion_proto::bytes::{physical_plan_from_bytes, physical_plan_to_bytes};

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_csv("t1", "testdata/test.csv", CsvReadOptions::default())
        .await?;
    let logical_plan = ctx.table("t1")?.to_logical_plan()?;
    let physical_plan = ctx.create_physical_plan(&logical_plan).await?;
    let bytes = physical_plan_to_bytes(physical_plan.clone())?;
    let physical_round_trip = physical_plan_from_bytes(&bytes, &ctx)?;
    assert_eq!(
        format!("{:?}", physical_plan),
        format!("{:?}", physical_round_trip)
    );
    Ok(())
}
