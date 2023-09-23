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

use object_store::path::Path;

use datafusion::prelude::*;
use datafusion_physical_plan::displayable;

#[tokio::test]
async fn teset_displayable() {
    // Hard code target_partitions as it appears in the RepartitionExec output
    let config = SessionConfig::new().with_target_partitions(3);
    let ctx = SessionContext::new_with_config(config);

    // register the a table
    ctx.register_csv("example", "tests/data/example.csv", CsvReadOptions::new())
        .await
        .unwrap();

    // create a plan to run a SQL query
    let dataframe = ctx.sql("SELECT a FROM example WHERE a < 5").await.unwrap();
    let physical_plan = dataframe.create_physical_plan().await.unwrap();

    // Format using display string in verbose mode
    let displayable_plan = displayable(physical_plan.as_ref());
    let plan_string = format!("{}", displayable_plan.indent(true));

    let working_directory = std::env::current_dir().unwrap();
    let normalized = Path::from_filesystem_path(working_directory).unwrap();
    let plan_string = plan_string.replace(normalized.as_ref(), "WORKING_DIR");

    assert_eq!("CoalesceBatchesExec: target_batch_size=8192\
                \n  FilterExec: a@0 < 5\
                \n    RepartitionExec: partitioning=RoundRobinBatch(3), input_partitions=1\
                \n      CsvExec: file_groups={1 group: [[WORKING_DIR/tests/data/example.csv]]}, projection=[a], has_header=true",
               plan_string.trim());

    let one_line = format!("{}", displayable_plan.one_line());
    assert_eq!(
        "CoalesceBatchesExec: target_batch_size=8192",
        one_line.trim()
    );
}
