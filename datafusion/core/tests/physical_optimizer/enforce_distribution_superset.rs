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

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use crate::physical_optimizer::test_utils::memory_exec;
use datafusion_common::config::ConfigOptions;
use datafusion_common::{JoinType, NullEquality, Result};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{Partitioning, physical_exprs_equal};
use datafusion_physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion_physical_plan::joins::utils::JoinOn;
use datafusion_physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_physical_plan::repartition::RepartitionExec;

fn repartition_superset_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, true),
        Field::new("ts", DataType::Int64, true),
    ]))
}

#[test]
fn enforce_distribution_repartitions_superset_hash_keys() -> Result<()> {
    let schema = repartition_superset_schema();
    let left = memory_exec(&schema);
    let right = memory_exec(&schema);

    let left_superset = Arc::new(RepartitionExec::try_new(
        Arc::clone(&left),
        Partitioning::Hash(
            vec![
                Arc::new(Column::new_with_schema("region", &schema).unwrap()),
                Arc::new(Column::new_with_schema("ts", &schema).unwrap()),
            ],
            4,
        ),
    )?);

    let right_partitioned = Arc::new(RepartitionExec::try_new(
        right,
        Partitioning::Hash(
            vec![Arc::new(Column::new_with_schema("ts", &schema).unwrap())],
            4,
        ),
    )?);

    let on: JoinOn = vec![
        (
            Arc::new(Column::new_with_schema("ts", &schema).unwrap()),
            Arc::new(Column::new_with_schema("ts", &schema).unwrap()),
        ),
    ];

    let join = Arc::new(HashJoinExec::try_new(
        left_superset,
        right_partitioned,
        on,
        None,
        &JoinType::Inner,
        None,
        PartitionMode::Partitioned,
        NullEquality::NullEqualsNothing,
    )?);

    let mut config = ConfigOptions::new();
    config.execution.target_partitions = 4;

    let optimized = EnforceDistribution::new().optimize(join, &config)?;
    let join = optimized
        .as_any()
        .downcast_ref::<HashJoinExec>()
        .expect("expected hash join");

    let repartition = join
        .left()
        .as_any()
        .downcast_ref::<RepartitionExec>()
        .expect("left side should be repartitioned");

    match repartition.partitioning() {
        Partitioning::Hash(exprs, partitions) => {
            assert_eq!(*partitions, 4);
            let expected =
                vec![Arc::new(Column::new_with_schema("ts", &schema).unwrap()) as _];
            assert!(
                physical_exprs_equal(exprs, &expected),
                "expected repartitioning on [ts]"
            );
        }
        other => panic!("expected hash repartitioning, got {other:?}"),
    }

    // The original (superset) partitioning should remain below the new repartition
    // so we can rehash to the required keys even when the partition counts already match.
    let original = repartition
        .input()
        .as_any()
        .downcast_ref::<RepartitionExec>()
        .expect("expected original repartition to remain");
    assert!(matches!(
        original.partitioning(),
        Partitioning::Hash(_, 4)
    ));

    Ok(())
}
