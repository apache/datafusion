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

pub mod from_proto;
pub mod to_proto;

#[cfg(test)]
mod roundtrip_tests {
    use std::{convert::TryInto, sync::Arc};

    use datafusion::{
        arrow::{
            compute::kernels::sort::SortOptions,
            datatypes::{DataType, Field, Schema},
        },
        logical_plan::{JoinType, Operator},
        physical_plan::{
            empty::EmptyExec,
            expressions::{binary, col, lit, InListExpr, NotExpr},
            expressions::{Avg, Column, PhysicalSortExpr},
            filter::FilterExec,
            hash_aggregate::{AggregateMode, HashAggregateExec},
            hash_join::{HashJoinExec, PartitionMode},
            limit::{GlobalLimitExec, LocalLimitExec},
            sort::SortExec,
            AggregateExpr, ColumnarValue, Distribution, ExecutionPlan, Partitioning,
            PhysicalExpr,
        },
        scalar::ScalarValue,
    };

    use super::super::super::error::Result;
    use super::super::protobuf;
    use crate::execution_plans::ShuffleWriterExec;

    fn roundtrip_test(exec_plan: Arc<dyn ExecutionPlan>) -> Result<()> {
        let proto: protobuf::PhysicalPlanNode = exec_plan.clone().try_into()?;
        let result_exec_plan: Arc<dyn ExecutionPlan> = (&proto).try_into()?;
        assert_eq!(
            format!("{:?}", exec_plan),
            format!("{:?}", result_exec_plan)
        );
        Ok(())
    }

    #[test]
    fn roundtrip_empty() -> Result<()> {
        roundtrip_test(Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))))
    }

    #[test]
    fn roundtrip_local_limit() -> Result<()> {
        roundtrip_test(Arc::new(LocalLimitExec::new(
            Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))),
            25,
        )))
    }

    #[test]
    fn roundtrip_global_limit() -> Result<()> {
        roundtrip_test(Arc::new(GlobalLimitExec::new(
            Arc::new(EmptyExec::new(false, Arc::new(Schema::empty()))),
            25,
        )))
    }

    #[test]
    fn roundtrip_hash_join() -> Result<()> {
        let field_a = Field::new("col", DataType::Int64, false);
        let schema_left = Schema::new(vec![field_a.clone()]);
        let schema_right = Schema::new(vec![field_a]);
        let on = vec![(
            Column::new("col", schema_left.index_of("col")?),
            Column::new("col", schema_right.index_of("col")?),
        )];

        let schema_left = Arc::new(schema_left);
        let schema_right = Arc::new(schema_right);
        for join_type in &[
            JoinType::Inner,
            JoinType::Left,
            JoinType::Right,
            JoinType::Full,
            JoinType::Anti,
            JoinType::Semi,
        ] {
            for partition_mode in
                &[PartitionMode::Partitioned, PartitionMode::CollectLeft]
            {
                roundtrip_test(Arc::new(HashJoinExec::try_new(
                    Arc::new(EmptyExec::new(false, schema_left.clone())),
                    Arc::new(EmptyExec::new(false, schema_right.clone())),
                    on.clone(),
                    join_type,
                    *partition_mode,
                )?))?;
            }
        }
        Ok(())
    }

    #[test]
    fn rountrip_hash_aggregate() -> Result<()> {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));

        let groups: Vec<(Arc<dyn PhysicalExpr>, String)> =
            vec![(col("a", &schema)?, "unused".to_string())];

        let aggregates: Vec<Arc<dyn AggregateExpr>> = vec![Arc::new(Avg::new(
            col("b", &schema)?,
            "AVG(b)".to_string(),
            DataType::Float64,
        ))];

        roundtrip_test(Arc::new(HashAggregateExec::try_new(
            AggregateMode::Final,
            groups.clone(),
            aggregates.clone(),
            Arc::new(EmptyExec::new(false, schema.clone())),
            schema,
        )?))
    }

    #[test]
    fn roundtrip_filter_with_not_and_in_list() -> Result<()> {
        let field_a = Field::new("a", DataType::Boolean, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let field_c = Field::new("c", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b, field_c]));
        let not = Arc::new(NotExpr::new(col("a", &schema)?));
        let in_list = Arc::new(InListExpr::new(
            col("b", &schema)?,
            vec![
                lit(ScalarValue::Int64(Some(1))),
                lit(ScalarValue::Int64(Some(2))),
            ],
            false,
        ));
        let and = binary(not, Operator::And, in_list, &schema)?;
        roundtrip_test(Arc::new(FilterExec::try_new(
            and,
            Arc::new(EmptyExec::new(false, schema.clone())),
        )?))
    }

    #[test]
    fn roundtrip_sort() -> Result<()> {
        let field_a = Field::new("a", DataType::Boolean, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));
        let sort_exprs = vec![
            PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions {
                    descending: true,
                    nulls_first: false,
                },
            },
            PhysicalSortExpr {
                expr: col("b", &schema)?,
                options: SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            },
        ];
        roundtrip_test(Arc::new(SortExec::try_new(
            sort_exprs,
            Arc::new(EmptyExec::new(false, schema)),
        )?))
    }

    #[test]
    fn roundtrip_shuffle_writer() -> Result<()> {
        let field_a = Field::new("a", DataType::Int64, false);
        let field_b = Field::new("b", DataType::Int64, false);
        let schema = Arc::new(Schema::new(vec![field_a, field_b]));

        roundtrip_test(Arc::new(ShuffleWriterExec::try_new(
            "job123".to_string(),
            123,
            Arc::new(EmptyExec::new(false, schema)),
            "".to_string(),
            Some(Partitioning::Hash(vec![Arc::new(Column::new("a", 0))], 4)),
        )?))
    }
}
