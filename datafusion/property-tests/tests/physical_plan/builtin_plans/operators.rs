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

//! The operators of the simple operator snapshot that are not sorts, limits,
//! repartitions, projections or filters: unnest, async functions, scalar
//! subqueries, recursive queries, `EXPLAIN`, the optimizer's
//! `OutputRequirementExec`, and plans wrapped for FFI.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::physical_optimizer::output_requirements::OutputRequirementExec;
use datafusion_common::config::ConfigOptions;
use datafusion_common::display::{PlanType, StringifiedPlan};
use datafusion_common::{DFSchema, Result, ScalarValue, UnnestOptions, internal_err};
use datafusion_expr::async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl};
use datafusion_expr::logical_plan::builder::unnest_with_options;
use datafusion_expr::physical_planning_context::{ScalarSubqueryResults, SubqueryIndex};
use datafusion_expr::{
    ColumnarValue, EmptyRelation, LogicalPlan, Operator, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Unnest, Volatility,
};
use datafusion_ffi::execution_plan::{FFI_ExecutionPlan, ForeignExecutionPlan};
use datafusion_functions_aggregate::min_max::max_udaf;
use datafusion_physical_expr::async_scalar_function::AsyncFuncExpr;
use datafusion_physical_expr::expressions::{BinaryExpr, Literal, col};
use datafusion_physical_expr::scalar_subquery::ScalarSubqueryExpr;
use datafusion_physical_expr::{
    Distribution, LexOrdering, OrderingRequirements, Partitioning, PhysicalExpr,
    PhysicalSortExpr, ScalarFunctionExpr,
};
use datafusion_physical_plan::aggregates::AggregateMode;
use datafusion_physical_plan::analyze::AnalyzeExecBuilder;
use datafusion_physical_plan::async_func::AsyncFuncExec;
use datafusion_physical_plan::explain::ExplainExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::recursive_query::RecursiveQueryExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::scalar_subquery::{ScalarSubqueryExec, ScalarSubqueryLink};
use datafusion_physical_plan::sorts::partial_sort::PartialSortExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::union::InterleaveExec;
use datafusion_physical_plan::unnest::{ListUnnest, UnnestExec};
use datafusion_physical_plan::work_table::WorkTableExec;
use datafusion_property_tests::physical_plan::fixtures::SourceSpec;
use datafusion_property_tests::physical_plan::harness::PlanFactory;

use super::{
    FETCH, Plan, aggregate, no_group_by, one_input, ordering_on, single_phase,
    sorted_spec, spec,
};

/// The operators under test
pub(super) fn operator_plans() -> Result<Vec<PlanFactory>> {
    Ok(vec![
        // Sorts each partition on (a, c), on input already sorted on a
        one_input("PartialSortExec", sorted_spec()?, |input| {
            Ok(Arc::new(PartialSortExec::new(
                ordering_on_a_c(&input)?,
                input,
                1,
            )))
        }),
        one_input(
            "PartialSortExec with preserve_partitioning",
            sorted_spec()?,
            |input| {
                Ok(Arc::new(
                    PartialSortExec::new(ordering_on_a_c(&input)?, input, 1)
                        .with_preserve_partitioning(true),
                ))
            },
        ),
        one_input("PartialSortExec with fetch", sorted_spec()?, |input| {
            Ok(Arc::new(
                PartialSortExec::new(ordering_on_a_c(&input)?, input, 1)
                    .with_fetch(Some(FETCH)),
            ))
        }),
        // As `EnforceDistribution` builds it in place of a `UnionExec` whose
        // children are hash partitioned the same way
        PlanFactory::new("InterleaveExec", vec![spec(), spec()], |inputs| {
            let children = inputs
                .into_iter()
                .map(|input| {
                    let a = col("a", &input.schema())?;
                    Ok(Arc::new(RepartitionExec::try_new(
                        input,
                        Partitioning::Hash(vec![a], 3),
                    )?) as Plan)
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(InterleaveExec::try_new(children)?))
        }),
        unnest("UnnestExec on a list", &["l"], UnnestOptions::new()),
        unnest("UnnestExec on a struct", &["s"], UnnestOptions::new()),
        unnest(
            "UnnestExec on a list and a struct",
            &["l", "s"],
            UnnestOptions::new(),
        ),
        unnest(
            "UnnestExec on a list without preserving nulls",
            &["l"],
            UnnestOptions::new().with_preserve_nulls(false),
        ),
        one_input("AsyncFuncExec", spec(), |input| {
            let schema = input.schema();
            let udf = Arc::new(
                AsyncScalarUDF::new(Arc::new(AsyncIdentity::new())).into_scalar_udf(),
            );
            let call = ScalarFunctionExpr::try_new(
                udf,
                vec![col("a", &schema)?],
                &schema,
                Arc::new(ConfigOptions::default()),
            )?;
            let expr =
                AsyncFuncExpr::try_new("async_identity(a)", Arc::new(call), &schema)?;
            Ok(Arc::new(AsyncFuncExec::try_new(
                vec![Arc::new(expr)],
                input,
            )?))
        }),
        // SELECT * FROM t WHERE a < (SELECT max(a) FROM s)
        PlanFactory::new("ScalarSubqueryExec", vec![spec(), spec()], |inputs| {
            let [main, subquery_input] = <[Plan; 2]>::try_from(inputs)
                .map_err(|_| datafusion_common::internal_datafusion_err!("two inputs"))?;
            let results = ScalarSubqueryResults::new(1);
            let index = SubqueryIndex::new(0);
            let max = aggregate(max_udaf(), "a", "max(a)", &subquery_input)?;
            let subquery = single_phase(
                AggregateMode::Single,
                no_group_by(),
                vec![max],
                subquery_input,
                None,
            )?;
            let max_a: Arc<dyn PhysicalExpr> = Arc::new(ScalarSubqueryExpr::new(
                DataType::Int32,
                true,
                index,
                results.clone(),
            ));
            let predicate = Arc::new(BinaryExpr::new(
                col("a", &main.schema())?,
                Operator::Lt,
                max_a,
            ));
            let filter = Arc::new(FilterExec::try_new(predicate, main)?);
            let links = vec![ScalarSubqueryLink {
                plan: subquery,
                index,
            }];
            Ok(Arc::new(ScalarSubqueryExec::new(filter, links, results)))
        }),
        // As `OutputRequirements` adds it above a plan without an ordering
        one_input("OutputRequirementExec", spec(), |input| {
            Ok(Arc::new(OutputRequirementExec::new(
                input,
                None,
                Distribution::UnspecifiedDistribution,
                None,
            )))
        }),
        // As `OutputRequirements` adds it above a sort with a fetch: with the
        // ordering, a single partition and the fetch of the sort
        one_input(
            "OutputRequirementExec above a SortExec with fetch",
            spec(),
            |input| {
                let ordering = ordering_on("a", &input)?;
                let sort: Plan = Arc::new(
                    SortExec::new(ordering.clone(), input).with_fetch(Some(FETCH)),
                );
                Ok(Arc::new(OutputRequirementExec::new(
                    sort,
                    Some(OrderingRequirements::from(ordering)),
                    Distribution::SinglePartition,
                    Some(FETCH),
                )))
            },
        ),
        one_input("AnalyzeExec", spec(), |input| {
            Ok(Arc::new(
                AnalyzeExecBuilder::new(
                    false,
                    false,
                    input,
                    LogicalPlan::explain_schema(),
                )
                .build(),
            ))
        }),
        PlanFactory::new("ExplainExec", vec![], |_| {
            let plans = vec![
                StringifiedPlan::new(PlanType::InitialLogicalPlan, "TableScan: t"),
                StringifiedPlan::new(PlanType::FinalPhysicalPlan, "DataSourceExec"),
            ];
            Ok(Arc::new(ExplainExec::new(
                LogicalPlan::explain_schema(),
                plans,
                false,
            )))
        }),
        // WITH RECURSIVE r AS (SELECT * FROM t UNION ALL SELECT a + 1, ...
        // FROM r WHERE a < 10), whose recursive term reads the work table
        one_input("RecursiveQueryExec", spec(), |static_term| {
            let schema = static_term.schema();
            let work_table: Plan = Arc::new(WorkTableExec::new(
                "r".to_string(),
                Arc::clone(&schema),
                None,
            )?);
            let a = col("a", &schema)?;
            let ten = Arc::new(Literal::new(ScalarValue::Int32(Some(10))));
            let below_ten = Arc::new(BinaryExpr::new(Arc::clone(&a), Operator::Lt, ten));
            let filter: Plan = Arc::new(FilterExec::try_new(below_ten, work_table)?);
            let one = Arc::new(Literal::new(ScalarValue::Int32(Some(1))));
            let plus_one: Arc<dyn PhysicalExpr> =
                Arc::new(BinaryExpr::new(a, Operator::Plus, one));
            let exprs = schema
                .fields()
                .iter()
                .map(|field| {
                    let name = field.name();
                    let expr = if name == "a" {
                        Arc::clone(&plus_one)
                    } else {
                        col(name, &schema)?
                    };
                    Ok((expr, name.clone()))
                })
                .collect::<Result<Vec<_>>>()?;
            let recursive_term: Plan = Arc::new(ProjectionExec::try_new(exprs, filter)?);
            Ok(Arc::new(RecursiveQueryExec::try_new(
                "r".to_string(),
                schema,
                static_term,
                recursive_term,
                false,
            )?))
        }),
        // A plan of another library, as a `ForeignExecutionPlan` sees it: its
        // properties and statistics cross the FFI boundary
        foreign("ForeignExecutionPlan of a FilterExec", spec(), |input| {
            let predicate = col("b", &input.schema())?;
            Ok(Arc::new(FilterExec::try_new(predicate, input)?))
        }),
        foreign("ForeignExecutionPlan of a SortExec", spec(), |input| {
            Ok(Arc::new(
                SortExec::new(ordering_on("a", &input)?, input)
                    .with_preserve_partitioning(true),
            ))
        }),
        foreign(
            "ForeignExecutionPlan of a RepartitionExec",
            spec(),
            |input| {
                let a = col("a", &input.schema())?;
                Ok(Arc::new(RepartitionExec::try_new(
                    input,
                    Partitioning::Hash(vec![a], 4),
                )?))
            },
        ),
    ])
}

/// The ascending ordering on (a, c) of `input`
fn ordering_on_a_c(input: &Plan) -> Result<LexOrdering> {
    let schema = input.schema();
    Ok(LexOrdering::new(vec![
        PhysicalSortExpr::new_default(col("a", &schema)?),
        PhysicalSortExpr::new_default(col("c", &schema)?),
    ])
    .unwrap())
}

/// A key, a nullable list and a nullable struct
fn nested_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("l", DataType::new_list(DataType::Int32, false), true),
        Field::new(
            "s",
            DataType::Struct(Fields::from(vec![
                Field::new("x", DataType::Int32, false),
                Field::new("y", DataType::Utf8, true),
            ])),
            true,
        ),
    ]))
}

/// An `UnnestExec` of `columns` of [`nested_schema`], built as the physical
/// planner builds it from the logical `Unnest`, which computes its schema
fn unnest(name: &str, columns: &[&str], options: UnnestOptions) -> PlanFactory {
    let columns: Vec<String> = columns.iter().map(ToString::to_string).collect();
    let spec = SourceSpec::new(nested_schema()).with_num_rows(600);
    one_input(name, spec, move |input| {
        let relation = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::try_from(input.schema())?),
        });
        let columns = columns
            .iter()
            .map(datafusion_common::Column::from_name)
            .collect();
        let LogicalPlan::Unnest(Unnest {
            list_type_columns,
            struct_type_columns,
            schema,
            options,
            ..
        }) = unnest_with_options(relation, columns, options.clone())?
        else {
            return internal_err!("unnest_with_options did not return an Unnest");
        };
        let list_column_indices = list_type_columns
            .iter()
            .map(|(index, unnesting)| ListUnnest {
                index_in_input_schema: *index,
                depth: unnesting.depth,
            })
            .collect();
        Ok(Arc::new(UnnestExec::new(
            input,
            list_column_indices,
            struct_type_columns,
            Arc::clone(schema.inner()),
            options,
        )?))
    })
}

/// The plan `create` builds, wrapped for FFI and read back as a plan of
/// another library: a `ForeignExecutionPlan` whose children are the
/// children of the plan
fn foreign<F>(name: &str, spec: SourceSpec, create: F) -> PlanFactory
where
    F: Fn(Plan) -> Result<Plan> + Send + Sync + 'static,
{
    one_input(name, spec, move |input| {
        let plan = FFI_ExecutionPlan::new(create(input)?, None);
        Ok(Arc::new(ForeignExecutionPlan::try_from(plan)?))
    })
}

/// An async UDF that returns its argument
#[derive(Debug, PartialEq, Eq, Hash)]
struct AsyncIdentity {
    signature: Signature,
}

impl AsyncIdentity {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Volatile),
        }
    }
}

impl ScalarUDFImpl for AsyncIdentity {
    fn name(&self) -> &str {
        "async_identity"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("async_identity can only be called from async contexts")
    }
}

#[async_trait]
impl AsyncScalarUDFImpl for AsyncIdentity {
    async fn invoke_async_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        Ok(args.args[0].clone())
    }
}
