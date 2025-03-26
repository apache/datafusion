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

use arrow::array::types::{IntervalDayTime, IntervalMonthDayNano};
use arrow::buffer::OffsetBuffer;
use async_recursion::async_recursion;
use datafusion::arrow::array::MapArray;
use datafusion::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, IntervalUnit, Schema, TimeUnit,
};
use datafusion::common::{
    not_impl_datafusion_err, not_impl_err, plan_datafusion_err, plan_err,
    substrait_datafusion_err, substrait_err, DFSchema, DFSchemaRef, Spans,
    TableReference,
};
use datafusion::datasource::provider_as_source;
use datafusion::logical_expr::expr::{Exists, InSubquery, Sort, WindowFunctionParams};

use datafusion::logical_expr::{
    Aggregate, BinaryExpr, Case, Cast, EmptyRelation, Expr, ExprSchemable, Extension,
    LogicalPlan, Operator, Projection, SortExpr, Subquery, TryCast, Values,
};
use substrait::proto::aggregate_rel::Grouping;
use substrait::proto::expression as substrait_expression;
use substrait::proto::expression::subquery::set_predicate::PredicateOp;
use substrait::proto::expression_reference::ExprType;
use url::Url;

use crate::extensions::Extensions;
use crate::variation_const::{
    DATE_32_TYPE_VARIATION_REF, DATE_64_TYPE_VARIATION_REF,
    DECIMAL_128_TYPE_VARIATION_REF, DECIMAL_256_TYPE_VARIATION_REF,
    DEFAULT_CONTAINER_TYPE_VARIATION_REF, DEFAULT_TYPE_VARIATION_REF,
    LARGE_CONTAINER_TYPE_VARIATION_REF, UNSIGNED_INTEGER_TYPE_VARIATION_REF,
    VIEW_CONTAINER_TYPE_VARIATION_REF,
};
#[allow(deprecated)]
use crate::variation_const::{
    INTERVAL_DAY_TIME_TYPE_REF, INTERVAL_MONTH_DAY_NANO_TYPE_NAME,
    INTERVAL_MONTH_DAY_NANO_TYPE_REF, INTERVAL_YEAR_MONTH_TYPE_REF,
    TIMESTAMP_MICRO_TYPE_VARIATION_REF, TIMESTAMP_MILLI_TYPE_VARIATION_REF,
    TIMESTAMP_NANO_TYPE_VARIATION_REF, TIMESTAMP_SECOND_TYPE_VARIATION_REF,
};
use async_trait::async_trait;
use datafusion::arrow::array::{new_empty_array, AsArray};
use datafusion::arrow::temporal_conversions::NANOSECONDS;
use datafusion::catalog::TableProvider;
use datafusion::common::scalar::ScalarStructBuilder;
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::logical_expr::builder::project;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{
    col, expr, GroupingSet, Like, LogicalPlanBuilder, Partitioning, Repartition,
    WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
};
use datafusion::prelude::{lit, JoinType};
use datafusion::{
    arrow, error::Result, logical_expr::utils::split_conjunction,
    logical_expr::utils::split_conjunction_owned, prelude::Column, scalar::ScalarValue,
};
use std::collections::HashSet;
use std::sync::Arc;
use substrait::proto;
use substrait::proto::exchange_rel::ExchangeKind;
use substrait::proto::expression::cast::FailureBehavior::ReturnNull;
use substrait::proto::expression::literal::user_defined::Val;
use substrait::proto::expression::literal::{
    interval_day_to_second, IntervalCompound, IntervalDayToSecond, IntervalYearToMonth,
};
use substrait::proto::expression::subquery::SubqueryType;
use substrait::proto::expression::{
    Enum, FieldReference, IfThen, Literal, MultiOrList, Nested, ScalarFunction,
    SingularOrList, SwitchExpression, WindowFunction,
};
use substrait::proto::read_rel::local_files::file_or_files::PathType::UriFile;
use substrait::proto::rel_common::{Emit, EmitKind};
use substrait::proto::set_rel::SetOp;
use substrait::proto::{
    aggregate_function::AggregationInvocation,
    expression::{
        field_reference::ReferenceType::DirectReference, literal::LiteralType,
        reference_segment::ReferenceType::StructField,
        window_function::bound as SubstraitBound,
        window_function::bound::Kind as BoundKind, window_function::Bound,
        window_function::BoundsType, MaskExpression, RexType,
    },
    fetch_rel,
    function_argument::ArgType,
    join_rel, plan_rel, r#type,
    read_rel::ReadType,
    rel::RelType,
    rel_common,
    sort_field::{SortDirection, SortKind::*},
    AggregateFunction, AggregateRel, ConsistentPartitionWindowRel, CrossRel,
    DynamicParameter, ExchangeRel, Expression, ExtendedExpression, ExtensionLeafRel,
    ExtensionMultiRel, ExtensionSingleRel, FetchRel, FilterRel, FunctionArgument,
    JoinRel, NamedStruct, Plan, ProjectRel, ReadRel, Rel, RelCommon, SetRel, SortField,
    SortRel, Type,
};

#[async_trait]
/// This trait is used to consume Substrait plans, converting them into DataFusion Logical Plans.
/// It can be implemented by users to allow for custom handling of relations, expressions, etc.
///
/// Combined with the [crate::logical_plan::producer::SubstraitProducer] this allows for fully
/// customizable Substrait serde.
///
/// # Example Usage
///
/// ```
/// # use async_trait::async_trait;
/// # use datafusion::catalog::TableProvider;
/// # use datafusion::common::{not_impl_err, substrait_err, DFSchema, ScalarValue, TableReference};
/// # use datafusion::error::Result;
/// # use datafusion::execution::{FunctionRegistry, SessionState};
/// # use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
/// # use std::sync::Arc;
/// # use substrait::proto;
/// # use substrait::proto::{ExtensionLeafRel, FilterRel, ProjectRel};
/// # use datafusion::arrow::datatypes::DataType;
/// # use datafusion::logical_expr::expr::ScalarFunction;
/// # use datafusion_substrait::extensions::Extensions;
/// # use datafusion_substrait::logical_plan::consumer::{
/// #     from_project_rel, from_substrait_rel, from_substrait_rex, SubstraitConsumer
/// # };
///
/// struct CustomSubstraitConsumer {
///     extensions: Arc<Extensions>,
///     state: Arc<SessionState>,
/// }
///
/// #[async_trait]
/// impl SubstraitConsumer for CustomSubstraitConsumer {
///     async fn resolve_table_ref(
///         &self,
///         table_ref: &TableReference,
///     ) -> Result<Option<Arc<dyn TableProvider>>> {
///         let table = table_ref.table().to_string();
///         let schema = self.state.schema_for_ref(table_ref.clone())?;
///         let table_provider = schema.table(&table).await?;
///         Ok(table_provider)
///     }
///
///     fn get_extensions(&self) -> &Extensions {
///         self.extensions.as_ref()
///     }
///
///     fn get_function_registry(&self) -> &impl FunctionRegistry {
///         self.state.as_ref()
///     }
///
///     // You can reuse existing consumer code to assist in handling advanced extensions
///     async fn consume_project(&self, rel: &ProjectRel) -> Result<LogicalPlan> {
///         let df_plan = from_project_rel(self, rel).await?;
///         if let Some(advanced_extension) = rel.advanced_extension.as_ref() {
///             not_impl_err!(
///                 "decode and handle an advanced extension: {:?}",
///                 advanced_extension
///             )
///         } else {
///             Ok(df_plan)
///         }
///     }
///
///     // You can implement a fully custom consumer method if you need special handling
///     async fn consume_filter(&self, rel: &FilterRel) -> Result<LogicalPlan> {
///         let input = self.consume_rel(rel.input.as_ref().unwrap()).await?;
///         let expression =
///             self.consume_expression(rel.condition.as_ref().unwrap(), input.schema())
///                 .await?;
///         // though this one is quite boring
///         LogicalPlanBuilder::from(input).filter(expression)?.build()
///     }
///
///     // You can add handlers for extension relations
///     async fn consume_extension_leaf(
///         &self,
///         rel: &ExtensionLeafRel,
///     ) -> Result<LogicalPlan> {
///         not_impl_err!(
///             "handle protobuf Any {} as you need",
///             rel.detail.as_ref().unwrap().type_url
///         )
///     }
///
///     // and handlers for user-define types
///     fn consume_user_defined_type(&self, typ: &proto::r#type::UserDefined) -> Result<DataType> {
///         let type_string = self.extensions.types.get(&typ.type_reference).unwrap();
///         match type_string.as_str() {
///             "u!foo" => not_impl_err!("handle foo conversion"),
///             "u!bar" => not_impl_err!("handle bar conversion"),
///             _ => substrait_err!("unexpected type")
///         }
///     }
///
///     // and user-defined literals
///     fn consume_user_defined_literal(&self, literal: &proto::expression::literal::UserDefined) -> Result<ScalarValue> {
///         let type_string = self.extensions.types.get(&literal.type_reference).unwrap();
///         match type_string.as_str() {
///             "u!foo" => not_impl_err!("handle foo conversion"),
///             "u!bar" => not_impl_err!("handle bar conversion"),
///             _ => substrait_err!("unexpected type")
///         }
///     }
/// }
/// ```
///
pub trait SubstraitConsumer: Send + Sync + Sized {
    async fn resolve_table_ref(
        &self,
        table_ref: &TableReference,
    ) -> Result<Option<Arc<dyn TableProvider>>>;

    // TODO: Remove these two methods
    //   Ideally, the abstract consumer should not place any constraints on implementations.
    //   The functionality for which the Extensions and FunctionRegistry is needed should be abstracted
    //   out into methods on the trait. As an example, resolve_table_reference is such a method.
    //   See: https://github.com/apache/datafusion/issues/13863
    fn get_extensions(&self) -> &Extensions;
    fn get_function_registry(&self) -> &impl FunctionRegistry;

    // Relation Methods
    // There is one method per Substrait relation to allow for easy overriding of consumer behaviour.
    // These methods have default implementations calling the common handler code, to allow for users
    // to re-use common handling logic.

    /// All [Rel]s to be converted pass through this method.
    /// You can provide your own implementation if you wish to customize the conversion behaviour.
    async fn consume_rel(&self, rel: &Rel) -> Result<LogicalPlan> {
        from_substrait_rel(self, rel).await
    }

    async fn consume_read(&self, rel: &ReadRel) -> Result<LogicalPlan> {
        from_read_rel(self, rel).await
    }

    async fn consume_filter(&self, rel: &FilterRel) -> Result<LogicalPlan> {
        from_filter_rel(self, rel).await
    }

    async fn consume_fetch(&self, rel: &FetchRel) -> Result<LogicalPlan> {
        from_fetch_rel(self, rel).await
    }

    async fn consume_aggregate(&self, rel: &AggregateRel) -> Result<LogicalPlan> {
        from_aggregate_rel(self, rel).await
    }

    async fn consume_sort(&self, rel: &SortRel) -> Result<LogicalPlan> {
        from_sort_rel(self, rel).await
    }

    async fn consume_join(&self, rel: &JoinRel) -> Result<LogicalPlan> {
        from_join_rel(self, rel).await
    }

    async fn consume_project(&self, rel: &ProjectRel) -> Result<LogicalPlan> {
        from_project_rel(self, rel).await
    }

    async fn consume_set(&self, rel: &SetRel) -> Result<LogicalPlan> {
        from_set_rel(self, rel).await
    }

    async fn consume_cross(&self, rel: &CrossRel) -> Result<LogicalPlan> {
        from_cross_rel(self, rel).await
    }

    async fn consume_consistent_partition_window(
        &self,
        _rel: &ConsistentPartitionWindowRel,
    ) -> Result<LogicalPlan> {
        not_impl_err!("Consistent Partition Window Rel not supported")
    }

    async fn consume_exchange(&self, rel: &ExchangeRel) -> Result<LogicalPlan> {
        from_exchange_rel(self, rel).await
    }

    // Expression Methods
    // There is one method per Substrait expression to allow for easy overriding of consumer behaviour
    // These methods have default implementations calling the common handler code, to allow for users
    // to re-use common handling logic.

    /// All [Expression]s to be converted pass through this method.
    /// You can provide your own implementation if you wish to customize the conversion behaviour.
    async fn consume_expression(
        &self,
        expr: &Expression,
        input_schema: &DFSchema,
    ) -> Result<Expr> {
        from_substrait_rex(self, expr, input_schema).await
    }

    async fn consume_literal(&self, expr: &Literal) -> Result<Expr> {
        from_literal(self, expr).await
    }

    async fn consume_field_reference(
        &self,
        expr: &FieldReference,
        input_schema: &DFSchema,
    ) -> Result<Expr> {
        from_field_reference(self, expr, input_schema).await
    }

    async fn consume_scalar_function(
        &self,
        expr: &ScalarFunction,
        input_schema: &DFSchema,
    ) -> Result<Expr> {
        from_scalar_function(self, expr, input_schema).await
    }

    async fn consume_window_function(
        &self,
        expr: &WindowFunction,
        input_schema: &DFSchema,
    ) -> Result<Expr> {
        from_window_function(self, expr, input_schema).await
    }

    async fn consume_if_then(
        &self,
        expr: &IfThen,
        input_schema: &DFSchema,
    ) -> Result<Expr> {
        from_if_then(self, expr, input_schema).await
    }

    async fn consume_switch(
        &self,
        _expr: &SwitchExpression,
        _input_schema: &DFSchema,
    ) -> Result<Expr> {
        not_impl_err!("Switch expression not supported")
    }

    async fn consume_singular_or_list(
        &self,
        expr: &SingularOrList,
        input_schema: &DFSchema,
    ) -> Result<Expr> {
        from_singular_or_list(self, expr, input_schema).await
    }

    async fn consume_multi_or_list(
        &self,
        _expr: &MultiOrList,
        _input_schema: &DFSchema,
    ) -> Result<Expr> {
        not_impl_err!("Multi Or List expression not supported")
    }

    async fn consume_cast(
        &self,
        expr: &substrait_expression::Cast,
        input_schema: &DFSchema,
    ) -> Result<Expr> {
        from_cast(self, expr, input_schema).await
    }

    async fn consume_subquery(
        &self,
        expr: &substrait_expression::Subquery,
        input_schema: &DFSchema,
    ) -> Result<Expr> {
        from_subquery(self, expr, input_schema).await
    }

    async fn consume_nested(
        &self,
        _expr: &Nested,
        _input_schema: &DFSchema,
    ) -> Result<Expr> {
        not_impl_err!("Nested expression not supported")
    }

    async fn consume_enum(&self, _expr: &Enum, _input_schema: &DFSchema) -> Result<Expr> {
        not_impl_err!("Enum expression not supported")
    }

    async fn consume_dynamic_parameter(
        &self,
        _expr: &DynamicParameter,
        _input_schema: &DFSchema,
    ) -> Result<Expr> {
        not_impl_err!("Dynamic Parameter expression not supported")
    }

    // User-Defined Functionality

    // The details of extension relations, and how to handle them, are fully up to users to specify.
    // The following methods allow users to customize the consumer behaviour

    async fn consume_extension_leaf(
        &self,
        rel: &ExtensionLeafRel,
    ) -> Result<LogicalPlan> {
        if let Some(detail) = rel.detail.as_ref() {
            return substrait_err!(
                "Missing handler for ExtensionLeafRel: {}",
                detail.type_url
            );
        }
        substrait_err!("Missing handler for ExtensionLeafRel")
    }

    async fn consume_extension_single(
        &self,
        rel: &ExtensionSingleRel,
    ) -> Result<LogicalPlan> {
        if let Some(detail) = rel.detail.as_ref() {
            return substrait_err!(
                "Missing handler for ExtensionSingleRel: {}",
                detail.type_url
            );
        }
        substrait_err!("Missing handler for ExtensionSingleRel")
    }

    async fn consume_extension_multi(
        &self,
        rel: &ExtensionMultiRel,
    ) -> Result<LogicalPlan> {
        if let Some(detail) = rel.detail.as_ref() {
            return substrait_err!(
                "Missing handler for ExtensionMultiRel: {}",
                detail.type_url
            );
        }
        substrait_err!("Missing handler for ExtensionMultiRel")
    }

    // Users can bring their own types to Substrait which require custom handling

    fn consume_user_defined_type(
        &self,
        user_defined_type: &r#type::UserDefined,
    ) -> Result<DataType> {
        substrait_err!(
            "Missing handler for user-defined type: {}",
            user_defined_type.type_reference
        )
    }

    fn consume_user_defined_literal(
        &self,
        user_defined_literal: &proto::expression::literal::UserDefined,
    ) -> Result<ScalarValue> {
        substrait_err!(
            "Missing handler for user-defined literals {}",
            user_defined_literal.type_reference
        )
    }
}

/// Convert Substrait Rel to DataFusion DataFrame
#[async_recursion]
pub async fn from_substrait_rel(
    consumer: &impl SubstraitConsumer,
    relation: &Rel,
) -> Result<LogicalPlan> {
    let plan: Result<LogicalPlan> = match &relation.rel_type {
        Some(rel_type) => match rel_type {
            RelType::Read(rel) => consumer.consume_read(rel).await,
            RelType::Filter(rel) => consumer.consume_filter(rel).await,
            RelType::Fetch(rel) => consumer.consume_fetch(rel).await,
            RelType::Aggregate(rel) => consumer.consume_aggregate(rel).await,
            RelType::Sort(rel) => consumer.consume_sort(rel).await,
            RelType::Join(rel) => consumer.consume_join(rel).await,
            RelType::Project(rel) => consumer.consume_project(rel).await,
            RelType::Set(rel) => consumer.consume_set(rel).await,
            RelType::ExtensionSingle(rel) => consumer.consume_extension_single(rel).await,
            RelType::ExtensionMulti(rel) => consumer.consume_extension_multi(rel).await,
            RelType::ExtensionLeaf(rel) => consumer.consume_extension_leaf(rel).await,
            RelType::Cross(rel) => consumer.consume_cross(rel).await,
            RelType::Window(rel) => {
                consumer.consume_consistent_partition_window(rel).await
            }
            RelType::Exchange(rel) => consumer.consume_exchange(rel).await,
            rt => not_impl_err!("{rt:?} rel not supported yet"),
        },
        None => return substrait_err!("rel must set rel_type"),
    };
    apply_emit_kind(retrieve_rel_common(relation), plan?)
}

/// Default SubstraitConsumer for converting standard Substrait without user-defined extensions.
///
/// Used as the consumer in [from_substrait_plan]
pub struct DefaultSubstraitConsumer<'a> {
    extensions: &'a Extensions,
    state: &'a SessionState,
}

impl<'a> DefaultSubstraitConsumer<'a> {
    pub fn new(extensions: &'a Extensions, state: &'a SessionState) -> Self {
        DefaultSubstraitConsumer { extensions, state }
    }
}

#[async_trait]
impl SubstraitConsumer for DefaultSubstraitConsumer<'_> {
    async fn resolve_table_ref(
        &self,
        table_ref: &TableReference,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        let table = table_ref.table().to_string();
        let schema = self.state.schema_for_ref(table_ref.clone())?;
        let table_provider = schema.table(&table).await?;
        Ok(table_provider)
    }

    fn get_extensions(&self) -> &Extensions {
        self.extensions
    }

    fn get_function_registry(&self) -> &impl FunctionRegistry {
        self.state
    }

    async fn consume_extension_leaf(
        &self,
        rel: &ExtensionLeafRel,
    ) -> Result<LogicalPlan> {
        let Some(ext_detail) = &rel.detail else {
            return substrait_err!("Unexpected empty detail in ExtensionLeafRel");
        };
        let plan = self
            .state
            .serializer_registry()
            .deserialize_logical_plan(&ext_detail.type_url, &ext_detail.value)?;
        Ok(LogicalPlan::Extension(Extension { node: plan }))
    }

    async fn consume_extension_single(
        &self,
        rel: &ExtensionSingleRel,
    ) -> Result<LogicalPlan> {
        let Some(ext_detail) = &rel.detail else {
            return substrait_err!("Unexpected empty detail in ExtensionSingleRel");
        };
        let plan = self
            .state
            .serializer_registry()
            .deserialize_logical_plan(&ext_detail.type_url, &ext_detail.value)?;
        let Some(input_rel) = &rel.input else {
            return substrait_err!(
                    "ExtensionSingleRel missing input rel, try using ExtensionLeafRel instead"
                );
        };
        let input_plan = self.consume_rel(input_rel).await?;
        let plan = plan.with_exprs_and_inputs(plan.expressions(), vec![input_plan])?;
        Ok(LogicalPlan::Extension(Extension { node: plan }))
    }

    async fn consume_extension_multi(
        &self,
        rel: &ExtensionMultiRel,
    ) -> Result<LogicalPlan> {
        let Some(ext_detail) = &rel.detail else {
            return substrait_err!("Unexpected empty detail in ExtensionMultiRel");
        };
        let plan = self
            .state
            .serializer_registry()
            .deserialize_logical_plan(&ext_detail.type_url, &ext_detail.value)?;
        let mut inputs = Vec::with_capacity(rel.inputs.len());
        for input in &rel.inputs {
            let input_plan = self.consume_rel(input).await?;
            inputs.push(input_plan);
        }
        let plan = plan.with_exprs_and_inputs(plan.expressions(), inputs)?;
        Ok(LogicalPlan::Extension(Extension { node: plan }))
    }
}

// Substrait PrecisionTimestampTz indicates that the timestamp is relative to UTC, which
// is the same as the expectation for any non-empty timezone in DF, so any non-empty timezone
// results in correct points on the timeline, and we pick UTC as a reasonable default.
// However, DF uses the timezone also for some arithmetic and display purposes (see e.g.
// https://github.com/apache/arrow-rs/blob/ee5694078c86c8201549654246900a4232d531a9/arrow-cast/src/cast/mod.rs#L1749).
const DEFAULT_TIMEZONE: &str = "UTC";

pub fn name_to_op(name: &str) -> Option<Operator> {
    match name {
        "equal" => Some(Operator::Eq),
        "not_equal" => Some(Operator::NotEq),
        "lt" => Some(Operator::Lt),
        "lte" => Some(Operator::LtEq),
        "gt" => Some(Operator::Gt),
        "gte" => Some(Operator::GtEq),
        "add" => Some(Operator::Plus),
        "subtract" => Some(Operator::Minus),
        "multiply" => Some(Operator::Multiply),
        "divide" => Some(Operator::Divide),
        "mod" => Some(Operator::Modulo),
        "modulus" => Some(Operator::Modulo),
        "and" => Some(Operator::And),
        "or" => Some(Operator::Or),
        "is_distinct_from" => Some(Operator::IsDistinctFrom),
        "is_not_distinct_from" => Some(Operator::IsNotDistinctFrom),
        "regex_match" => Some(Operator::RegexMatch),
        "regex_imatch" => Some(Operator::RegexIMatch),
        "regex_not_match" => Some(Operator::RegexNotMatch),
        "regex_not_imatch" => Some(Operator::RegexNotIMatch),
        "bitwise_and" => Some(Operator::BitwiseAnd),
        "bitwise_or" => Some(Operator::BitwiseOr),
        "str_concat" => Some(Operator::StringConcat),
        "at_arrow" => Some(Operator::AtArrow),
        "arrow_at" => Some(Operator::ArrowAt),
        "bitwise_xor" => Some(Operator::BitwiseXor),
        "bitwise_shift_right" => Some(Operator::BitwiseShiftRight),
        "bitwise_shift_left" => Some(Operator::BitwiseShiftLeft),
        _ => None,
    }
}

pub fn substrait_fun_name(name: &str) -> &str {
    let name = match name.rsplit_once(':') {
        // Since 0.32.0, Substrait requires the function names to be in a compound format
        // https://substrait.io/extensions/#function-signature-compound-names
        // for example, `add:i8_i8`.
        // On the consumer side, we don't really care about the signature though, just the name.
        Some((name, _)) => name,
        None => name,
    };
    name
}

fn split_eq_and_noneq_join_predicate_with_nulls_equality(
    filter: &Expr,
) -> (Vec<(Column, Column)>, bool, Option<Expr>) {
    let exprs = split_conjunction(filter);

    let mut accum_join_keys: Vec<(Column, Column)> = vec![];
    let mut accum_filters: Vec<Expr> = vec![];
    let mut nulls_equal_nulls = false;

    for expr in exprs {
        #[allow(clippy::collapsible_match)]
        match expr {
            Expr::BinaryExpr(binary_expr) => match binary_expr {
                x @ (BinaryExpr {
                    left,
                    op: Operator::Eq,
                    right,
                }
                | BinaryExpr {
                    left,
                    op: Operator::IsNotDistinctFrom,
                    right,
                }) => {
                    nulls_equal_nulls = match x.op {
                        Operator::Eq => false,
                        Operator::IsNotDistinctFrom => true,
                        _ => unreachable!(),
                    };

                    match (left.as_ref(), right.as_ref()) {
                        (Expr::Column(l), Expr::Column(r)) => {
                            accum_join_keys.push((l.clone(), r.clone()));
                        }
                        _ => accum_filters.push(expr.clone()),
                    }
                }
                _ => accum_filters.push(expr.clone()),
            },
            _ => accum_filters.push(expr.clone()),
        }
    }

    let join_filter = accum_filters.into_iter().reduce(Expr::and);
    (accum_join_keys, nulls_equal_nulls, join_filter)
}

async fn union_rels(
    consumer: &impl SubstraitConsumer,
    rels: &[Rel],
    is_all: bool,
) -> Result<LogicalPlan> {
    let mut union_builder = Ok(LogicalPlanBuilder::from(
        consumer.consume_rel(&rels[0]).await?,
    ));
    for input in &rels[1..] {
        let rel_plan = consumer.consume_rel(input).await?;

        union_builder = if is_all {
            union_builder?.union(rel_plan)
        } else {
            union_builder?.union_distinct(rel_plan)
        };
    }
    union_builder?.build()
}

async fn intersect_rels(
    consumer: &impl SubstraitConsumer,
    rels: &[Rel],
    is_all: bool,
) -> Result<LogicalPlan> {
    let mut rel = consumer.consume_rel(&rels[0]).await?;

    for input in &rels[1..] {
        rel = LogicalPlanBuilder::intersect(
            rel,
            consumer.consume_rel(input).await?,
            is_all,
        )?
    }

    Ok(rel)
}

async fn except_rels(
    consumer: &impl SubstraitConsumer,
    rels: &[Rel],
    is_all: bool,
) -> Result<LogicalPlan> {
    let mut rel = consumer.consume_rel(&rels[0]).await?;

    for input in &rels[1..] {
        rel = LogicalPlanBuilder::except(rel, consumer.consume_rel(input).await?, is_all)?
    }

    Ok(rel)
}

/// Convert Substrait Plan to DataFusion LogicalPlan
pub async fn from_substrait_plan(
    state: &SessionState,
    plan: &Plan,
) -> Result<LogicalPlan> {
    // Register function extension
    let extensions = Extensions::try_from(&plan.extensions)?;
    if !extensions.type_variations.is_empty() {
        return not_impl_err!("Type variation extensions are not supported");
    }

    let consumer = DefaultSubstraitConsumer {
        extensions: &extensions,
        state,
    };
    from_substrait_plan_with_consumer(&consumer, plan).await
}

/// Convert Substrait Plan to DataFusion LogicalPlan using the given consumer
pub async fn from_substrait_plan_with_consumer(
    consumer: &impl SubstraitConsumer,
    plan: &Plan,
) -> Result<LogicalPlan> {
    match plan.relations.len() {
        1 => {
            match plan.relations[0].rel_type.as_ref() {
                Some(rt) => match rt {
                    plan_rel::RelType::Rel(rel) => Ok(consumer.consume_rel(rel).await?),
                    plan_rel::RelType::Root(root) => {
                        let plan = consumer.consume_rel(root.input.as_ref().unwrap()).await?;
                        if root.names.is_empty() {
                            // Backwards compatibility for plans missing names
                            return Ok(plan);
                        }
                        let renamed_schema = make_renamed_schema(plan.schema(), &root.names)?;
                        if renamed_schema.has_equivalent_names_and_types(plan.schema()).is_ok() {
                            // Nothing to do if the schema is already equivalent
                            return Ok(plan);
                        }
                        match plan {
                            // If the last node of the plan produces expressions, bake the renames into those expressions.
                            // This isn't necessary for correctness, but helps with roundtrip tests.
                            LogicalPlan::Projection(p) => Ok(LogicalPlan::Projection(Projection::try_new(rename_expressions(p.expr, p.input.schema(), renamed_schema.fields())?, p.input)?)),
                            LogicalPlan::Aggregate(a) => {
                                let (group_fields, expr_fields) = renamed_schema.fields().split_at(a.group_expr.len());
                                let new_group_exprs = rename_expressions(a.group_expr, a.input.schema(), group_fields)?;
                                let new_aggr_exprs = rename_expressions(a.aggr_expr, a.input.schema(), expr_fields)?;
                                Ok(LogicalPlan::Aggregate(Aggregate::try_new(a.input, new_group_exprs, new_aggr_exprs)?))
                            },
                            // There are probably more plans where we could bake things in, can add them later as needed.
                            // Otherwise, add a new Project to handle the renaming.
                            _ => Ok(LogicalPlan::Projection(Projection::try_new(rename_expressions(plan.schema().columns().iter().map(|c| col(c.to_owned())), plan.schema(), renamed_schema.fields())?, Arc::new(plan))?))
                        }
                    }
                },
                None => plan_err!("Cannot parse plan relation: None")
            }
        },
        _ => not_impl_err!(
            "Substrait plan with more than 1 relation trees not supported. Number of relation trees: {:?}",
            plan.relations.len()
        )
    }
}

/// An ExprContainer is a container for a collection of expressions with a common input schema
///
/// In addition, each expression is associated with a field, which defines the
/// expression's output.  The data type and nullability of the field are calculated from the
/// expression and the input schema.  However the names of the field (and its nested fields) are
/// derived from the Substrait message.
pub struct ExprContainer {
    /// The input schema for the expressions
    pub input_schema: DFSchemaRef,
    /// The expressions
    ///
    /// Each item contains an expression and the field that defines the expected nullability and name of the expr's output
    pub exprs: Vec<(Expr, Field)>,
}

/// Convert Substrait ExtendedExpression to ExprContainer
///
/// A Substrait ExtendedExpression message contains one or more expressions,
/// with names for the outputs, and an input schema.  These pieces are all included
/// in the ExprContainer.
///
/// This is a top-level message and can be used to send expressions (not plans)
/// between systems.  This is often useful for scenarios like pushdown where filter
/// expressions need to be sent to remote systems.
pub async fn from_substrait_extended_expr(
    state: &SessionState,
    extended_expr: &ExtendedExpression,
) -> Result<ExprContainer> {
    // Register function extension
    let extensions = Extensions::try_from(&extended_expr.extensions)?;
    if !extensions.type_variations.is_empty() {
        return not_impl_err!("Type variation extensions are not supported");
    }

    let consumer = DefaultSubstraitConsumer {
        extensions: &extensions,
        state,
    };

    let input_schema = DFSchemaRef::new(match &extended_expr.base_schema {
        Some(base_schema) => from_substrait_named_struct(&consumer, base_schema),
        None => {
            plan_err!("required property `base_schema` missing from Substrait ExtendedExpression message")
        }
    }?);

    // Parse expressions
    let mut exprs = Vec::with_capacity(extended_expr.referred_expr.len());
    for (expr_idx, substrait_expr) in extended_expr.referred_expr.iter().enumerate() {
        let scalar_expr = match &substrait_expr.expr_type {
            Some(ExprType::Expression(scalar_expr)) => Ok(scalar_expr),
            Some(ExprType::Measure(_)) => {
                not_impl_err!("Measure expressions are not yet supported")
            }
            None => {
                plan_err!("required property `expr_type` missing from Substrait ExpressionReference message")
            }
        }?;
        let expr = consumer
            .consume_expression(scalar_expr, &input_schema)
            .await?;
        let (output_type, expected_nullability) =
            expr.data_type_and_nullable(&input_schema)?;
        let output_field = Field::new("", output_type, expected_nullability);
        let mut names_idx = 0;
        let output_field = rename_field(
            &output_field,
            &substrait_expr.output_names,
            expr_idx,
            &mut names_idx,
            /*rename_self=*/ true,
        )?;
        exprs.push((expr, output_field));
    }

    Ok(ExprContainer {
        input_schema,
        exprs,
    })
}

pub fn apply_masking(
    schema: DFSchema,
    mask_expression: &::core::option::Option<MaskExpression>,
) -> Result<DFSchema> {
    match mask_expression {
        Some(MaskExpression { select, .. }) => match &select.as_ref() {
            Some(projection) => {
                let column_indices: Vec<usize> = projection
                    .struct_items
                    .iter()
                    .map(|item| item.field as usize)
                    .collect();

                let fields = column_indices
                    .iter()
                    .map(|i| schema.qualified_field(*i))
                    .map(|(qualifier, field)| {
                        (qualifier.cloned(), Arc::new(field.clone()))
                    })
                    .collect();

                Ok(DFSchema::new_with_metadata(
                    fields,
                    schema.metadata().clone(),
                )?)
            }
            None => Ok(schema),
        },
        None => Ok(schema),
    }
}

/// Ensure the expressions have the right name(s) according to the new schema.
/// This includes the top-level (column) name, which will be renamed through aliasing if needed,
/// as well as nested names (if the expression produces any struct types), which will be renamed
/// through casting if needed.
fn rename_expressions(
    exprs: impl IntoIterator<Item = Expr>,
    input_schema: &DFSchema,
    new_schema_fields: &[Arc<Field>],
) -> Result<Vec<Expr>> {
    exprs
        .into_iter()
        .zip(new_schema_fields)
        .map(|(old_expr, new_field)| {
            // Check if type (i.e. nested struct field names) match, use Cast to rename if needed
            let new_expr = if &old_expr.get_type(input_schema)? != new_field.data_type() {
                Expr::Cast(Cast::new(
                    Box::new(old_expr),
                    new_field.data_type().to_owned(),
                ))
            } else {
                old_expr
            };
            // Alias column if needed to fix the top-level name
            match &new_expr {
                // If expr is a column reference, alias_if_changed would cause an aliasing if the old expr has a qualifier
                Expr::Column(c) if &c.name == new_field.name() => Ok(new_expr),
                _ => new_expr.alias_if_changed(new_field.name().to_owned()),
            }
        })
        .collect()
}

fn rename_field(
    field: &Field,
    dfs_names: &Vec<String>,
    unnamed_field_suffix: usize, // If Substrait doesn't provide a name, we'll use this "c{unnamed_field_suffix}"
    name_idx: &mut usize,        // Index into dfs_names
    rename_self: bool, // Some fields (e.g. list items) don't have names in Substrait and this will be false to keep old name
) -> Result<Field> {
    let name = if rename_self {
        next_struct_field_name(unnamed_field_suffix, dfs_names, name_idx)?
    } else {
        field.name().to_string()
    };
    match field.data_type() {
        DataType::Struct(children) => {
            let children = children
                .iter()
                .enumerate()
                .map(|(child_idx, f)| {
                    rename_field(
                        f.as_ref(),
                        dfs_names,
                        child_idx,
                        name_idx,
                        /*rename_self=*/ true,
                    )
                })
                .collect::<Result<_>>()?;
            Ok(field
                .to_owned()
                .with_name(name)
                .with_data_type(DataType::Struct(children)))
        }
        DataType::List(inner) => {
            let renamed_inner = rename_field(
                inner.as_ref(),
                dfs_names,
                0,
                name_idx,
                /*rename_self=*/ false,
            )?;
            Ok(field
                .to_owned()
                .with_data_type(DataType::List(FieldRef::new(renamed_inner)))
                .with_name(name))
        }
        DataType::LargeList(inner) => {
            let renamed_inner = rename_field(
                inner.as_ref(),
                dfs_names,
                0,
                name_idx,
                /*rename_self= */ false,
            )?;
            Ok(field
                .to_owned()
                .with_data_type(DataType::LargeList(FieldRef::new(renamed_inner)))
                .with_name(name))
        }
        _ => Ok(field.to_owned().with_name(name)),
    }
}

/// Produce a version of the given schema with names matching the given list of names.
/// Substrait doesn't deal with column (incl. nested struct field) names within the schema,
/// but it does give us the list of expected names at the end of the plan, so we use this
/// to rename the schema to match the expected names.
fn make_renamed_schema(
    schema: &DFSchemaRef,
    dfs_names: &Vec<String>,
) -> Result<DFSchema> {
    let mut name_idx = 0;

    let (qualifiers, fields): (_, Vec<Field>) = schema
        .iter()
        .enumerate()
        .map(|(field_idx, (q, f))| {
            let renamed_f = rename_field(
                f.as_ref(),
                dfs_names,
                field_idx,
                &mut name_idx,
                /*rename_self=*/ true,
            )?;
            Ok((q.cloned(), renamed_f))
        })
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .unzip();

    if name_idx != dfs_names.len() {
        return substrait_err!(
            "Names list must match exactly to nested schema, but found {} uses for {} names",
            name_idx,
            dfs_names.len());
    }

    DFSchema::from_field_specific_qualified_schema(
        qualifiers,
        &Arc::new(Schema::new(fields)),
    )
}

#[async_recursion]
pub async fn from_project_rel(
    consumer: &impl SubstraitConsumer,
    p: &ProjectRel,
) -> Result<LogicalPlan> {
    if let Some(input) = p.input.as_ref() {
        let input = consumer.consume_rel(input).await?;
        let original_schema = Arc::clone(input.schema());

        // Ensure that all expressions have a unique display name, so that
        // validate_unique_names does not fail when constructing the project.
        let mut name_tracker = NameTracker::new();

        // By default, a Substrait Project emits all inputs fields followed by all expressions.
        // We build the explicit expressions first, and then the input expressions to avoid
        // adding aliases to the explicit expressions (as part of ensuring unique names).
        //
        // This is helpful for plan visualization and tests, because when DataFusion produces
        // Substrait Projects it adds an output mapping that excludes all input columns
        // leaving only explicit expressions.

        let mut explicit_exprs: Vec<Expr> = vec![];
        // For WindowFunctions, we need to wrap them in a Window relation. If there are duplicates,
        // we can do the window'ing only once, then the project will duplicate the result.
        // Order here doesn't matter since LPB::window_plan sorts the expressions.
        let mut window_exprs: HashSet<Expr> = HashSet::new();
        for expr in &p.expressions {
            let e = consumer
                .consume_expression(expr, input.clone().schema())
                .await?;
            // if the expression is WindowFunction, wrap in a Window relation
            if let Expr::WindowFunction(_) = &e {
                // Adding the same expression here and in the project below
                // works because the project's builder uses columnize_expr(..)
                // to transform it into a column reference
                window_exprs.insert(e.clone());
            }
            explicit_exprs.push(name_tracker.get_uniquely_named_expr(e)?);
        }

        let input = if !window_exprs.is_empty() {
            LogicalPlanBuilder::window_plan(input, window_exprs)?
        } else {
            input
        };

        let mut final_exprs: Vec<Expr> = vec![];
        for index in 0..original_schema.fields().len() {
            let e = Expr::Column(Column::from(original_schema.qualified_field(index)));
            final_exprs.push(name_tracker.get_uniquely_named_expr(e)?);
        }
        final_exprs.append(&mut explicit_exprs);
        project(input, final_exprs)
    } else {
        not_impl_err!("Projection without an input is not supported")
    }
}

#[async_recursion]
pub async fn from_filter_rel(
    consumer: &impl SubstraitConsumer,
    filter: &FilterRel,
) -> Result<LogicalPlan> {
    if let Some(input) = filter.input.as_ref() {
        let input = LogicalPlanBuilder::from(consumer.consume_rel(input).await?);
        if let Some(condition) = filter.condition.as_ref() {
            let expr = consumer
                .consume_expression(condition, input.schema())
                .await?;
            input.filter(expr)?.build()
        } else {
            not_impl_err!("Filter without an condition is not valid")
        }
    } else {
        not_impl_err!("Filter without an input is not valid")
    }
}

#[async_recursion]
pub async fn from_fetch_rel(
    consumer: &impl SubstraitConsumer,
    fetch: &FetchRel,
) -> Result<LogicalPlan> {
    if let Some(input) = fetch.input.as_ref() {
        let input = LogicalPlanBuilder::from(consumer.consume_rel(input).await?);
        let empty_schema = DFSchemaRef::new(DFSchema::empty());
        let offset = match &fetch.offset_mode {
            Some(fetch_rel::OffsetMode::Offset(offset)) => Some(lit(*offset)),
            Some(fetch_rel::OffsetMode::OffsetExpr(expr)) => {
                Some(consumer.consume_expression(expr, &empty_schema).await?)
            }
            None => None,
        };
        let count = match &fetch.count_mode {
            Some(fetch_rel::CountMode::Count(count)) => {
                // -1 means that ALL records should be returned, equivalent to None
                (*count != -1).then(|| lit(*count))
            }
            Some(fetch_rel::CountMode::CountExpr(expr)) => {
                Some(consumer.consume_expression(expr, &empty_schema).await?)
            }
            None => None,
        };
        input.limit_by_expr(offset, count)?.build()
    } else {
        not_impl_err!("Fetch without an input is not valid")
    }
}

pub async fn from_sort_rel(
    consumer: &impl SubstraitConsumer,
    sort: &SortRel,
) -> Result<LogicalPlan> {
    if let Some(input) = sort.input.as_ref() {
        let input = LogicalPlanBuilder::from(consumer.consume_rel(input).await?);
        let sorts = from_substrait_sorts(consumer, &sort.sorts, input.schema()).await?;
        input.sort(sorts)?.build()
    } else {
        not_impl_err!("Sort without an input is not valid")
    }
}

pub async fn from_aggregate_rel(
    consumer: &impl SubstraitConsumer,
    agg: &AggregateRel,
) -> Result<LogicalPlan> {
    if let Some(input) = agg.input.as_ref() {
        let input = LogicalPlanBuilder::from(consumer.consume_rel(input).await?);
        let mut ref_group_exprs = vec![];

        for e in &agg.grouping_expressions {
            let x = consumer.consume_expression(e, input.schema()).await?;
            ref_group_exprs.push(x);
        }

        let mut group_exprs = vec![];
        let mut aggr_exprs = vec![];

        match agg.groupings.len() {
            1 => {
                group_exprs.extend_from_slice(
                    &from_substrait_grouping(
                        consumer,
                        &agg.groupings[0],
                        &ref_group_exprs,
                        input.schema(),
                    )
                    .await?,
                );
            }
            _ => {
                let mut grouping_sets = vec![];
                for grouping in &agg.groupings {
                    let grouping_set = from_substrait_grouping(
                        consumer,
                        grouping,
                        &ref_group_exprs,
                        input.schema(),
                    )
                    .await?;
                    grouping_sets.push(grouping_set);
                }
                // Single-element grouping expression of type Expr::GroupingSet.
                // Note that GroupingSet::Rollup would become GroupingSet::GroupingSets, when
                // parsed by the producer and consumer, since Substrait does not have a type dedicated
                // to ROLLUP. Only vector of Groupings (grouping sets) is available.
                group_exprs
                    .push(Expr::GroupingSet(GroupingSet::GroupingSets(grouping_sets)));
            }
        };

        for m in &agg.measures {
            let filter = match &m.filter {
                Some(fil) => Some(Box::new(
                    consumer.consume_expression(fil, input.schema()).await?,
                )),
                None => None,
            };
            let agg_func = match &m.measure {
                Some(f) => {
                    let distinct = match f.invocation {
                        _ if f.invocation == AggregationInvocation::Distinct as i32 => {
                            true
                        }
                        _ if f.invocation == AggregationInvocation::All as i32 => false,
                        _ => false,
                    };
                    let order_by = if !f.sorts.is_empty() {
                        Some(
                            from_substrait_sorts(consumer, &f.sorts, input.schema())
                                .await?,
                        )
                    } else {
                        None
                    };

                    from_substrait_agg_func(
                        consumer,
                        f,
                        input.schema(),
                        filter,
                        order_by,
                        distinct,
                    )
                    .await
                }
                None => {
                    not_impl_err!("Aggregate without aggregate function is not supported")
                }
            };
            aggr_exprs.push(agg_func?.as_ref().clone());
        }
        input.aggregate(group_exprs, aggr_exprs)?.build()
    } else {
        not_impl_err!("Aggregate without an input is not valid")
    }
}

pub async fn from_join_rel(
    consumer: &impl SubstraitConsumer,
    join: &JoinRel,
) -> Result<LogicalPlan> {
    if join.post_join_filter.is_some() {
        return not_impl_err!("JoinRel with post_join_filter is not yet supported");
    }

    let left: LogicalPlanBuilder = LogicalPlanBuilder::from(
        consumer.consume_rel(join.left.as_ref().unwrap()).await?,
    );
    let right = LogicalPlanBuilder::from(
        consumer.consume_rel(join.right.as_ref().unwrap()).await?,
    );
    let (left, right) = requalify_sides_if_needed(left, right)?;

    let join_type = from_substrait_jointype(join.r#type)?;
    // The join condition expression needs full input schema and not the output schema from join since we lose columns from
    // certain join types such as semi and anti joins
    let in_join_schema = left.schema().join(right.schema())?;

    // If join expression exists, parse the `on` condition expression, build join and return
    // Otherwise, build join with only the filter, without join keys
    match &join.expression.as_ref() {
        Some(expr) => {
            let on = consumer.consume_expression(expr, &in_join_schema).await?;
            // The join expression can contain both equal and non-equal ops.
            // As of datafusion 31.0.0, the equal and non equal join conditions are in separate fields.
            // So we extract each part as follows:
            // - If an Eq or IsNotDistinctFrom op is encountered, add the left column, right column and is_null_equal_nulls to `join_ons` vector
            // - Otherwise we add the expression to join_filter (use conjunction if filter already exists)
            let (join_ons, nulls_equal_nulls, join_filter) =
                split_eq_and_noneq_join_predicate_with_nulls_equality(&on);
            let (left_cols, right_cols): (Vec<_>, Vec<_>) =
                itertools::multiunzip(join_ons);
            left.join_detailed(
                right.build()?,
                join_type,
                (left_cols, right_cols),
                join_filter,
                nulls_equal_nulls,
            )?
            .build()
        }
        None => {
            let on: Vec<String> = vec![];
            left.join_detailed(right.build()?, join_type, (on.clone(), on), None, false)?
                .build()
        }
    }
}

pub async fn from_cross_rel(
    consumer: &impl SubstraitConsumer,
    cross: &CrossRel,
) -> Result<LogicalPlan> {
    let left = LogicalPlanBuilder::from(
        consumer.consume_rel(cross.left.as_ref().unwrap()).await?,
    );
    let right = LogicalPlanBuilder::from(
        consumer.consume_rel(cross.right.as_ref().unwrap()).await?,
    );
    let (left, right) = requalify_sides_if_needed(left, right)?;
    left.cross_join(right.build()?)?.build()
}

#[allow(deprecated)]
pub async fn from_read_rel(
    consumer: &impl SubstraitConsumer,
    read: &ReadRel,
) -> Result<LogicalPlan> {
    async fn read_with_schema(
        consumer: &impl SubstraitConsumer,
        table_ref: TableReference,
        schema: DFSchema,
        projection: &Option<MaskExpression>,
        filter: &Option<Box<Expression>>,
    ) -> Result<LogicalPlan> {
        let schema = schema.replace_qualifier(table_ref.clone());

        let filters = if let Some(f) = filter {
            let filter_expr = consumer.consume_expression(f, &schema).await?;
            split_conjunction_owned(filter_expr)
        } else {
            vec![]
        };

        let plan = {
            let provider = match consumer.resolve_table_ref(&table_ref).await? {
                Some(ref provider) => Arc::clone(provider),
                _ => return plan_err!("No table named '{table_ref}'"),
            };

            LogicalPlanBuilder::scan_with_filters(
                table_ref,
                provider_as_source(Arc::clone(&provider)),
                None,
                filters,
            )?
            .build()?
        };

        ensure_schema_compatibility(plan.schema(), schema.clone())?;

        let schema = apply_masking(schema, projection)?;

        apply_projection(plan, schema)
    }

    let named_struct = read.base_schema.as_ref().ok_or_else(|| {
        substrait_datafusion_err!("No base schema provided for Read Relation")
    })?;

    let substrait_schema = from_substrait_named_struct(consumer, named_struct)?;

    match &read.read_type {
        Some(ReadType::NamedTable(nt)) => {
            let table_reference = match nt.names.len() {
                0 => {
                    return plan_err!("No table name found in NamedTable");
                }
                1 => TableReference::Bare {
                    table: nt.names[0].clone().into(),
                },
                2 => TableReference::Partial {
                    schema: nt.names[0].clone().into(),
                    table: nt.names[1].clone().into(),
                },
                _ => TableReference::Full {
                    catalog: nt.names[0].clone().into(),
                    schema: nt.names[1].clone().into(),
                    table: nt.names[2].clone().into(),
                },
            };

            read_with_schema(
                consumer,
                table_reference,
                substrait_schema,
                &read.projection,
                &read.filter,
            )
            .await
        }
        Some(ReadType::VirtualTable(vt)) => {
            if vt.values.is_empty() {
                return Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: DFSchemaRef::new(substrait_schema),
                }));
            }

            let values = vt
                .values
                .iter()
                .map(|row| {
                    let mut name_idx = 0;
                    let lits = row
                        .fields
                        .iter()
                        .map(|lit| {
                            name_idx += 1; // top-level names are provided through schema
                            Ok(Expr::Literal(from_substrait_literal(
                                consumer,
                                lit,
                                &named_struct.names,
                                &mut name_idx,
                            )?))
                        })
                        .collect::<Result<_>>()?;
                    if name_idx != named_struct.names.len() {
                        return substrait_err!(
                                "Names list must match exactly to nested schema, but found {} uses for {} names",
                                name_idx,
                                named_struct.names.len()
                            );
                    }
                    Ok(lits)
                })
                .collect::<Result<_>>()?;

            Ok(LogicalPlan::Values(Values {
                schema: DFSchemaRef::new(substrait_schema),
                values,
            }))
        }
        Some(ReadType::LocalFiles(lf)) => {
            fn extract_filename(name: &str) -> Option<String> {
                let corrected_url =
                    if name.starts_with("file://") && !name.starts_with("file:///") {
                        name.replacen("file://", "file:///", 1)
                    } else {
                        name.to_string()
                    };

                Url::parse(&corrected_url).ok().and_then(|url| {
                    let path = url.path();
                    std::path::Path::new(path)
                        .file_name()
                        .map(|filename| filename.to_string_lossy().to_string())
                })
            }

            // we could use the file name to check the original table provider
            // TODO: currently does not support multiple local files
            let filename: Option<String> =
                lf.items.first().and_then(|x| match x.path_type.as_ref() {
                    Some(UriFile(name)) => extract_filename(name),
                    _ => None,
                });

            if lf.items.len() > 1 || filename.is_none() {
                return not_impl_err!("Only single file reads are supported");
            }
            let name = filename.unwrap();
            // directly use unwrap here since we could determine it is a valid one
            let table_reference = TableReference::Bare { table: name.into() };

            read_with_schema(
                consumer,
                table_reference,
                substrait_schema,
                &read.projection,
                &read.filter,
            )
            .await
        }
        _ => {
            not_impl_err!("Unsupported ReadType: {:?}", read.read_type)
        }
    }
}

pub async fn from_set_rel(
    consumer: &impl SubstraitConsumer,
    set: &SetRel,
) -> Result<LogicalPlan> {
    if set.inputs.len() < 2 {
        substrait_err!("Set operation requires at least two inputs")
    } else {
        match set.op() {
            SetOp::UnionAll => union_rels(consumer, &set.inputs, true).await,
            SetOp::UnionDistinct => union_rels(consumer, &set.inputs, false).await,
            SetOp::IntersectionPrimary => LogicalPlanBuilder::intersect(
                consumer.consume_rel(&set.inputs[0]).await?,
                union_rels(consumer, &set.inputs[1..], true).await?,
                false,
            ),
            SetOp::IntersectionMultiset => {
                intersect_rels(consumer, &set.inputs, false).await
            }
            SetOp::IntersectionMultisetAll => {
                intersect_rels(consumer, &set.inputs, true).await
            }
            SetOp::MinusPrimary => except_rels(consumer, &set.inputs, false).await,
            SetOp::MinusPrimaryAll => except_rels(consumer, &set.inputs, true).await,
            set_op => not_impl_err!("Unsupported set operator: {set_op:?}"),
        }
    }
}

pub async fn from_exchange_rel(
    consumer: &impl SubstraitConsumer,
    exchange: &ExchangeRel,
) -> Result<LogicalPlan> {
    let Some(input) = exchange.input.as_ref() else {
        return substrait_err!("Unexpected empty input in ExchangeRel");
    };
    let input = Arc::new(consumer.consume_rel(input).await?);

    let Some(exchange_kind) = &exchange.exchange_kind else {
        return substrait_err!("Unexpected empty input in ExchangeRel");
    };

    // ref: https://substrait.io/relations/physical_relations/#exchange-types
    let partitioning_scheme = match exchange_kind {
        ExchangeKind::ScatterByFields(scatter_fields) => {
            let mut partition_columns = vec![];
            let input_schema = input.schema();
            for field_ref in &scatter_fields.fields {
                let column = from_substrait_field_reference(field_ref, input_schema)?;
                partition_columns.push(column);
            }
            Partitioning::Hash(partition_columns, exchange.partition_count as usize)
        }
        ExchangeKind::RoundRobin(_) => {
            Partitioning::RoundRobinBatch(exchange.partition_count as usize)
        }
        ExchangeKind::SingleTarget(_)
        | ExchangeKind::MultiTarget(_)
        | ExchangeKind::Broadcast(_) => {
            return not_impl_err!("Unsupported exchange kind: {exchange_kind:?}");
        }
    };
    Ok(LogicalPlan::Repartition(Repartition {
        input,
        partitioning_scheme,
    }))
}

fn retrieve_rel_common(rel: &Rel) -> Option<&RelCommon> {
    match rel.rel_type.as_ref() {
        None => None,
        Some(rt) => match rt {
            RelType::Read(r) => r.common.as_ref(),
            RelType::Filter(f) => f.common.as_ref(),
            RelType::Fetch(f) => f.common.as_ref(),
            RelType::Aggregate(a) => a.common.as_ref(),
            RelType::Sort(s) => s.common.as_ref(),
            RelType::Join(j) => j.common.as_ref(),
            RelType::Project(p) => p.common.as_ref(),
            RelType::Set(s) => s.common.as_ref(),
            RelType::ExtensionSingle(e) => e.common.as_ref(),
            RelType::ExtensionMulti(e) => e.common.as_ref(),
            RelType::ExtensionLeaf(e) => e.common.as_ref(),
            RelType::Cross(c) => c.common.as_ref(),
            RelType::Reference(_) => None,
            RelType::Write(w) => w.common.as_ref(),
            RelType::Ddl(d) => d.common.as_ref(),
            RelType::HashJoin(j) => j.common.as_ref(),
            RelType::MergeJoin(j) => j.common.as_ref(),
            RelType::NestedLoopJoin(j) => j.common.as_ref(),
            RelType::Window(w) => w.common.as_ref(),
            RelType::Exchange(e) => e.common.as_ref(),
            RelType::Expand(e) => e.common.as_ref(),
            RelType::Update(_) => None,
        },
    }
}

fn retrieve_emit_kind(rel_common: Option<&RelCommon>) -> EmitKind {
    // the default EmitKind is Direct if it is not set explicitly
    let default = EmitKind::Direct(rel_common::Direct {});
    rel_common
        .and_then(|rc| rc.emit_kind.as_ref())
        .map_or(default, |ek| ek.clone())
}

fn contains_volatile_expr(proj: &Projection) -> bool {
    proj.expr.iter().any(|e| e.is_volatile())
}

fn apply_emit_kind(
    rel_common: Option<&RelCommon>,
    plan: LogicalPlan,
) -> Result<LogicalPlan> {
    match retrieve_emit_kind(rel_common) {
        EmitKind::Direct(_) => Ok(plan),
        EmitKind::Emit(Emit { output_mapping }) => {
            // It is valid to reference the same field multiple times in the Emit
            // In this case, we need to provide unique names to avoid collisions
            let mut name_tracker = NameTracker::new();
            match plan {
                // To avoid adding a projection on top of a projection, we apply special case
                // handling to flatten Substrait Emits. This is only applicable if none of the
                // expressions in the projection are volatile. This is to avoid issues like
                // converting a single call of the random() function into multiple calls due to
                // duplicate fields in the output_mapping.
                LogicalPlan::Projection(proj) if !contains_volatile_expr(&proj) => {
                    let mut exprs: Vec<Expr> = vec![];
                    for field in output_mapping {
                        let expr = proj.expr
                            .get(field as usize)
                            .ok_or_else(|| substrait_datafusion_err!(
                                  "Emit output field {} cannot be resolved in input schema {}",
                                  field, proj.input.schema()
                                ))?;
                        exprs.push(name_tracker.get_uniquely_named_expr(expr.clone())?);
                    }

                    let input = Arc::unwrap_or_clone(proj.input);
                    project(input, exprs)
                }
                // Otherwise we just handle the output_mapping as a projection
                _ => {
                    let input_schema = plan.schema();

                    let mut exprs: Vec<Expr> = vec![];
                    for index in output_mapping.into_iter() {
                        let column = Expr::Column(Column::from(
                            input_schema.qualified_field(index as usize),
                        ));
                        let expr = name_tracker.get_uniquely_named_expr(column)?;
                        exprs.push(expr);
                    }

                    project(plan, exprs)
                }
            }
        }
    }
}

struct NameTracker {
    seen_names: HashSet<String>,
}

enum NameTrackerStatus {
    NeverSeen,
    SeenBefore,
}

impl NameTracker {
    fn new() -> Self {
        NameTracker {
            seen_names: HashSet::default(),
        }
    }
    fn get_unique_name(&mut self, name: String) -> (String, NameTrackerStatus) {
        match self.seen_names.insert(name.clone()) {
            true => (name, NameTrackerStatus::NeverSeen),
            false => {
                let mut counter = 0;
                loop {
                    let candidate_name = format!("{}__temp__{}", name, counter);
                    if self.seen_names.insert(candidate_name.clone()) {
                        return (candidate_name, NameTrackerStatus::SeenBefore);
                    }
                    counter += 1;
                }
            }
        }
    }

    fn get_uniquely_named_expr(&mut self, expr: Expr) -> Result<Expr> {
        match self.get_unique_name(expr.name_for_alias()?) {
            (_, NameTrackerStatus::NeverSeen) => Ok(expr),
            (name, NameTrackerStatus::SeenBefore) => Ok(expr.alias(name)),
        }
    }
}

/// Ensures that the given Substrait schema is compatible with the schema as given by DataFusion
///
/// This means:
/// 1. All fields present in the Substrait schema are present in the DataFusion schema. The
///    DataFusion schema may have MORE fields, but not the other way around.
/// 2. All fields are compatible. See [`ensure_field_compatibility`] for details
fn ensure_schema_compatibility(
    table_schema: &DFSchema,
    substrait_schema: DFSchema,
) -> Result<()> {
    substrait_schema
        .strip_qualifiers()
        .fields()
        .iter()
        .try_for_each(|substrait_field| {
            let df_field =
                table_schema.field_with_unqualified_name(substrait_field.name())?;
            ensure_field_compatibility(df_field, substrait_field)
        })
}

/// This function returns a DataFrame with fields adjusted if necessary in the event that the
/// Substrait schema is a subset of the DataFusion schema.
fn apply_projection(
    plan: LogicalPlan,
    substrait_schema: DFSchema,
) -> Result<LogicalPlan> {
    let df_schema = plan.schema();

    if df_schema.logically_equivalent_names_and_types(&substrait_schema) {
        return Ok(plan);
    }

    let df_schema = df_schema.to_owned();

    match plan {
        LogicalPlan::TableScan(mut scan) => {
            let column_indices: Vec<usize> = substrait_schema
                .strip_qualifiers()
                .fields()
                .iter()
                .map(|substrait_field| {
                    Ok(df_schema
                        .index_of_column_by_name(None, substrait_field.name().as_str())
                        .unwrap())
                })
                .collect::<Result<_>>()?;

            let fields = column_indices
                .iter()
                .map(|i| df_schema.qualified_field(*i))
                .map(|(qualifier, field)| (qualifier.cloned(), Arc::new(field.clone())))
                .collect();

            scan.projected_schema = DFSchemaRef::new(DFSchema::new_with_metadata(
                fields,
                df_schema.metadata().clone(),
            )?);
            scan.projection = Some(column_indices);

            Ok(LogicalPlan::TableScan(scan))
        }
        _ => plan_err!("DataFrame passed to apply_projection must be a TableScan"),
    }
}

/// Ensures that the given Substrait field is compatible with the given DataFusion field
///
/// A field is compatible between Substrait and DataFusion if:
/// 1. They have logically equivalent types.
/// 2. They have the same nullability OR the Substrait field is nullable and the DataFusion fields
///    is not nullable.
///
/// If a Substrait field is not nullable, the Substrait plan may be built around assuming it is not
/// nullable. As such if DataFusion has that field as nullable the plan should be rejected.
fn ensure_field_compatibility(
    datafusion_field: &Field,
    substrait_field: &Field,
) -> Result<()> {
    if !DFSchema::datatype_is_logically_equal(
        datafusion_field.data_type(),
        substrait_field.data_type(),
    ) {
        return substrait_err!(
            "Field '{}' in Substrait schema has a different type ({}) than the corresponding field in the table schema ({}).",
            substrait_field.name(),
            substrait_field.data_type(),
            datafusion_field.data_type()
        );
    }

    if !compatible_nullabilities(
        datafusion_field.is_nullable(),
        substrait_field.is_nullable(),
    ) {
        // TODO: from_substrait_struct_type needs to be updated to set the nullability correctly. It defaults to true for now.
        return substrait_err!(
            "Field '{}' is nullable in the DataFusion schema but not nullable in the Substrait schema.",
            substrait_field.name()
        );
    }
    Ok(())
}

/// Returns true if the DataFusion and Substrait nullabilities are compatible, false otherwise
fn compatible_nullabilities(
    datafusion_nullability: bool,
    substrait_nullability: bool,
) -> bool {
    // DataFusion and Substrait have the same nullability
    (datafusion_nullability == substrait_nullability)
    // DataFusion is not nullable and Substrait is nullable
     || (!datafusion_nullability && substrait_nullability)
}

/// (Re)qualify the sides of a join if needed, i.e. if the columns from one side would otherwise
/// conflict with the columns from the other.
/// Substrait doesn't currently allow specifying aliases, neither for columns nor for tables. For
/// Substrait the names don't matter since it only refers to columns by indices, however DataFusion
/// requires columns to be uniquely identifiable, in some places (see e.g. DFSchema::check_names).
fn requalify_sides_if_needed(
    left: LogicalPlanBuilder,
    right: LogicalPlanBuilder,
) -> Result<(LogicalPlanBuilder, LogicalPlanBuilder)> {
    let left_cols = left.schema().columns();
    let right_cols = right.schema().columns();
    if left_cols.iter().any(|l| {
        right_cols.iter().any(|r| {
            l == r || (l.name == r.name && (l.relation.is_none() || r.relation.is_none()))
        })
    }) {
        // These names have no connection to the original plan, but they'll make the columns
        // (mostly) unique. There may be cases where this still causes duplicates, if either left
        // or right side itself contains duplicate names with different qualifiers.
        Ok((
            left.alias(TableReference::bare("left"))?,
            right.alias(TableReference::bare("right"))?,
        ))
    } else {
        Ok((left, right))
    }
}

fn from_substrait_jointype(join_type: i32) -> Result<JoinType> {
    if let Ok(substrait_join_type) = join_rel::JoinType::try_from(join_type) {
        match substrait_join_type {
            join_rel::JoinType::Inner => Ok(JoinType::Inner),
            join_rel::JoinType::Left => Ok(JoinType::Left),
            join_rel::JoinType::Right => Ok(JoinType::Right),
            join_rel::JoinType::Outer => Ok(JoinType::Full),
            join_rel::JoinType::LeftAnti => Ok(JoinType::LeftAnti),
            join_rel::JoinType::LeftSemi => Ok(JoinType::LeftSemi),
            join_rel::JoinType::LeftMark => Ok(JoinType::LeftMark),
            _ => plan_err!("unsupported join type {substrait_join_type:?}"),
        }
    } else {
        plan_err!("invalid join type variant {join_type:?}")
    }
}

/// Convert Substrait Sorts to DataFusion Exprs
pub async fn from_substrait_sorts(
    consumer: &impl SubstraitConsumer,
    substrait_sorts: &Vec<SortField>,
    input_schema: &DFSchema,
) -> Result<Vec<Sort>> {
    let mut sorts: Vec<Sort> = vec![];
    for s in substrait_sorts {
        let expr = consumer
            .consume_expression(s.expr.as_ref().unwrap(), input_schema)
            .await?;
        let asc_nullfirst = match &s.sort_kind {
            Some(k) => match k {
                Direction(d) => {
                    let Ok(direction) = SortDirection::try_from(*d) else {
                        return not_impl_err!(
                            "Unsupported Substrait SortDirection value {d}"
                        );
                    };

                    match direction {
                        SortDirection::AscNullsFirst => Ok((true, true)),
                        SortDirection::AscNullsLast => Ok((true, false)),
                        SortDirection::DescNullsFirst => Ok((false, true)),
                        SortDirection::DescNullsLast => Ok((false, false)),
                        SortDirection::Clustered => not_impl_err!(
                            "Sort with direction clustered is not yet supported"
                        ),
                        SortDirection::Unspecified => {
                            not_impl_err!("Unspecified sort direction is invalid")
                        }
                    }
                }
                ComparisonFunctionReference(_) => not_impl_err!(
                    "Sort using comparison function reference is not supported"
                ),
            },
            None => not_impl_err!("Sort without sort kind is invalid"),
        };
        let (asc, nulls_first) = asc_nullfirst.unwrap();
        sorts.push(Sort {
            expr,
            asc,
            nulls_first,
        });
    }
    Ok(sorts)
}

/// Convert Substrait Expressions to DataFusion Exprs
pub async fn from_substrait_rex_vec(
    consumer: &impl SubstraitConsumer,
    exprs: &Vec<Expression>,
    input_schema: &DFSchema,
) -> Result<Vec<Expr>> {
    let mut expressions: Vec<Expr> = vec![];
    for expr in exprs {
        let expression = consumer.consume_expression(expr, input_schema).await?;
        expressions.push(expression);
    }
    Ok(expressions)
}

/// Convert Substrait FunctionArguments to DataFusion Exprs
pub async fn from_substrait_func_args(
    consumer: &impl SubstraitConsumer,
    arguments: &Vec<FunctionArgument>,
    input_schema: &DFSchema,
) -> Result<Vec<Expr>> {
    let mut args: Vec<Expr> = vec![];
    for arg in arguments {
        let arg_expr = match &arg.arg_type {
            Some(ArgType::Value(e)) => consumer.consume_expression(e, input_schema).await,
            _ => not_impl_err!("Function argument non-Value type not supported"),
        };
        args.push(arg_expr?);
    }
    Ok(args)
}

/// Convert Substrait AggregateFunction to DataFusion Expr
pub async fn from_substrait_agg_func(
    consumer: &impl SubstraitConsumer,
    f: &AggregateFunction,
    input_schema: &DFSchema,
    filter: Option<Box<Expr>>,
    order_by: Option<Vec<SortExpr>>,
    distinct: bool,
) -> Result<Arc<Expr>> {
    let Some(fn_signature) = consumer
        .get_extensions()
        .functions
        .get(&f.function_reference)
    else {
        return plan_err!(
            "Aggregate function not registered: function anchor = {:?}",
            f.function_reference
        );
    };

    let fn_name = substrait_fun_name(fn_signature);
    let udaf = consumer.get_function_registry().udaf(fn_name);
    let udaf = udaf.map_err(|_| {
        not_impl_datafusion_err!(
            "Aggregate function {} is not supported: function anchor = {:?}",
            fn_signature,
            f.function_reference
        )
    })?;

    let args = from_substrait_func_args(consumer, &f.arguments, input_schema).await?;

    // Datafusion does not support aggregate functions with no arguments, so
    // we inject a dummy argument that does not affect the query, but allows
    // us to bypass this limitation.
    let args = if udaf.name() == "count" && args.is_empty() {
        vec![Expr::Literal(ScalarValue::Int64(Some(1)))]
    } else {
        args
    };

    Ok(Arc::new(Expr::AggregateFunction(
        expr::AggregateFunction::new_udf(udaf, args, distinct, filter, order_by, None),
    )))
}

/// Convert Substrait Rex to DataFusion Expr
pub async fn from_substrait_rex(
    consumer: &impl SubstraitConsumer,
    expression: &Expression,
    input_schema: &DFSchema,
) -> Result<Expr> {
    match &expression.rex_type {
        Some(t) => match t {
            RexType::Literal(expr) => consumer.consume_literal(expr).await,
            RexType::Selection(expr) => {
                consumer.consume_field_reference(expr, input_schema).await
            }
            RexType::ScalarFunction(expr) => {
                consumer.consume_scalar_function(expr, input_schema).await
            }
            RexType::WindowFunction(expr) => {
                consumer.consume_window_function(expr, input_schema).await
            }
            RexType::IfThen(expr) => consumer.consume_if_then(expr, input_schema).await,
            RexType::SwitchExpression(expr) => {
                consumer.consume_switch(expr, input_schema).await
            }
            RexType::SingularOrList(expr) => {
                consumer.consume_singular_or_list(expr, input_schema).await
            }

            RexType::MultiOrList(expr) => {
                consumer.consume_multi_or_list(expr, input_schema).await
            }

            RexType::Cast(expr) => {
                consumer.consume_cast(expr.as_ref(), input_schema).await
            }

            RexType::Subquery(expr) => {
                consumer.consume_subquery(expr.as_ref(), input_schema).await
            }
            RexType::Nested(expr) => consumer.consume_nested(expr, input_schema).await,
            RexType::Enum(expr) => consumer.consume_enum(expr, input_schema).await,
            RexType::DynamicParameter(expr) => {
                consumer.consume_dynamic_parameter(expr, input_schema).await
            }
        },
        None => substrait_err!("Expression must set rex_type: {:?}", expression),
    }
}

pub async fn from_singular_or_list(
    consumer: &impl SubstraitConsumer,
    expr: &SingularOrList,
    input_schema: &DFSchema,
) -> Result<Expr> {
    let substrait_expr = expr.value.as_ref().unwrap();
    let substrait_list = expr.options.as_ref();
    Ok(Expr::InList(InList {
        expr: Box::new(
            consumer
                .consume_expression(substrait_expr, input_schema)
                .await?,
        ),
        list: from_substrait_rex_vec(consumer, substrait_list, input_schema).await?,
        negated: false,
    }))
}

pub async fn from_field_reference(
    _consumer: &impl SubstraitConsumer,
    field_ref: &FieldReference,
    input_schema: &DFSchema,
) -> Result<Expr> {
    from_substrait_field_reference(field_ref, input_schema)
}

pub async fn from_if_then(
    consumer: &impl SubstraitConsumer,
    if_then: &IfThen,
    input_schema: &DFSchema,
) -> Result<Expr> {
    // Parse `ifs`
    // If the first element does not have a `then` part, then we can assume it's a base expression
    let mut when_then_expr: Vec<(Box<Expr>, Box<Expr>)> = vec![];
    let mut expr = None;
    for (i, if_expr) in if_then.ifs.iter().enumerate() {
        if i == 0 {
            // Check if the first element is type base expression
            if if_expr.then.is_none() {
                expr = Some(Box::new(
                    consumer
                        .consume_expression(if_expr.r#if.as_ref().unwrap(), input_schema)
                        .await?,
                ));
                continue;
            }
        }
        when_then_expr.push((
            Box::new(
                consumer
                    .consume_expression(if_expr.r#if.as_ref().unwrap(), input_schema)
                    .await?,
            ),
            Box::new(
                consumer
                    .consume_expression(if_expr.then.as_ref().unwrap(), input_schema)
                    .await?,
            ),
        ));
    }
    // Parse `else`
    let else_expr = match &if_then.r#else {
        Some(e) => Some(Box::new(
            consumer.consume_expression(e, input_schema).await?,
        )),
        None => None,
    };
    Ok(Expr::Case(Case {
        expr,
        when_then_expr,
        else_expr,
    }))
}

pub async fn from_scalar_function(
    consumer: &impl SubstraitConsumer,
    f: &ScalarFunction,
    input_schema: &DFSchema,
) -> Result<Expr> {
    let Some(fn_signature) = consumer
        .get_extensions()
        .functions
        .get(&f.function_reference)
    else {
        return plan_err!(
            "Scalar function not found: function reference = {:?}",
            f.function_reference
        );
    };
    let fn_name = substrait_fun_name(fn_signature);
    let args = from_substrait_func_args(consumer, &f.arguments, input_schema).await?;

    // try to first match the requested function into registered udfs, then built-in ops
    // and finally built-in expressions
    if let Ok(func) = consumer.get_function_registry().udf(fn_name) {
        Ok(Expr::ScalarFunction(expr::ScalarFunction::new_udf(
            func.to_owned(),
            args,
        )))
    } else if let Some(op) = name_to_op(fn_name) {
        if f.arguments.len() < 2 {
            return not_impl_err!(
                        "Expect at least two arguments for binary operator {op:?}, the provided number of operators is {:?}",
                       f.arguments.len()
                    );
        }
        // Some expressions are binary in DataFusion but take in a variadic number of args in Substrait.
        // In those cases we iterate through all the arguments, applying the binary expression against them all
        let combined_expr = args
            .into_iter()
            .fold(None, |combined_expr: Option<Expr>, arg: Expr| {
                Some(match combined_expr {
                    Some(expr) => Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(expr),
                        op,
                        right: Box::new(arg),
                    }),
                    None => arg,
                })
            })
            .unwrap();

        Ok(combined_expr)
    } else if let Some(builder) = BuiltinExprBuilder::try_from_name(fn_name) {
        builder.build(consumer, f, input_schema).await
    } else {
        not_impl_err!("Unsupported function name: {fn_name:?}")
    }
}

pub async fn from_literal(
    consumer: &impl SubstraitConsumer,
    expr: &Literal,
) -> Result<Expr> {
    let scalar_value = from_substrait_literal_without_names(consumer, expr)?;
    Ok(Expr::Literal(scalar_value))
}

pub async fn from_cast(
    consumer: &impl SubstraitConsumer,
    cast: &substrait_expression::Cast,
    input_schema: &DFSchema,
) -> Result<Expr> {
    match cast.r#type.as_ref() {
        Some(output_type) => {
            let input_expr = Box::new(
                consumer
                    .consume_expression(
                        cast.input.as_ref().unwrap().as_ref(),
                        input_schema,
                    )
                    .await?,
            );
            let data_type = from_substrait_type_without_names(consumer, output_type)?;
            if cast.failure_behavior() == ReturnNull {
                Ok(Expr::TryCast(TryCast::new(input_expr, data_type)))
            } else {
                Ok(Expr::Cast(Cast::new(input_expr, data_type)))
            }
        }
        None => substrait_err!("Cast expression without output type is not allowed"),
    }
}

pub async fn from_window_function(
    consumer: &impl SubstraitConsumer,
    window: &WindowFunction,
    input_schema: &DFSchema,
) -> Result<Expr> {
    let Some(fn_signature) = consumer
        .get_extensions()
        .functions
        .get(&window.function_reference)
    else {
        return plan_err!(
            "Window function not found: function reference = {:?}",
            window.function_reference
        );
    };
    let fn_name = substrait_fun_name(fn_signature);

    // check udwf first, then udaf, then built-in window and aggregate functions
    let fun = if let Ok(udwf) = consumer.get_function_registry().udwf(fn_name) {
        Ok(WindowFunctionDefinition::WindowUDF(udwf))
    } else if let Ok(udaf) = consumer.get_function_registry().udaf(fn_name) {
        Ok(WindowFunctionDefinition::AggregateUDF(udaf))
    } else {
        not_impl_err!(
            "Window function {} is not supported: function anchor = {:?}",
            fn_name,
            window.function_reference
        )
    }?;

    let mut order_by =
        from_substrait_sorts(consumer, &window.sorts, input_schema).await?;

    let bound_units = match BoundsType::try_from(window.bounds_type).map_err(|e| {
        plan_datafusion_err!("Invalid bound type {}: {e}", window.bounds_type)
    })? {
        BoundsType::Rows => WindowFrameUnits::Rows,
        BoundsType::Range => WindowFrameUnits::Range,
        BoundsType::Unspecified => {
            // If the plan does not specify the bounds type, then we use a simple logic to determine the units
            // If there is no `ORDER BY`, then by default, the frame counts each row from the lower up to upper boundary
            // If there is `ORDER BY`, then by default, each frame is a range starting from unbounded preceding to current row
            if order_by.is_empty() {
                WindowFrameUnits::Rows
            } else {
                WindowFrameUnits::Range
            }
        }
    };
    let window_frame = datafusion::logical_expr::WindowFrame::new_bounds(
        bound_units,
        from_substrait_bound(&window.lower_bound, true)?,
        from_substrait_bound(&window.upper_bound, false)?,
    );

    window_frame.regularize_order_bys(&mut order_by)?;

    // Datafusion does not support aggregate functions with no arguments, so
    // we inject a dummy argument that does not affect the query, but allows
    // us to bypass this limitation.
    let args = if fun.name() == "count" && window.arguments.is_empty() {
        vec![Expr::Literal(ScalarValue::Int64(Some(1)))]
    } else {
        from_substrait_func_args(consumer, &window.arguments, input_schema).await?
    };

    Ok(Expr::WindowFunction(expr::WindowFunction {
        fun,
        params: WindowFunctionParams {
            args,
            partition_by: from_substrait_rex_vec(
                consumer,
                &window.partitions,
                input_schema,
            )
            .await?,
            order_by,
            window_frame,
            null_treatment: None,
        },
    }))
}

pub async fn from_subquery(
    consumer: &impl SubstraitConsumer,
    subquery: &substrait_expression::Subquery,
    input_schema: &DFSchema,
) -> Result<Expr> {
    match &subquery.subquery_type {
        Some(subquery_type) => match subquery_type {
            SubqueryType::InPredicate(in_predicate) => {
                if in_predicate.needles.len() != 1 {
                    substrait_err!("InPredicate Subquery type must have exactly one Needle expression")
                } else {
                    let needle_expr = &in_predicate.needles[0];
                    let haystack_expr = &in_predicate.haystack;
                    if let Some(haystack_expr) = haystack_expr {
                        let haystack_expr = consumer.consume_rel(haystack_expr).await?;
                        let outer_refs = haystack_expr.all_out_ref_exprs();
                        Ok(Expr::InSubquery(InSubquery {
                            expr: Box::new(
                                consumer
                                    .consume_expression(needle_expr, input_schema)
                                    .await?,
                            ),
                            subquery: Subquery {
                                subquery: Arc::new(haystack_expr),
                                outer_ref_columns: outer_refs,
                                spans: Spans::new(),
                            },
                            negated: false,
                        }))
                    } else {
                        substrait_err!(
                            "InPredicate Subquery type must have a Haystack expression"
                        )
                    }
                }
            }
            SubqueryType::Scalar(query) => {
                let plan = consumer
                    .consume_rel(&(query.input.clone()).unwrap_or_default())
                    .await?;
                let outer_ref_columns = plan.all_out_ref_exprs();
                Ok(Expr::ScalarSubquery(Subquery {
                    subquery: Arc::new(plan),
                    outer_ref_columns,
                    spans: Spans::new(),
                }))
            }
            SubqueryType::SetPredicate(predicate) => {
                match predicate.predicate_op() {
                    // exist
                    PredicateOp::Exists => {
                        let relation = &predicate.tuples;
                        let plan = consumer
                            .consume_rel(&relation.clone().unwrap_or_default())
                            .await?;
                        let outer_ref_columns = plan.all_out_ref_exprs();
                        Ok(Expr::Exists(Exists::new(
                            Subquery {
                                subquery: Arc::new(plan),
                                outer_ref_columns,
                                spans: Spans::new(),
                            },
                            false,
                        )))
                    }
                    other_type => substrait_err!(
                        "unimplemented type {:?} for set predicate",
                        other_type
                    ),
                }
            }
            other_type => {
                substrait_err!("Subquery type {:?} not implemented", other_type)
            }
        },
        None => {
            substrait_err!("Subquery expression without SubqueryType is not allowed")
        }
    }
}

pub(crate) fn from_substrait_type_without_names(
    consumer: &impl SubstraitConsumer,
    dt: &Type,
) -> Result<DataType> {
    from_substrait_type(consumer, dt, &[], &mut 0)
}

fn from_substrait_type(
    consumer: &impl SubstraitConsumer,
    dt: &Type,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> Result<DataType> {
    match &dt.kind {
        Some(s_kind) => match s_kind {
            r#type::Kind::Bool(_) => Ok(DataType::Boolean),
            r#type::Kind::I8(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int8),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt8),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::I16(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int16),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt16),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::I32(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int32),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt32),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::I64(integer) => match integer.type_variation_reference {
                DEFAULT_TYPE_VARIATION_REF => Ok(DataType::Int64),
                UNSIGNED_INTEGER_TYPE_VARIATION_REF => Ok(DataType::UInt64),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::Fp32(_) => Ok(DataType::Float32),
            r#type::Kind::Fp64(_) => Ok(DataType::Float64),
            r#type::Kind::Timestamp(ts) => {
                // Kept for backwards compatibility, new plans should use PrecisionTimestamp(Tz) instead
                #[allow(deprecated)]
                match ts.type_variation_reference {
                    TIMESTAMP_SECOND_TYPE_VARIATION_REF => {
                        Ok(DataType::Timestamp(TimeUnit::Second, None))
                    }
                    TIMESTAMP_MILLI_TYPE_VARIATION_REF => {
                        Ok(DataType::Timestamp(TimeUnit::Millisecond, None))
                    }
                    TIMESTAMP_MICRO_TYPE_VARIATION_REF => {
                        Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
                    }
                    TIMESTAMP_NANO_TYPE_VARIATION_REF => {
                        Ok(DataType::Timestamp(TimeUnit::Nanosecond, None))
                    }
                    v => not_impl_err!(
                        "Unsupported Substrait type variation {v} of type {s_kind:?}"
                    ),
                }
            }
            r#type::Kind::PrecisionTimestamp(pts) => {
                let unit = match pts.precision {
                    0 => Ok(TimeUnit::Second),
                    3 => Ok(TimeUnit::Millisecond),
                    6 => Ok(TimeUnit::Microsecond),
                    9 => Ok(TimeUnit::Nanosecond),
                    p => not_impl_err!(
                        "Unsupported Substrait precision {p} for PrecisionTimestamp"
                    ),
                }?;
                Ok(DataType::Timestamp(unit, None))
            }
            r#type::Kind::PrecisionTimestampTz(pts) => {
                let unit = match pts.precision {
                    0 => Ok(TimeUnit::Second),
                    3 => Ok(TimeUnit::Millisecond),
                    6 => Ok(TimeUnit::Microsecond),
                    9 => Ok(TimeUnit::Nanosecond),
                    p => not_impl_err!(
                        "Unsupported Substrait precision {p} for PrecisionTimestampTz"
                    ),
                }?;
                Ok(DataType::Timestamp(unit, Some(DEFAULT_TIMEZONE.into())))
            }
            r#type::Kind::Date(date) => match date.type_variation_reference {
                DATE_32_TYPE_VARIATION_REF => Ok(DataType::Date32),
                DATE_64_TYPE_VARIATION_REF => Ok(DataType::Date64),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::Binary(binary) => match binary.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Binary),
                LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::LargeBinary),
                VIEW_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::BinaryView),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::FixedBinary(fixed) => {
                Ok(DataType::FixedSizeBinary(fixed.length))
            }
            r#type::Kind::String(string) => match string.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Utf8),
                LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::LargeUtf8),
                VIEW_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::Utf8View),
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::List(list) => {
                let inner_type = list.r#type.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("List type must have inner type")
                })?;
                let field = Arc::new(Field::new_list_field(
                    from_substrait_type(consumer, inner_type, dfs_names, name_idx)?,
                    // We ignore Substrait's nullability here to match to_substrait_literal
                    // which always creates nullable lists
                    true,
                ));
                match list.type_variation_reference {
                    DEFAULT_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::List(field)),
                    LARGE_CONTAINER_TYPE_VARIATION_REF => Ok(DataType::LargeList(field)),
                    v => not_impl_err!(
                        "Unsupported Substrait type variation {v} of type {s_kind:?}"
                    )?,
                }
            }
            r#type::Kind::Map(map) => {
                let key_type = map.key.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("Map type must have key type")
                })?;
                let value_type = map.value.as_ref().ok_or_else(|| {
                    substrait_datafusion_err!("Map type must have value type")
                })?;
                let key_field = Arc::new(Field::new(
                    "key",
                    from_substrait_type(consumer, key_type, dfs_names, name_idx)?,
                    false,
                ));
                let value_field = Arc::new(Field::new(
                    "value",
                    from_substrait_type(consumer, value_type, dfs_names, name_idx)?,
                    true,
                ));
                Ok(DataType::Map(
                    Arc::new(Field::new_struct(
                        "entries",
                        [key_field, value_field],
                        false, // The inner map field is always non-nullable (Arrow #1697),
                    )),
                    false, // whether keys are sorted
                ))
            }
            r#type::Kind::Decimal(d) => match d.type_variation_reference {
                DECIMAL_128_TYPE_VARIATION_REF => {
                    Ok(DataType::Decimal128(d.precision as u8, d.scale as i8))
                }
                DECIMAL_256_TYPE_VARIATION_REF => {
                    Ok(DataType::Decimal256(d.precision as u8, d.scale as i8))
                }
                v => not_impl_err!(
                    "Unsupported Substrait type variation {v} of type {s_kind:?}"
                ),
            },
            r#type::Kind::IntervalYear(_) => {
                Ok(DataType::Interval(IntervalUnit::YearMonth))
            }
            r#type::Kind::IntervalDay(_) => Ok(DataType::Interval(IntervalUnit::DayTime)),
            r#type::Kind::IntervalCompound(_) => {
                Ok(DataType::Interval(IntervalUnit::MonthDayNano))
            }
            r#type::Kind::UserDefined(u) => {
                if let Ok(data_type) = consumer.consume_user_defined_type(u) {
                    return Ok(data_type);
                }

                // TODO: remove the code below once the producer has been updated
                if let Some(name) = consumer.get_extensions().types.get(&u.type_reference)
                {
                    #[allow(deprecated)]
                        match name.as_ref() {
                            // Kept for backwards compatibility, producers should use IntervalCompound instead
                            INTERVAL_MONTH_DAY_NANO_TYPE_NAME => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),
                            _ => not_impl_err!(
                                "Unsupported Substrait user defined type with ref {} and variation {}",
                                u.type_reference,
                                u.type_variation_reference
                            ),
                        }
                } else {
                    #[allow(deprecated)]
                        match u.type_reference {
                            // Kept for backwards compatibility, producers should use IntervalYear instead
                            INTERVAL_YEAR_MONTH_TYPE_REF => {
                                Ok(DataType::Interval(IntervalUnit::YearMonth))
                            }
                            // Kept for backwards compatibility, producers should use IntervalDay instead
                            INTERVAL_DAY_TIME_TYPE_REF => {
                                Ok(DataType::Interval(IntervalUnit::DayTime))
                            }
                            // Kept for backwards compatibility, producers should use IntervalCompound instead
                            INTERVAL_MONTH_DAY_NANO_TYPE_REF => {
                                Ok(DataType::Interval(IntervalUnit::MonthDayNano))
                            }
                            _ => not_impl_err!(
                        "Unsupported Substrait user defined type with ref {} and variation {}",
                        u.type_reference,
                        u.type_variation_reference
                    ),
                        }
                }
            }
            r#type::Kind::Struct(s) => Ok(DataType::Struct(from_substrait_struct_type(
                consumer, s, dfs_names, name_idx,
            )?)),
            r#type::Kind::Varchar(_) => Ok(DataType::Utf8),
            r#type::Kind::FixedChar(_) => Ok(DataType::Utf8),
            _ => not_impl_err!("Unsupported Substrait type: {s_kind:?}"),
        },
        _ => not_impl_err!("`None` Substrait kind is not supported"),
    }
}

fn from_substrait_struct_type(
    consumer: &impl SubstraitConsumer,
    s: &r#type::Struct,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> Result<Fields> {
    let mut fields = vec![];
    for (i, f) in s.types.iter().enumerate() {
        let field = Field::new(
            next_struct_field_name(i, dfs_names, name_idx)?,
            from_substrait_type(consumer, f, dfs_names, name_idx)?,
            true, // We assume everything to be nullable since that's easier than ensuring it matches
        );
        fields.push(field);
    }
    Ok(fields.into())
}

fn next_struct_field_name(
    column_idx: usize,
    dfs_names: &[String],
    name_idx: &mut usize,
) -> Result<String> {
    if dfs_names.is_empty() {
        // If names are not given, create dummy names
        // c0, c1, ... align with e.g. SqlToRel::create_named_struct
        Ok(format!("c{column_idx}"))
    } else {
        let name = dfs_names.get(*name_idx).cloned().ok_or_else(|| {
            substrait_datafusion_err!("Named schema must contain names for all fields")
        })?;
        *name_idx += 1;
        Ok(name)
    }
}

/// Convert Substrait NamedStruct to DataFusion DFSchemaRef
pub fn from_substrait_named_struct(
    consumer: &impl SubstraitConsumer,
    base_schema: &NamedStruct,
) -> Result<DFSchema> {
    let mut name_idx = 0;
    let fields = from_substrait_struct_type(
        consumer,
        base_schema.r#struct.as_ref().ok_or_else(|| {
            substrait_datafusion_err!("Named struct must contain a struct")
        })?,
        &base_schema.names,
        &mut name_idx,
    );
    if name_idx != base_schema.names.len() {
        return substrait_err!(
            "Names list must match exactly to nested schema, but found {} uses for {} names",
            name_idx,
            base_schema.names.len()
        );
    }
    DFSchema::try_from(Schema::new(fields?))
}

fn from_substrait_bound(
    bound: &Option<Bound>,
    is_lower: bool,
) -> Result<WindowFrameBound> {
    match bound {
        Some(b) => match &b.kind {
            Some(k) => match k {
                BoundKind::CurrentRow(SubstraitBound::CurrentRow {}) => {
                    Ok(WindowFrameBound::CurrentRow)
                }
                BoundKind::Preceding(SubstraitBound::Preceding { offset }) => {
                    if *offset <= 0 {
                        return plan_err!("Preceding bound must be positive");
                    }
                    Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(Some(
                        *offset as u64,
                    ))))
                }
                BoundKind::Following(SubstraitBound::Following { offset }) => {
                    if *offset <= 0 {
                        return plan_err!("Following bound must be positive");
                    }
                    Ok(WindowFrameBound::Following(ScalarValue::UInt64(Some(
                        *offset as u64,
                    ))))
                }
                BoundKind::Unbounded(SubstraitBound::Unbounded {}) => {
                    if is_lower {
                        Ok(WindowFrameBound::Preceding(ScalarValue::Null))
                    } else {
                        Ok(WindowFrameBound::Following(ScalarValue::Null))
                    }
                }
            },
            None => substrait_err!("WindowFunction missing Substrait Bound kind"),
        },
        None => {
            if is_lower {
                Ok(WindowFrameBound::Preceding(ScalarValue::Null))
            } else {
                Ok(WindowFrameBound::Following(ScalarValue::Null))
            }
        }
    }
}

pub(crate) fn from_substrait_literal_without_names(
    consumer: &impl SubstraitConsumer,
    lit: &Literal,
) -> Result<ScalarValue> {
    from_substrait_literal(consumer, lit, &vec![], &mut 0)
}

fn from_substrait_literal(
    consumer: &impl SubstraitConsumer,
    lit: &Literal,
    dfs_names: &Vec<String>,
    name_idx: &mut usize,
) -> Result<ScalarValue> {
    let scalar_value = match &lit.literal_type {
        Some(LiteralType::Boolean(b)) => ScalarValue::Boolean(Some(*b)),
        Some(LiteralType::I8(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int8(Some(*n as i8)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt8(Some(*n as u8)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I16(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int16(Some(*n as i16)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt16(Some(*n as u16)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I32(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int32(Some(*n)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt32(Some(*n as u32)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::I64(n)) => match lit.type_variation_reference {
            DEFAULT_TYPE_VARIATION_REF => ScalarValue::Int64(Some(*n)),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF => ScalarValue::UInt64(Some(*n as u64)),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::Fp32(f)) => ScalarValue::Float32(Some(*f)),
        Some(LiteralType::Fp64(f)) => ScalarValue::Float64(Some(*f)),
        Some(LiteralType::Timestamp(t)) => {
            // Kept for backwards compatibility, new plans should use PrecisionTimestamp(Tz) instead
            #[allow(deprecated)]
            match lit.type_variation_reference {
                TIMESTAMP_SECOND_TYPE_VARIATION_REF => {
                    ScalarValue::TimestampSecond(Some(*t), None)
                }
                TIMESTAMP_MILLI_TYPE_VARIATION_REF => {
                    ScalarValue::TimestampMillisecond(Some(*t), None)
                }
                TIMESTAMP_MICRO_TYPE_VARIATION_REF => {
                    ScalarValue::TimestampMicrosecond(Some(*t), None)
                }
                TIMESTAMP_NANO_TYPE_VARIATION_REF => {
                    ScalarValue::TimestampNanosecond(Some(*t), None)
                }
                others => {
                    return substrait_err!("Unknown type variation reference {others}");
                }
            }
        }
        Some(LiteralType::PrecisionTimestamp(pt)) => match pt.precision {
            0 => ScalarValue::TimestampSecond(Some(pt.value), None),
            3 => ScalarValue::TimestampMillisecond(Some(pt.value), None),
            6 => ScalarValue::TimestampMicrosecond(Some(pt.value), None),
            9 => ScalarValue::TimestampNanosecond(Some(pt.value), None),
            p => {
                return not_impl_err!(
                    "Unsupported Substrait precision {p} for PrecisionTimestamp"
                );
            }
        },
        Some(LiteralType::PrecisionTimestampTz(pt)) => match pt.precision {
            0 => ScalarValue::TimestampSecond(
                Some(pt.value),
                Some(DEFAULT_TIMEZONE.into()),
            ),
            3 => ScalarValue::TimestampMillisecond(
                Some(pt.value),
                Some(DEFAULT_TIMEZONE.into()),
            ),
            6 => ScalarValue::TimestampMicrosecond(
                Some(pt.value),
                Some(DEFAULT_TIMEZONE.into()),
            ),
            9 => ScalarValue::TimestampNanosecond(
                Some(pt.value),
                Some(DEFAULT_TIMEZONE.into()),
            ),
            p => {
                return not_impl_err!(
                    "Unsupported Substrait precision {p} for PrecisionTimestamp"
                );
            }
        },
        Some(LiteralType::Date(d)) => ScalarValue::Date32(Some(*d)),
        Some(LiteralType::String(s)) => match lit.type_variation_reference {
            DEFAULT_CONTAINER_TYPE_VARIATION_REF => ScalarValue::Utf8(Some(s.clone())),
            LARGE_CONTAINER_TYPE_VARIATION_REF => ScalarValue::LargeUtf8(Some(s.clone())),
            VIEW_CONTAINER_TYPE_VARIATION_REF => ScalarValue::Utf8View(Some(s.clone())),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::Binary(b)) => match lit.type_variation_reference {
            DEFAULT_CONTAINER_TYPE_VARIATION_REF => ScalarValue::Binary(Some(b.clone())),
            LARGE_CONTAINER_TYPE_VARIATION_REF => {
                ScalarValue::LargeBinary(Some(b.clone()))
            }
            VIEW_CONTAINER_TYPE_VARIATION_REF => ScalarValue::BinaryView(Some(b.clone())),
            others => {
                return substrait_err!("Unknown type variation reference {others}");
            }
        },
        Some(LiteralType::FixedBinary(b)) => {
            ScalarValue::FixedSizeBinary(b.len() as _, Some(b.clone()))
        }
        Some(LiteralType::Decimal(d)) => {
            let value: [u8; 16] = d
                .value
                .clone()
                .try_into()
                .or(substrait_err!("Failed to parse decimal value"))?;
            let p = d.precision.try_into().map_err(|e| {
                substrait_datafusion_err!("Failed to parse decimal precision: {e}")
            })?;
            let s = d.scale.try_into().map_err(|e| {
                substrait_datafusion_err!("Failed to parse decimal scale: {e}")
            })?;
            ScalarValue::Decimal128(Some(i128::from_le_bytes(value)), p, s)
        }
        Some(LiteralType::List(l)) => {
            // Each element should start the name index from the same value, then we increase it
            // once at the end
            let mut element_name_idx = *name_idx;
            let elements = l
                .values
                .iter()
                .map(|el| {
                    element_name_idx = *name_idx;
                    from_substrait_literal(consumer, el, dfs_names, &mut element_name_idx)
                })
                .collect::<Result<Vec<_>>>()?;
            *name_idx = element_name_idx;
            if elements.is_empty() {
                return substrait_err!(
                    "Empty list must be encoded as EmptyList literal type, not List"
                );
            }
            let element_type = elements[0].data_type();
            match lit.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => ScalarValue::List(
                    ScalarValue::new_list_nullable(elements.as_slice(), &element_type),
                ),
                LARGE_CONTAINER_TYPE_VARIATION_REF => ScalarValue::LargeList(
                    ScalarValue::new_large_list(elements.as_slice(), &element_type),
                ),
                others => {
                    return substrait_err!("Unknown type variation reference {others}");
                }
            }
        }
        Some(LiteralType::EmptyList(l)) => {
            let element_type = from_substrait_type(
                consumer,
                l.r#type.clone().unwrap().as_ref(),
                dfs_names,
                name_idx,
            )?;
            match lit.type_variation_reference {
                DEFAULT_CONTAINER_TYPE_VARIATION_REF => {
                    ScalarValue::List(ScalarValue::new_list_nullable(&[], &element_type))
                }
                LARGE_CONTAINER_TYPE_VARIATION_REF => ScalarValue::LargeList(
                    ScalarValue::new_large_list(&[], &element_type),
                ),
                others => {
                    return substrait_err!("Unknown type variation reference {others}");
                }
            }
        }
        Some(LiteralType::Map(m)) => {
            // Each entry should start the name index from the same value, then we increase it
            // once at the end
            let mut entry_name_idx = *name_idx;
            let entries = m
                .key_values
                .iter()
                .map(|kv| {
                    entry_name_idx = *name_idx;
                    let key_sv = from_substrait_literal(
                        consumer,
                        kv.key.as_ref().unwrap(),
                        dfs_names,
                        &mut entry_name_idx,
                    )?;
                    let value_sv = from_substrait_literal(
                        consumer,
                        kv.value.as_ref().unwrap(),
                        dfs_names,
                        &mut entry_name_idx,
                    )?;
                    ScalarStructBuilder::new()
                        .with_scalar(Field::new("key", key_sv.data_type(), false), key_sv)
                        .with_scalar(
                            Field::new("value", value_sv.data_type(), true),
                            value_sv,
                        )
                        .build()
                })
                .collect::<Result<Vec<_>>>()?;
            *name_idx = entry_name_idx;

            if entries.is_empty() {
                return substrait_err!(
                    "Empty map must be encoded as EmptyMap literal type, not Map"
                );
            }

            ScalarValue::Map(Arc::new(MapArray::new(
                Arc::new(Field::new("entries", entries[0].data_type(), false)),
                OffsetBuffer::new(vec![0, entries.len() as i32].into()),
                ScalarValue::iter_to_array(entries)?.as_struct().to_owned(),
                None,
                false,
            )))
        }
        Some(LiteralType::EmptyMap(m)) => {
            let key = match &m.key {
                Some(k) => Ok(k),
                _ => plan_err!("Missing key type for empty map"),
            }?;
            let value = match &m.value {
                Some(v) => Ok(v),
                _ => plan_err!("Missing value type for empty map"),
            }?;
            let key_type = from_substrait_type(consumer, key, dfs_names, name_idx)?;
            let value_type = from_substrait_type(consumer, value, dfs_names, name_idx)?;

            // new_empty_array on a MapType creates a too empty array
            // We want it to contain an empty struct array to align with an empty MapBuilder one
            let entries = Field::new_struct(
                "entries",
                vec![
                    Field::new("key", key_type, false),
                    Field::new("value", value_type, true),
                ],
                false,
            );
            let struct_array =
                new_empty_array(entries.data_type()).as_struct().to_owned();
            ScalarValue::Map(Arc::new(MapArray::new(
                Arc::new(entries),
                OffsetBuffer::new(vec![0, 0].into()),
                struct_array,
                None,
                false,
            )))
        }
        Some(LiteralType::Struct(s)) => {
            let mut builder = ScalarStructBuilder::new();
            for (i, field) in s.fields.iter().enumerate() {
                let name = next_struct_field_name(i, dfs_names, name_idx)?;
                let sv = from_substrait_literal(consumer, field, dfs_names, name_idx)?;
                // We assume everything to be nullable, since Arrow's strict about things matching
                // and it's hard to match otherwise.
                builder = builder.with_scalar(Field::new(name, sv.data_type(), true), sv);
            }
            builder.build()?
        }
        Some(LiteralType::Null(null_type)) => {
            let data_type =
                from_substrait_type(consumer, null_type, dfs_names, name_idx)?;
            ScalarValue::try_from(&data_type)?
        }
        Some(LiteralType::IntervalDayToSecond(IntervalDayToSecond {
            days,
            seconds,
            subseconds,
            precision_mode,
        })) => {
            use interval_day_to_second::PrecisionMode;
            // DF only supports millisecond precision, so for any more granular type we lose precision
            let milliseconds = match precision_mode {
                Some(PrecisionMode::Microseconds(ms)) => ms / 1000,
                None =>
                    if *subseconds != 0 {
                        return substrait_err!("Cannot set subseconds field of IntervalDayToSecond without setting precision");
                    } else {
                        0_i32
                    }
                Some(PrecisionMode::Precision(0)) => *subseconds as i32 * 1000,
                Some(PrecisionMode::Precision(3)) => *subseconds as i32,
                Some(PrecisionMode::Precision(6)) => (subseconds / 1000) as i32,
                Some(PrecisionMode::Precision(9)) => (subseconds / 1000 / 1000) as i32,
                _ => {
                    return not_impl_err!(
                    "Unsupported Substrait interval day to second precision mode: {precision_mode:?}")
                }
            };

            ScalarValue::new_interval_dt(*days, (seconds * 1000) + milliseconds)
        }
        Some(LiteralType::IntervalYearToMonth(IntervalYearToMonth { years, months })) => {
            ScalarValue::new_interval_ym(*years, *months)
        }
        Some(LiteralType::IntervalCompound(IntervalCompound {
            interval_year_to_month,
            interval_day_to_second,
        })) => match (interval_year_to_month, interval_day_to_second) {
            (
                Some(IntervalYearToMonth { years, months }),
                Some(IntervalDayToSecond {
                    days,
                    seconds,
                    subseconds,
                    precision_mode:
                        Some(interval_day_to_second::PrecisionMode::Precision(p)),
                }),
            ) => {
                if *p < 0 || *p > 9 {
                    return plan_err!(
                        "Unsupported Substrait interval day to second precision: {}",
                        p
                    );
                }
                let nanos = *subseconds * i64::pow(10, (9 - p) as u32);
                ScalarValue::new_interval_mdn(
                    *years * 12 + months,
                    *days,
                    *seconds as i64 * NANOSECONDS + nanos,
                )
            }
            _ => return plan_err!("Substrait compound interval missing components"),
        },
        Some(LiteralType::FixedChar(c)) => ScalarValue::Utf8(Some(c.clone())),
        Some(LiteralType::UserDefined(user_defined)) => {
            if let Ok(value) = consumer.consume_user_defined_literal(user_defined) {
                return Ok(value);
            }

            // TODO: remove the code below once the producer has been updated

            // Helper function to prevent duplicating this code - can be inlined once the non-extension path is removed
            let interval_month_day_nano =
                |user_defined: &proto::expression::literal::UserDefined| -> Result<ScalarValue> {
                    let Some(Val::Value(raw_val)) = user_defined.val.as_ref() else {
                        return substrait_err!("Interval month day nano value is empty");
                    };
                    let value_slice: [u8; 16] =
                        (*raw_val.value).try_into().map_err(|_| {
                            substrait_datafusion_err!(
                                "Failed to parse interval month day nano value"
                            )
                        })?;
                    let months =
                        i32::from_le_bytes(value_slice[0..4].try_into().unwrap());
                    let days = i32::from_le_bytes(value_slice[4..8].try_into().unwrap());
                    let nanoseconds =
                        i64::from_le_bytes(value_slice[8..16].try_into().unwrap());
                    Ok(ScalarValue::IntervalMonthDayNano(Some(
                        IntervalMonthDayNano {
                            months,
                            days,
                            nanoseconds,
                        },
                    )))
                };

            if let Some(name) = consumer
                .get_extensions()
                .types
                .get(&user_defined.type_reference)
            {
                match name.as_ref() {
                    // Kept for backwards compatibility - producers should use IntervalCompound instead
                    #[allow(deprecated)]
                    INTERVAL_MONTH_DAY_NANO_TYPE_NAME => {
                        interval_month_day_nano(user_defined)?
                    }
                    _ => {
                        return not_impl_err!(
                        "Unsupported Substrait user defined type with ref {} and name {}",
                        user_defined.type_reference,
                        name
                    )
                    }
                }
            } else {
                #[allow(deprecated)]
                match user_defined.type_reference {
                    // Kept for backwards compatibility, producers should useIntervalYearToMonth instead
                    INTERVAL_YEAR_MONTH_TYPE_REF => {
                        let Some(Val::Value(raw_val)) = user_defined.val.as_ref() else {
                            return substrait_err!("Interval year month value is empty");
                        };
                        let value_slice: [u8; 4] =
                            (*raw_val.value).try_into().map_err(|_| {
                                substrait_datafusion_err!(
                                    "Failed to parse interval year month value"
                                )
                            })?;
                        ScalarValue::IntervalYearMonth(Some(i32::from_le_bytes(
                            value_slice,
                        )))
                    }
                    // Kept for backwards compatibility, producers should useIntervalDayToSecond instead
                    INTERVAL_DAY_TIME_TYPE_REF => {
                        let Some(Val::Value(raw_val)) = user_defined.val.as_ref() else {
                            return substrait_err!("Interval day time value is empty");
                        };
                        let value_slice: [u8; 8] =
                            (*raw_val.value).try_into().map_err(|_| {
                                substrait_datafusion_err!(
                                    "Failed to parse interval day time value"
                                )
                            })?;
                        let days =
                            i32::from_le_bytes(value_slice[0..4].try_into().unwrap());
                        let milliseconds =
                            i32::from_le_bytes(value_slice[4..8].try_into().unwrap());
                        ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                            days,
                            milliseconds,
                        }))
                    }
                    // Kept for backwards compatibility, producers should useIntervalCompound instead
                    INTERVAL_MONTH_DAY_NANO_TYPE_REF => {
                        interval_month_day_nano(user_defined)?
                    }
                    _ => {
                        return not_impl_err!(
                            "Unsupported Substrait user defined type literal with ref {}",
                            user_defined.type_reference
                        )
                    }
                }
            }
        }
        _ => return not_impl_err!("Unsupported literal_type: {:?}", lit.literal_type),
    };

    Ok(scalar_value)
}

#[allow(deprecated)]
async fn from_substrait_grouping(
    consumer: &impl SubstraitConsumer,
    grouping: &Grouping,
    expressions: &[Expr],
    input_schema: &DFSchemaRef,
) -> Result<Vec<Expr>> {
    let mut group_exprs = vec![];
    if !grouping.grouping_expressions.is_empty() {
        for e in &grouping.grouping_expressions {
            let expr = consumer.consume_expression(e, input_schema).await?;
            group_exprs.push(expr);
        }
        return Ok(group_exprs);
    }
    for idx in &grouping.expression_references {
        let e = &expressions[*idx as usize];
        group_exprs.push(e.clone());
    }
    Ok(group_exprs)
}

fn from_substrait_field_reference(
    field_ref: &FieldReference,
    input_schema: &DFSchema,
) -> Result<Expr> {
    match &field_ref.reference_type {
        Some(DirectReference(direct)) => match &direct.reference_type.as_ref() {
            Some(StructField(x)) => match &x.child.as_ref() {
                Some(_) => not_impl_err!(
                    "Direct reference StructField with child is not supported"
                ),
                None => Ok(Expr::Column(Column::from(
                    input_schema.qualified_field(x.field as usize),
                ))),
            },
            _ => not_impl_err!(
                "Direct reference with types other than StructField is not supported"
            ),
        },
        _ => not_impl_err!("unsupported field ref type"),
    }
}

/// Build [`Expr`] from its name and required inputs.
struct BuiltinExprBuilder {
    expr_name: String,
}

impl BuiltinExprBuilder {
    pub fn try_from_name(name: &str) -> Option<Self> {
        match name {
            "not" | "like" | "ilike" | "is_null" | "is_not_null" | "is_true"
            | "is_false" | "is_not_true" | "is_not_false" | "is_unknown"
            | "is_not_unknown" | "negative" | "negate" => Some(Self {
                expr_name: name.to_string(),
            }),
            _ => None,
        }
    }

    pub async fn build(
        self,
        consumer: &impl SubstraitConsumer,
        f: &ScalarFunction,
        input_schema: &DFSchema,
    ) -> Result<Expr> {
        match self.expr_name.as_str() {
            "like" => Self::build_like_expr(consumer, false, f, input_schema).await,
            "ilike" => Self::build_like_expr(consumer, true, f, input_schema).await,
            "not" | "negative" | "negate" | "is_null" | "is_not_null" | "is_true"
            | "is_false" | "is_not_true" | "is_not_false" | "is_unknown"
            | "is_not_unknown" => {
                Self::build_unary_expr(consumer, &self.expr_name, f, input_schema).await
            }
            _ => {
                not_impl_err!("Unsupported builtin expression: {}", self.expr_name)
            }
        }
    }

    async fn build_unary_expr(
        consumer: &impl SubstraitConsumer,
        fn_name: &str,
        f: &ScalarFunction,
        input_schema: &DFSchema,
    ) -> Result<Expr> {
        if f.arguments.len() != 1 {
            return substrait_err!("Expect one argument for {fn_name} expr");
        }
        let Some(ArgType::Value(expr_substrait)) = &f.arguments[0].arg_type else {
            return substrait_err!("Invalid arguments type for {fn_name} expr");
        };
        let arg = consumer
            .consume_expression(expr_substrait, input_schema)
            .await?;
        let arg = Box::new(arg);

        let expr = match fn_name {
            "not" => Expr::Not(arg),
            "negative" | "negate" => Expr::Negative(arg),
            "is_null" => Expr::IsNull(arg),
            "is_not_null" => Expr::IsNotNull(arg),
            "is_true" => Expr::IsTrue(arg),
            "is_false" => Expr::IsFalse(arg),
            "is_not_true" => Expr::IsNotTrue(arg),
            "is_not_false" => Expr::IsNotFalse(arg),
            "is_unknown" => Expr::IsUnknown(arg),
            "is_not_unknown" => Expr::IsNotUnknown(arg),
            _ => return not_impl_err!("Unsupported builtin expression: {}", fn_name),
        };

        Ok(expr)
    }

    async fn build_like_expr(
        consumer: &impl SubstraitConsumer,
        case_insensitive: bool,
        f: &ScalarFunction,
        input_schema: &DFSchema,
    ) -> Result<Expr> {
        let fn_name = if case_insensitive { "ILIKE" } else { "LIKE" };
        if f.arguments.len() != 2 && f.arguments.len() != 3 {
            return substrait_err!("Expect two or three arguments for `{fn_name}` expr");
        }

        let Some(ArgType::Value(expr_substrait)) = &f.arguments[0].arg_type else {
            return substrait_err!("Invalid arguments type for `{fn_name}` expr");
        };
        let expr = consumer
            .consume_expression(expr_substrait, input_schema)
            .await?;
        let Some(ArgType::Value(pattern_substrait)) = &f.arguments[1].arg_type else {
            return substrait_err!("Invalid arguments type for `{fn_name}` expr");
        };
        let pattern = consumer
            .consume_expression(pattern_substrait, input_schema)
            .await?;

        // Default case: escape character is Literal(Utf8(None))
        let escape_char = if f.arguments.len() == 3 {
            let Some(ArgType::Value(escape_char_substrait)) = &f.arguments[2].arg_type
            else {
                return substrait_err!("Invalid arguments type for `{fn_name}` expr");
            };

            let escape_char_expr = consumer
                .consume_expression(escape_char_substrait, input_schema)
                .await?;

            match escape_char_expr {
                Expr::Literal(ScalarValue::Utf8(escape_char_string)) => {
                    // Convert Option<String> to Option<char>
                    escape_char_string.and_then(|s| s.chars().next())
                }
                _ => {
                    return substrait_err!(
                    "Expect Utf8 literal for escape char, but found {escape_char_expr:?}"
                )
                }
            }
        } else {
            None
        };

        Ok(Expr::Like(Like {
            negated: false,
            expr: Box::new(expr),
            pattern: Box::new(pattern),
            escape_char,
            case_insensitive,
        }))
    }
}

#[cfg(test)]
mod test {
    use crate::extensions::Extensions;
    use crate::logical_plan::consumer::{
        from_substrait_literal_without_names, from_substrait_rex,
        DefaultSubstraitConsumer,
    };
    use arrow::array::types::IntervalMonthDayNano;
    use datafusion::arrow;
    use datafusion::common::DFSchema;
    use datafusion::error::Result;
    use datafusion::execution::SessionState;
    use datafusion::prelude::{Expr, SessionContext};
    use datafusion::scalar::ScalarValue;
    use std::sync::LazyLock;
    use substrait::proto::expression::literal::{
        interval_day_to_second, IntervalCompound, IntervalDayToSecond,
        IntervalYearToMonth, LiteralType,
    };
    use substrait::proto::expression::window_function::BoundsType;
    use substrait::proto::expression::Literal;

    static TEST_SESSION_STATE: LazyLock<SessionState> =
        LazyLock::new(|| SessionContext::default().state());
    static TEST_EXTENSIONS: LazyLock<Extensions> = LazyLock::new(Extensions::default);
    fn test_consumer() -> DefaultSubstraitConsumer<'static> {
        let extensions = &TEST_EXTENSIONS;
        let state = &TEST_SESSION_STATE;
        DefaultSubstraitConsumer::new(extensions, state)
    }

    #[test]
    fn interval_compound_different_precision() -> Result<()> {
        // DF producer (and thus roundtrip) always uses precision = 9,
        // this test exists to test with some other value.
        let substrait = Literal {
            nullable: false,
            type_variation_reference: 0,
            literal_type: Some(LiteralType::IntervalCompound(IntervalCompound {
                interval_year_to_month: Some(IntervalYearToMonth {
                    years: 1,
                    months: 2,
                }),
                interval_day_to_second: Some(IntervalDayToSecond {
                    days: 3,
                    seconds: 4,
                    subseconds: 5,
                    precision_mode: Some(
                        interval_day_to_second::PrecisionMode::Precision(6),
                    ),
                }),
            })),
        };

        let consumer = test_consumer();
        assert_eq!(
            from_substrait_literal_without_names(&consumer, &substrait)?,
            ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano {
                months: 14,
                days: 3,
                nanoseconds: 4_000_005_000
            }))
        );

        Ok(())
    }

    #[tokio::test]
    async fn window_function_with_range_unit_and_no_order_by() -> Result<()> {
        let substrait = substrait::proto::Expression {
            rex_type: Some(substrait::proto::expression::RexType::WindowFunction(
                substrait::proto::expression::WindowFunction {
                    function_reference: 0,
                    bounds_type: BoundsType::Range as i32,
                    sorts: vec![],
                    ..Default::default()
                },
            )),
        };

        let mut consumer = test_consumer();

        // Just registering a single function (index 0) so that the plan
        // does not throw a "function not found" error.
        let mut extensions = Extensions::default();
        extensions.register_function("count".to_string());
        consumer.extensions = &extensions;

        match from_substrait_rex(&consumer, &substrait, &DFSchema::empty()).await? {
            Expr::WindowFunction(window_function) => {
                assert_eq!(window_function.params.order_by.len(), 1)
            }
            _ => panic!("expr was not a WindowFunction"),
        };

        Ok(())
    }

    #[tokio::test]
    async fn window_function_with_count() -> Result<()> {
        let substrait = substrait::proto::Expression {
            rex_type: Some(substrait::proto::expression::RexType::WindowFunction(
                substrait::proto::expression::WindowFunction {
                    function_reference: 0,
                    ..Default::default()
                },
            )),
        };

        let mut consumer = test_consumer();

        let mut extensions = Extensions::default();
        extensions.register_function("count".to_string());
        consumer.extensions = &extensions;

        match from_substrait_rex(&consumer, &substrait, &DFSchema::empty()).await? {
            Expr::WindowFunction(window_function) => {
                assert_eq!(window_function.params.args.len(), 1)
            }
            _ => panic!("expr was not a WindowFunction"),
        };

        Ok(())
    }
}
