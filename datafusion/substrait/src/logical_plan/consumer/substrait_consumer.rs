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

use super::{
    from_aggregate_rel, from_cast, from_cross_rel, from_exchange_rel, from_fetch_rel,
    from_field_reference, from_filter_rel, from_if_then, from_join_rel, from_literal,
    from_nested, from_project_rel, from_read_rel, from_scalar_function, from_set_rel,
    from_singular_or_list, from_sort_rel, from_subquery, from_substrait_rel,
    from_substrait_rex, from_window_function,
};
use crate::extensions::Extensions;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::stats::Precision;
use datafusion::common::{
    DFSchema, ScalarValue, Statistics, TableReference, not_impl_err, substrait_err,
};
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::logical_expr::TableType;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::{Arc, RwLock};
use substrait::proto;
use substrait::proto::expression as substrait_expression;
use substrait::proto::expression::{
    Enum, FieldReference, IfThen, Literal, MultiOrList, Nested, ScalarFunction,
    SingularOrList, SwitchExpression, WindowFunction,
};
use substrait::proto::{
    AggregateRel, ConsistentPartitionWindowRel, CrossRel, DynamicParameter, ExchangeRel,
    Expression, ExtensionLeafRel, ExtensionMultiRel, ExtensionSingleRel, FetchRel,
    FilterRel, JoinRel, ProjectRel, ReadRel, Rel, SetRel, SortRel, r#type,
};

/// Advisory hints extracted from a Substrait `RelCommon.hint.stats` message,
/// passed to [`SubstraitConsumer::resolve_table_ref`] so that implementors can
/// incorporate them into the returned [`TableProvider`].
///
/// The struct is `#[non_exhaustive]` so that new fields can be added in future
/// versions without breaking existing implementations.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct SubstraitHints {
    /// Estimated number of rows, from `hint.stats.row_count`.
    ///
    /// `None` means the hint was absent or could not be reliably interpreted
    /// (e.g. proto3 default-zero or a non-finite value).
    pub row_count: Option<f64>,
    /// Estimated average byte size per record, from `hint.stats.record_size`.
    ///
    /// `None` means the hint was absent or non-positive / non-finite.
    pub record_size: Option<f64>,
}

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
/// #     from_project_rel, from_substrait_rel, from_substrait_rex, SubstraitConsumer, SubstraitHints
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
///         _hints: SubstraitHints,
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
///         // extract type_reference from the new TypeAnchorType oneof
///         let type_ref = match literal.type_anchor_type {
///             Some(proto::expression::literal::user_defined::TypeAnchorType::TypeReference(r)) => r,
///             Some(proto::expression::literal::user_defined::TypeAnchorType::TypeAliasReference(_)) => {
///                 return not_impl_err!("Type alias references are not yet supported")
///             }
///             None => 0,
///         };
///         let type_string = self.extensions.types.get(&type_ref).unwrap();
///         match type_string.as_str() {
///             "u!foo" => not_impl_err!("handle foo conversion"),
///             "u!bar" => not_impl_err!("handle bar conversion"),
///             _ => substrait_err!("unexpected type")
///         }
///     }
/// }
/// ```
pub trait SubstraitConsumer: Send + Sync + Sized {
    async fn resolve_table_ref(
        &self,
        table_ref: &TableReference,
        hints: SubstraitHints,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>>;

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
    async fn consume_rel(&self, rel: &Rel) -> datafusion::common::Result<LogicalPlan> {
        from_substrait_rel(self, rel).await
    }

    async fn consume_read(
        &self,
        rel: &ReadRel,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_read_rel(self, rel).await
    }

    async fn consume_filter(
        &self,
        rel: &FilterRel,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_filter_rel(self, rel).await
    }

    async fn consume_fetch(
        &self,
        rel: &FetchRel,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_fetch_rel(self, rel).await
    }

    async fn consume_aggregate(
        &self,
        rel: &AggregateRel,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_aggregate_rel(self, rel).await
    }

    async fn consume_sort(
        &self,
        rel: &SortRel,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_sort_rel(self, rel).await
    }

    async fn consume_join(
        &self,
        rel: &JoinRel,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_join_rel(self, rel).await
    }

    async fn consume_project(
        &self,
        rel: &ProjectRel,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_project_rel(self, rel).await
    }

    async fn consume_set(&self, rel: &SetRel) -> datafusion::common::Result<LogicalPlan> {
        from_set_rel(self, rel).await
    }

    async fn consume_cross(
        &self,
        rel: &CrossRel,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_cross_rel(self, rel).await
    }

    async fn consume_consistent_partition_window(
        &self,
        _rel: &ConsistentPartitionWindowRel,
    ) -> datafusion::common::Result<LogicalPlan> {
        not_impl_err!("Consistent Partition Window Rel not supported")
    }

    async fn consume_exchange(
        &self,
        rel: &ExchangeRel,
    ) -> datafusion::common::Result<LogicalPlan> {
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
    ) -> datafusion::common::Result<Expr> {
        from_substrait_rex(self, expr, input_schema).await
    }

    async fn consume_literal(&self, expr: &Literal) -> datafusion::common::Result<Expr> {
        from_literal(self, expr).await
    }

    async fn consume_field_reference(
        &self,
        expr: &FieldReference,
        input_schema: &DFSchema,
    ) -> datafusion::common::Result<Expr> {
        from_field_reference(self, expr, input_schema).await
    }

    async fn consume_scalar_function(
        &self,
        expr: &ScalarFunction,
        input_schema: &DFSchema,
    ) -> datafusion::common::Result<Expr> {
        from_scalar_function(self, expr, input_schema).await
    }

    async fn consume_window_function(
        &self,
        expr: &WindowFunction,
        input_schema: &DFSchema,
    ) -> datafusion::common::Result<Expr> {
        from_window_function(self, expr, input_schema).await
    }

    async fn consume_if_then(
        &self,
        expr: &IfThen,
        input_schema: &DFSchema,
    ) -> datafusion::common::Result<Expr> {
        from_if_then(self, expr, input_schema).await
    }

    async fn consume_switch(
        &self,
        _expr: &SwitchExpression,
        _input_schema: &DFSchema,
    ) -> datafusion::common::Result<Expr> {
        not_impl_err!("Switch expression not supported")
    }

    async fn consume_singular_or_list(
        &self,
        expr: &SingularOrList,
        input_schema: &DFSchema,
    ) -> datafusion::common::Result<Expr> {
        from_singular_or_list(self, expr, input_schema).await
    }

    async fn consume_multi_or_list(
        &self,
        _expr: &MultiOrList,
        _input_schema: &DFSchema,
    ) -> datafusion::common::Result<Expr> {
        not_impl_err!("Multi Or List expression not supported")
    }

    async fn consume_cast(
        &self,
        expr: &substrait_expression::Cast,
        input_schema: &DFSchema,
    ) -> datafusion::common::Result<Expr> {
        from_cast(self, expr, input_schema).await
    }

    async fn consume_subquery(
        &self,
        expr: &substrait_expression::Subquery,
        input_schema: &DFSchema,
    ) -> datafusion::common::Result<Expr> {
        from_subquery(self, expr, input_schema).await
    }

    async fn consume_nested(
        &self,
        expr: &Nested,
        input_schema: &DFSchema,
    ) -> datafusion::common::Result<Expr> {
        from_nested(self, expr, input_schema).await
    }

    async fn consume_enum(
        &self,
        _expr: &Enum,
        _input_schema: &DFSchema,
    ) -> datafusion::common::Result<Expr> {
        not_impl_err!("Enum expression not supported")
    }

    async fn consume_dynamic_parameter(
        &self,
        _expr: &DynamicParameter,
        _input_schema: &DFSchema,
    ) -> datafusion::common::Result<Expr> {
        not_impl_err!("Dynamic Parameter expression not supported")
    }

    // Outer Schema Stack
    // These methods manage a stack of outer schemas for correlated subquery support.
    // When entering a subquery, the enclosing query's schema is pushed onto the stack.
    // Field references with OuterReference root_type use these to resolve columns.

    /// Push an outer schema onto the stack when entering a subquery.
    fn push_outer_schema(&self, _schema: Arc<DFSchema>) {}

    /// Pop an outer schema from the stack when leaving a subquery.
    fn pop_outer_schema(&self) {}

    /// Get the outer schema at the given nesting depth.
    /// `steps_out = 1` is the immediately enclosing query, `steps_out = 2`
    /// is two levels out, etc. Returns `None` if `steps_out` is 0 or
    /// exceeds the current nesting depth (the caller should treat this as
    /// an error in the Substrait plan).
    fn get_outer_schema(&self, _steps_out: usize) -> Option<Arc<DFSchema>> {
        None
    }

    // User-Defined Functionality

    // The details of extension relations, and how to handle them, are fully up to users to specify.
    // The following methods allow users to customize the consumer behaviour

    async fn consume_extension_leaf(
        &self,
        rel: &ExtensionLeafRel,
    ) -> datafusion::common::Result<LogicalPlan> {
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
    ) -> datafusion::common::Result<LogicalPlan> {
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
    ) -> datafusion::common::Result<LogicalPlan> {
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
    ) -> datafusion::common::Result<DataType> {
        substrait_err!(
            "Missing handler for user-defined type: {}",
            user_defined_type.type_reference
        )
    }

    fn consume_user_defined_literal(
        &self,
        user_defined_literal: &proto::expression::literal::UserDefined,
    ) -> datafusion::common::Result<ScalarValue> {
        let type_ref = match user_defined_literal.type_anchor_type {
            Some(
                proto::expression::literal::user_defined::TypeAnchorType::TypeReference(
                    ref_val,
                ),
            ) => ref_val,
            Some(
                proto::expression::literal::user_defined::TypeAnchorType::TypeAliasReference(_),
            ) => {
                return not_impl_err!(
                    "Type alias references in user-defined literals are not yet supported"
                )
            }
            None => 0,
        };
        substrait_err!("Missing handler for user-defined literals {}", type_ref)
    }
}

/// Wraps an inner [`TableProvider`] and overrides its `statistics()` return value.
///
/// Used by [`DefaultSubstraitConsumer`] to inject a row-count hint carried in a
/// Substrait `RelCommon.hint.stats` when the resolved provider has no statistics.
///
/// # Note on `as_any()` behaviour
///
/// `as_any()` intentionally delegates to the inner provider so that callers can
/// still downcast to the concrete inner type (e.g. `MemTable`) through this
/// wrapper.  As a consequence, downcasting to `StatisticsOverrideTableProvider`
/// itself via `as_any()` will not work — but since this struct is private,
/// external code should never need to do that.
#[derive(Debug)]
struct StatisticsOverrideTableProvider {
    inner: Arc<dyn TableProvider>,
    statistics: Statistics,
}

#[async_trait]
impl TableProvider for StatisticsOverrideTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        // Delegate to the inner provider so that downcasting to the concrete
        // inner type works transparently through this wrapper.
        self.inner.as_any()
    }

    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn constraints(&self) -> Option<&datafusion::common::Constraints> {
        self.inner.constraints()
    }

    fn table_type(&self) -> TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<
        Vec<datafusion::logical_expr::TableProviderFilterPushDown>,
    > {
        self.inner.supports_filters_pushdown(filters)
    }

    fn statistics(&self) -> Option<Statistics> {
        Some(self.statistics.clone())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.scan(state, projection, filters, limit).await
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: datafusion::logical_expr::dml::InsertOp,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.insert_into(state, input, insert_op).await
    }

    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.delete_from(state, filters).await
    }

    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        self.inner.update(state, assignments, filters).await
    }
}

/// Default SubstraitConsumer for converting standard Substrait without user-defined extensions.
///
/// Used as the consumer in [crate::logical_plan::consumer::from_substrait_plan]
pub struct DefaultSubstraitConsumer<'a> {
    pub(super) extensions: &'a Extensions,
    pub(super) state: &'a SessionState,
    outer_schemas: RwLock<Vec<Arc<DFSchema>>>,
}

impl<'a> DefaultSubstraitConsumer<'a> {
    pub fn new(extensions: &'a Extensions, state: &'a SessionState) -> Self {
        DefaultSubstraitConsumer {
            extensions,
            state,
            outer_schemas: RwLock::new(Vec::new()),
        }
    }
}

#[async_trait]
impl SubstraitConsumer for DefaultSubstraitConsumer<'_> {
    async fn resolve_table_ref(
        &self,
        table_ref: &TableReference,
        hints: SubstraitHints,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let table = table_ref.table().to_string();
        let schema = self.state.schema_for_ref(table_ref.clone())?;
        let provider = schema.table(&table).await?;
        // Wrap the provider to inject hint statistics only for fields the
        // provider doesn't already have (checked individually, not as a whole).
        let has_hints = hints.row_count.is_some() || hints.record_size.is_some();
        let provider = match provider {
            Some(provider) if has_hints => {
                let existing = provider.statistics();
                let row_count_absent = existing
                    .as_ref()
                    .is_none_or(|s| matches!(s.num_rows, Precision::Absent));
                let byte_size_absent = existing
                    .as_ref()
                    .is_none_or(|s| matches!(s.total_byte_size, Precision::Absent));
                let inject_row_count = hints.row_count.is_some() && row_count_absent;
                // Both hints required: total_byte_size = row_count * record_size.
                let inject_byte_size = hints.row_count.is_some()
                    && hints.record_size.is_some()
                    && byte_size_absent;
                if inject_row_count || inject_byte_size {
                    let num_rows = if inject_row_count {
                        Precision::Inexact(hints.row_count.unwrap().round() as usize)
                    } else {
                        existing.as_ref().map_or(Precision::Absent, |s| s.num_rows)
                    };
                    let total_byte_size = if inject_byte_size {
                        // Prefer the provider's own row count for consistency.
                        let effective_rows = match &num_rows {
                            Precision::Exact(n) | Precision::Inexact(n) => *n as f64,
                            Precision::Absent => hints.row_count.unwrap(),
                        };
                        let byte_size = effective_rows * hints.record_size.unwrap();
                        // The product of two sub-usize::MAX values can still overflow.
                        if byte_size.is_finite() && byte_size < usize::MAX as f64 {
                            Precision::Inexact(byte_size.round() as usize)
                        } else {
                            Precision::Absent
                        }
                    } else {
                        existing
                            .as_ref()
                            .map_or(Precision::Absent, |s| s.total_byte_size)
                    };
                    let column_statistics =
                        existing.map(|s| s.column_statistics).unwrap_or_else(|| {
                            Statistics::unknown_column(&provider.schema())
                        });
                    let statistics = Statistics {
                        num_rows,
                        total_byte_size,
                        column_statistics,
                    };
                    Some(Arc::new(StatisticsOverrideTableProvider {
                        inner: provider,
                        statistics,
                    }) as Arc<dyn TableProvider>)
                } else {
                    Some(provider)
                }
            }
            provider => provider,
        };
        Ok(provider)
    }

    fn get_extensions(&self) -> &Extensions {
        self.extensions
    }

    fn get_function_registry(&self) -> &impl FunctionRegistry {
        self.state
    }

    fn push_outer_schema(&self, schema: Arc<DFSchema>) {
        self.outer_schemas.write().unwrap().push(schema);
    }

    fn pop_outer_schema(&self) {
        self.outer_schemas.write().unwrap().pop();
    }

    fn get_outer_schema(&self, steps_out: usize) -> Option<Arc<DFSchema>> {
        let schemas = self.outer_schemas.read().unwrap();
        // steps_out=1 → last element, steps_out=2 → second-to-last, etc.
        // Returns None for steps_out=0 or steps_out > stack depth.
        schemas
            .len()
            .checked_sub(steps_out)
            .and_then(|idx| schemas.get(idx).cloned())
    }

    async fn consume_extension_leaf(
        &self,
        rel: &ExtensionLeafRel,
    ) -> datafusion::common::Result<LogicalPlan> {
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
    ) -> datafusion::common::Result<LogicalPlan> {
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
    ) -> datafusion::common::Result<LogicalPlan> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logical_plan::consumer::utils::tests::test_consumer;
    use datafusion::arrow::datatypes::{Field, Schema};

    fn make_schema(fields: &[(&str, DataType)]) -> Arc<DFSchema> {
        let arrow_fields: Vec<Field> = fields
            .iter()
            .map(|(name, dt)| Field::new(*name, dt.clone(), true))
            .collect();
        Arc::new(
            DFSchema::try_from(Schema::new(arrow_fields))
                .expect("failed to create schema"),
        )
    }

    #[test]
    fn test_get_outer_schema_empty_stack() {
        let consumer = test_consumer();

        // No schemas pushed — any steps_out should return None
        assert!(consumer.get_outer_schema(0).is_none());
        assert!(consumer.get_outer_schema(1).is_none());
        assert!(consumer.get_outer_schema(2).is_none());
    }

    #[test]
    fn test_get_outer_schema_single_level() {
        let consumer = test_consumer();

        let schema_a = make_schema(&[("a", DataType::Int64)]);
        consumer.push_outer_schema(Arc::clone(&schema_a));

        // steps_out=1 returns the one pushed schema
        let result = consumer.get_outer_schema(1).unwrap();
        assert_eq!(result.fields().len(), 1);
        assert_eq!(result.fields()[0].name(), "a");

        // steps_out=0 and steps_out=2 are out of range
        assert!(consumer.get_outer_schema(0).is_none());
        assert!(consumer.get_outer_schema(2).is_none());

        consumer.pop_outer_schema();
        assert!(consumer.get_outer_schema(1).is_none());
    }

    #[test]
    fn test_get_outer_schema_nested() {
        let consumer = test_consumer();

        let schema_a = make_schema(&[("a", DataType::Int64)]);
        let schema_b = make_schema(&[("b", DataType::Utf8)]);

        consumer.push_outer_schema(Arc::clone(&schema_a));
        consumer.push_outer_schema(Arc::clone(&schema_b));

        // steps_out=1 returns the most recent (schema_b)
        let result = consumer.get_outer_schema(1).unwrap();
        assert_eq!(result.fields()[0].name(), "b");

        // steps_out=2 returns the grandparent (schema_a)
        let result = consumer.get_outer_schema(2).unwrap();
        assert_eq!(result.fields()[0].name(), "a");

        // steps_out=3 exceeds depth
        assert!(consumer.get_outer_schema(3).is_none());

        // Pop one level — now steps_out=1 returns schema_a
        consumer.pop_outer_schema();
        let result = consumer.get_outer_schema(1).unwrap();
        assert_eq!(result.fields()[0].name(), "a");
        assert!(consumer.get_outer_schema(2).is_none());
    }
}
