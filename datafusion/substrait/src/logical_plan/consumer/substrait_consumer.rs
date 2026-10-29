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
use crate::logical_plan::consumer::{
    field_from_substrait_type_without_names, from_lambda,
};
use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::catalog::TableProvider;
use datafusion::common::datatype::FieldExt;
use datafusion::common::{
    DFSchema, ScalarValue, TableReference, not_impl_err, substrait_err,
};
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::logical_expr::expr::LambdaVariable;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan};
use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use substrait::proto::expression as substrait_expression;
use substrait::proto::expression::{
    Enum, FieldReference, IfThen, Literal, MultiOrList, Nested, ScalarFunction,
    SingularOrList, SwitchExpression, WindowFunction,
};
use substrait::proto::{self, Type};
use substrait::proto::{
    AggregateRel, ConsistentPartitionWindowRel, CrossRel, DynamicParameter, ExchangeRel,
    Expression, ExtensionLeafRel, ExtensionMultiRel, ExtensionSingleRel, FetchRel,
    FilterRel, JoinRel, ProjectRel, ReadRel, Rel, SetRel, SortRel, r#type,
};

#[async_trait]
/// This trait is used to consume Substrait plans, converting them into DataFusion Logical Plans.
/// It can be implemented by users to allow for custom handling of relations, expressions, etc.
///
/// Relation and expression callbacks receive their lexical scope explicitly.
/// Independent inputs may invoke callbacks concurrently on the same consumer.
/// Keep traversal scopes in the context and make shared services concurrency-safe,
/// or override [`Self::consume_rel_inputs`] to consume inputs sequentially.
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
/// # use substrait::proto::{ExtensionLeafRel, FilterRel, ProjectRel, Type};
/// # use datafusion::arrow::datatypes::DataType;
/// # use datafusion::logical_expr::expr::ScalarFunction;
/// # use datafusion_substrait::extensions::Extensions;
/// # use datafusion_substrait::logical_plan::consumer::{
/// #     from_project_rel, from_substrait_rel, from_substrait_rex, SubstraitConsumer, SubstraitConsumerContext
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
///     async fn consume_project(&self, rel: &ProjectRel, context: &SubstraitConsumerContext) -> Result<LogicalPlan> {
///         let df_plan = from_project_rel(self, rel, context).await?;
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
///     async fn consume_filter(&self, rel: &FilterRel, context: &SubstraitConsumerContext) -> Result<LogicalPlan> {
///         let input = self.consume_rel(rel.input.as_ref().unwrap(), context).await?;
///         let expression =
///             self.consume_expression(rel.condition.as_ref().unwrap(), input.schema(), context)
///                 .await?;
///         // though this one is quite boring
///         LogicalPlanBuilder::from(input).filter(expression)?.build()
///     }
///
///     // You can add handlers for extension relations
///     async fn consume_extension_leaf(
///         &self,
///         rel: &ExtensionLeafRel,
///         _context: &SubstraitConsumerContext,
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
    async fn consume_rel(
        &self,
        rel: &Rel,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_substrait_rel(self, rel, context).await
    }

    /// Consumes independent inputs of a JOIN or UNION, returning plans in input order.
    ///
    /// Implementations must return exactly one plan per input, in the same order.
    /// On error, implementations must drop any unfinished input futures.
    ///
    /// The default consumes inputs concurrently using the same immutable context.
    /// Overrides can provide a different scheduling policy.
    async fn consume_rel_inputs(
        &self,
        rels: &[&Rel],
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<Vec<LogicalPlan>> {
        let pending = FuturesUnordered::new();
        for (index, rel) in rels.iter().enumerate() {
            pending.push(async move {
                self.consume_rel(rel, context)
                    .await
                    .map(|plan| (index, plan))
            });
        }
        let mut completed = pending.try_collect::<Vec<_>>().await?;
        completed.sort_unstable_by_key(|(index, _)| *index);
        Ok(completed.into_iter().map(|(_, plan)| plan).collect())
    }

    async fn consume_read(
        &self,
        rel: &ReadRel,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_read_rel(self, rel, context).await
    }

    async fn consume_filter(
        &self,
        rel: &FilterRel,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_filter_rel(self, rel, context).await
    }

    async fn consume_fetch(
        &self,
        rel: &FetchRel,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_fetch_rel(self, rel, context).await
    }

    async fn consume_aggregate(
        &self,
        rel: &AggregateRel,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_aggregate_rel(self, rel, context).await
    }

    async fn consume_sort(
        &self,
        rel: &SortRel,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_sort_rel(self, rel, context).await
    }

    async fn consume_join(
        &self,
        rel: &JoinRel,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_join_rel(self, rel, context).await
    }

    async fn consume_project(
        &self,
        rel: &ProjectRel,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_project_rel(self, rel, context).await
    }

    async fn consume_set(
        &self,
        rel: &SetRel,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_set_rel(self, rel, context).await
    }

    async fn consume_cross(
        &self,
        rel: &CrossRel,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_cross_rel(self, rel, context).await
    }

    async fn consume_consistent_partition_window(
        &self,
        _rel: &ConsistentPartitionWindowRel,
        _context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<LogicalPlan> {
        not_impl_err!("Consistent Partition Window Rel not supported")
    }

    async fn consume_exchange(
        &self,
        rel: &ExchangeRel,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<LogicalPlan> {
        from_exchange_rel(self, rel, context).await
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
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<Expr> {
        from_substrait_rex(self, expr, input_schema, context).await
    }

    async fn consume_literal(
        &self,
        expr: &Literal,
        _context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<Expr> {
        from_literal(self, expr).await
    }

    async fn consume_field_reference(
        &self,
        expr: &FieldReference,
        input_schema: &DFSchema,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<Expr> {
        from_field_reference(self, expr, input_schema, context).await
    }

    async fn consume_scalar_function(
        &self,
        expr: &ScalarFunction,
        input_schema: &DFSchema,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<Expr> {
        from_scalar_function(self, expr, input_schema, context).await
    }

    async fn consume_window_function(
        &self,
        expr: &WindowFunction,
        input_schema: &DFSchema,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<Expr> {
        from_window_function(self, expr, input_schema, context).await
    }

    async fn consume_if_then(
        &self,
        expr: &IfThen,
        input_schema: &DFSchema,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<Expr> {
        from_if_then(self, expr, input_schema, context).await
    }

    async fn consume_switch(
        &self,
        _expr: &SwitchExpression,
        _input_schema: &DFSchema,
        _context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<Expr> {
        not_impl_err!("Switch expression not supported")
    }

    async fn consume_singular_or_list(
        &self,
        expr: &SingularOrList,
        input_schema: &DFSchema,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<Expr> {
        from_singular_or_list(self, expr, input_schema, context).await
    }

    async fn consume_multi_or_list(
        &self,
        _expr: &MultiOrList,
        _input_schema: &DFSchema,
        _context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<Expr> {
        not_impl_err!("Multi Or List expression not supported")
    }

    async fn consume_cast(
        &self,
        expr: &substrait_expression::Cast,
        input_schema: &DFSchema,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<Expr> {
        from_cast(self, expr, input_schema, context).await
    }

    async fn consume_subquery(
        &self,
        expr: &substrait_expression::Subquery,
        input_schema: &DFSchema,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<Expr> {
        from_subquery(self, expr, input_schema, context).await
    }

    async fn consume_nested(
        &self,
        expr: &Nested,
        input_schema: &DFSchema,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<Expr> {
        from_nested(self, expr, input_schema, context).await
    }

    async fn consume_enum(
        &self,
        _expr: &Enum,
        _input_schema: &DFSchema,
        _context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<Expr> {
        not_impl_err!("Enum expression not supported")
    }

    async fn consume_dynamic_parameter(
        &self,
        expr: &DynamicParameter,
        _input_schema: &DFSchema,
        _context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<Expr> {
        let id = format!("${}", expr.parameter_reference + 1);
        let field = expr
            .r#type
            .as_ref()
            .map(|t| {
                super::from_substrait_type_without_names(self, t).map(|dt| {
                    Arc::new(datafusion::arrow::datatypes::Field::new(&id, dt, true))
                })
            })
            .transpose()?;
        Ok(Expr::Placeholder(
            datafusion::logical_expr::expr::Placeholder::new_with_field(id, field),
        ))
    }

    async fn consume_lambda(
        &self,
        expr: &proto::expression::Lambda,
        input_schema: &DFSchema,
        context: &SubstraitConsumerContext,
    ) -> datafusion::common::Result<Expr> {
        from_lambda(self, expr, input_schema, context).await
    }

    // User-Defined Functionality

    // The details of extension relations, and how to handle them, are fully up to users to specify.
    // The following methods allow users to customize the consumer behaviour

    async fn consume_extension_leaf(
        &self,
        rel: &ExtensionLeafRel,
        _context: &SubstraitConsumerContext,
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
        _context: &SubstraitConsumerContext,
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
        _context: &SubstraitConsumerContext,
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

    /// Optional Arrow field metadata for a Substrait type.
    ///
    /// Arrow cannot represent some Substrait types natively. A user-defined
    /// "json" type, for example, becomes a plain `DataType::Utf8`, which loses
    /// the fact that the values are JSON. A consumer can return metadata here
    /// to keep the original type recoverable from the Arrow schema.
    ///
    /// The consumer sees the whole [`Type`], so it can key the metadata off a
    /// user-defined type, a type variation, or anything else it recognizes.
    /// The metadata is attached to the Arrow [`Field`] built for this type,
    /// including a type nested inside a list, map, or struct.
    ///
    /// The default attaches nothing.
    ///
    /// [`Field`]: datafusion::arrow::datatypes::Field
    fn consume_type_metadata(
        &self,
        _typ: &Type,
    ) -> datafusion::common::Result<Option<HashMap<String, String>>> {
        Ok(None)
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

/// Default SubstraitConsumer for converting standard Substrait without user-defined extensions.
///
/// Used as the consumer in [crate::logical_plan::consumer::from_substrait_plan]
pub struct DefaultSubstraitConsumer<'a> {
    pub(super) extensions: &'a Extensions,
    pub(super) state: &'a SessionState,
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
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
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
        _context: &SubstraitConsumerContext,
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
        context: &SubstraitConsumerContext,
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
        let input_plan = self.consume_rel(input_rel, context).await?;
        let plan = plan.with_exprs_and_inputs(plan.expressions(), vec![input_plan])?;
        Ok(LogicalPlan::Extension(Extension { node: plan }))
    }

    async fn consume_extension_multi(
        &self,
        rel: &ExtensionMultiRel,
        context: &SubstraitConsumerContext,
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
            let input_plan = self.consume_rel(input, context).await?;
            inputs.push(input_plan);
        }
        let plan = plan.with_exprs_and_inputs(plan.expressions(), inputs)?;
        Ok(LogicalPlan::Extension(Extension { node: plan }))
    }
}

/// Immutable lexical scope for one path through a Substrait plan.
///
/// Pass the current context to child conversions. Enter a subquery or lambda by
/// deriving a child context; the parent stays unchanged, including on errors or
/// cancellation. Independent inputs may borrow the same context concurrently.
#[derive(Debug, Clone, Default)]
pub struct SubstraitConsumerContext {
    outer_schemas: Vec<Arc<DFSchema>>,
    lambda_parameters: VecDeque<Vec<FieldRef>>,
    next_lambda_parameter: usize,
}

impl SubstraitConsumerContext {
    /// Derives a child scope whose immediately enclosing query has `schema`.
    pub fn with_outer_schema(&self, schema: Arc<DFSchema>) -> Self {
        let mut child = self.clone();
        child.outer_schemas.push(schema);
        child
    }

    /// Returns the enclosing query schema; one means the immediate parent.
    /// Zero or a depth beyond the enclosing queries returns `None`.
    pub fn get_outer_schema(&self, steps_out: usize) -> Option<Arc<DFSchema>> {
        if steps_out == 0 {
            return None;
        }
        self.outer_schemas
            .len()
            .checked_sub(steps_out)
            .map(|index| Arc::clone(&self.outer_schemas[index]))
    }

    /// Derives a child lambda scope and returns its assigned parameter names.
    pub fn with_lambda_parameters(
        &self,
        consumer: &impl SubstraitConsumer,
        lambda_parameters: &[Type],
        input_schema: &DFSchema,
    ) -> datafusion::common::Result<(Self, Vec<String>)> {
        let mut child = self.clone();

        let lambda_parameters = lambda_parameters
            .iter()
            .map(|ty| {
                let (assigned_number, default_name) =
                    next_lambda_parameter_name(child.next_lambda_parameter, input_schema);

                child.next_lambda_parameter = assigned_number + 1;

                Ok(field_from_substrait_type_without_names(consumer, ty)?
                    .renamed(&default_name))
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?;

        let names = lambda_parameters.iter().map(|f| f.name().clone()).collect();

        child.lambda_parameters.push_front(lambda_parameters);

        Ok((child, names))
    }

    /// Resolves a parameter in the current lambda (zero) or an enclosing lambda.
    pub fn lambda_variable(
        &self,
        steps_out: usize,
        field_idx: usize,
    ) -> datafusion::common::Result<Expr> {
        let lambda_parameters = &self.lambda_parameters;

        let Some(lambda_parameters) = lambda_parameters.get(steps_out) else {
            return substrait_err!(
                "No lambda at {steps_out} steps out, got only {}",
                lambda_parameters.len()
            );
        };

        let Some(var) = lambda_parameters.get(field_idx) else {
            return substrait_err!(
                "At lambda {steps_out} steps out, no field at index {field_idx}, got only {}",
                lambda_parameters.len()
            );
        };

        Ok(Expr::LambdaVariable(LambdaVariable::new(
            var.name().clone(),
            Some(Arc::clone(var)),
        )))
    }
}

/// Returns the next available lambda parameter name and the index it was assigned.
///
/// Names follow the pattern `pN` where `N` starts at `next_lambda_parameter`. If `pN`
/// conflicts with an existing column name in `input_schema`, `N` is incremented until
/// a free name is found.
fn next_lambda_parameter_name(
    mut next_lambda_parameter: usize,
    input_schema: &DFSchema,
) -> (usize, String) {
    loop {
        let name = format!("p{next_lambda_parameter}");

        // avoid conflicts with column names
        if !input_schema.has_column_with_unqualified_name(&name) {
            return (next_lambda_parameter, name);
        }

        next_lambda_parameter += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{Field, Schema};

    use futures::poll;
    use proto::expression::{ReferenceSegment, field_reference, reference_segment};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Semaphore;

    struct ActiveInput<'a>(&'a AtomicUsize);

    impl Drop for ActiveInput<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

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
        let context = SubstraitConsumerContext::default();

        // No schemas pushed — any steps_out should return None
        assert!(context.get_outer_schema(0).is_none());
        assert!(context.get_outer_schema(1).is_none());
        assert!(context.get_outer_schema(2).is_none());
    }

    #[test]
    fn test_get_outer_schema_single_level() {
        let context = SubstraitConsumerContext::default();

        let schema_a = make_schema(&[("a", DataType::Int64)]);
        let parent = context.clone();
        let context = context.with_outer_schema(Arc::clone(&schema_a));

        // steps_out=1 returns the one pushed schema
        let result = context.get_outer_schema(1).unwrap();
        assert_eq!(result.fields().len(), 1);
        assert_eq!(result.fields()[0].name(), "a");

        // steps_out=0 and steps_out=2 are out of range
        assert!(context.get_outer_schema(0).is_none());
        assert!(context.get_outer_schema(2).is_none());

        let context = parent;
        assert!(context.get_outer_schema(1).is_none());
    }

    #[test]
    fn test_get_outer_schema_nested() {
        let context = SubstraitConsumerContext::default();

        let schema_a = make_schema(&[("a", DataType::Int64)]);
        let schema_b = make_schema(&[("b", DataType::Utf8)]);

        let context = context.with_outer_schema(Arc::clone(&schema_a));
        let parent = context.clone();
        let context = context.with_outer_schema(Arc::clone(&schema_b));

        // steps_out=1 returns the most recent (schema_b)
        let result = context.get_outer_schema(1).unwrap();
        assert_eq!(result.fields()[0].name(), "b");

        // steps_out=2 returns the grandparent (schema_a)
        let result = context.get_outer_schema(2).unwrap();
        assert_eq!(result.fields()[0].name(), "a");

        // steps_out=3 exceeds depth
        assert!(context.get_outer_schema(3).is_none());

        // The parent is unchanged — steps_out=1 still returns schema_a
        let context = parent;
        let result = context.get_outer_schema(1).unwrap();
        assert_eq!(result.fields()[0].name(), "a");
        assert!(context.get_outer_schema(2).is_none());
    }

    // A custom consumer exercises the trait default independently of the default
    // consumer's scheduling policy.
    struct InputConsumer {
        extensions: Arc<Extensions>,
        state: Arc<SessionState>,
        reads: Arc<Mutex<Vec<String>>>,
        ready: Arc<Semaphore>,
        gates: Arc<HashMap<String, Semaphore>>,
        active: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SubstraitConsumer for InputConsumer {
        fn get_extensions(&self) -> &Extensions {
            &self.extensions
        }

        fn get_function_registry(&self) -> &impl FunctionRegistry {
            self.state.as_ref()
        }

        async fn resolve_table_ref(
            &self,
            _: &TableReference,
        ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
            unreachable!("test consumes reads directly")
        }

        async fn consume_expression(
            &self,
            expr: &Expression,
            input_schema: &DFSchema,
            context: &SubstraitConsumerContext,
        ) -> datafusion::common::Result<Expr> {
            self.active.fetch_add(1, Ordering::SeqCst);
            let _active = ActiveInput(&self.active);
            self.ready.acquire().await.unwrap().forget();
            from_substrait_rex(self, expr, input_schema, context).await
        }

        async fn consume_rel(
            &self,
            rel: &Rel,
            _context: &SubstraitConsumerContext,
        ) -> datafusion::common::Result<LogicalPlan> {
            let Some(proto::rel::RelType::Read(read)) = &rel.rel_type else {
                panic!("expected a read")
            };
            let Some(proto::read_rel::ReadType::NamedTable(table)) = &read.read_type
            else {
                panic!("expected a named table")
            };
            self.reads.lock().unwrap().push(table.names[0].clone());
            self.active.fetch_add(1, Ordering::SeqCst);
            let _active = ActiveInput(&self.active);
            let gate = self.gates.get(&table.names[0]).unwrap_or(&self.ready);
            gate.acquire().await.unwrap().forget();
            if table.names[0] == "fail" {
                return substrait_err!("input failed");
            }
            let provider =
                datafusion::catalog::empty::EmptyTable::new(Arc::new(Schema::new(vec![
                    Field::new("x", DataType::Int64, false),
                ])));
            datafusion::logical_expr::LogicalPlanBuilder::scan(
                table.names[0].clone(),
                datafusion::datasource::provider_as_source(Arc::new(provider)),
                None,
            )?
            .build()
        }
    }

    fn input_consumer() -> InputConsumer {
        InputConsumer {
            extensions: Arc::new(Extensions::default()),
            state: Arc::new(
                datafusion::execution::SessionStateBuilder::new_with_default_features()
                    .build(),
            ),
            reads: Arc::new(Mutex::default()),
            ready: Arc::new(Semaphore::new(0)),
            gates: Arc::new(HashMap::new()),
            active: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn input_read(name: &str) -> Rel {
        Rel {
            rel_type: Some(proto::rel::RelType::Read(Box::new(ReadRel {
                read_type: Some(proto::read_rel::ReadType::NamedTable(
                    proto::read_rel::NamedTable {
                        names: vec![name.into()],
                        ..Default::default()
                    },
                )),
                ..Default::default()
            }))),
        }
    }

    fn identity_lambda(kind: r#type::Kind, steps_out: u32) -> proto::expression::Lambda {
        proto::expression::Lambda {
            parameters: Some(r#type::Struct {
                types: vec![Type { kind: Some(kind) }],
                ..Default::default()
            }),
            body: Some(Box::new(Expression {
                rex_type: Some(proto::expression::RexType::Selection(Box::new(
                    FieldReference {
                        reference_type: Some(
                            field_reference::ReferenceType::DirectReference(
                                ReferenceSegment {
                                    reference_type: Some(
                                        reference_segment::ReferenceType::StructField(
                                            Box::new(reference_segment::StructField {
                                                field: 0,
                                                child: None,
                                            }),
                                        ),
                                    ),
                                },
                            ),
                        ),
                        root_type: Some(
                            field_reference::RootType::LambdaParameterReference(
                                field_reference::LambdaParameterReference { steps_out },
                            ),
                        ),
                    },
                ))),
            })),
        }
    }

    fn assert_lambda_body(expr: Expr, name: &str, data_type: DataType) {
        let Expr::Lambda(lambda) = expr else {
            panic!("expected lambda")
        };
        let Expr::LambdaVariable(variable) = *lambda.body else {
            panic!("expected lambda variable")
        };
        assert_eq!(variable.name, name);
        assert_eq!(variable.field.unwrap().data_type(), &data_type);
    }

    #[tokio::test]
    async fn shared_consumer_keeps_concurrent_lambda_scopes_independent() {
        let consumer = input_consumer();
        let context = SubstraitConsumerContext::default();
        let schema = DFSchema::empty();
        let integer = identity_lambda(r#type::Kind::I64(r#type::I64::default()), 0);
        let string = identity_lambda(r#type::Kind::String(r#type::String::default()), 0);
        let mut first = Box::pin(consumer.consume_lambda(&integer, &schema, &context));
        let mut second = Box::pin(consumer.consume_lambda(&string, &schema, &context));
        assert!(poll!(first.as_mut()).is_pending());
        assert!(poll!(second.as_mut()).is_pending());
        assert_eq!(consumer.active.load(Ordering::SeqCst), 2);
        consumer.ready.add_permits(2);
        assert_lambda_body(first.await.unwrap(), "p0", DataType::Int64);
        assert_lambda_body(second.await.unwrap(), "p0", DataType::Utf8);
        assert!(context.lambda_variable(0, 0).is_err());
    }

    #[tokio::test]
    async fn lambda_error_and_cancellation_preserve_parent_scope() {
        let consumer = input_consumer();
        let schema = DFSchema::empty();
        let root = SubstraitConsumerContext::default();
        let (context, names) = root
            .with_lambda_parameters(
                &consumer,
                &[Type {
                    kind: Some(r#type::Kind::I64(r#type::I64::default())),
                }],
                &schema,
            )
            .unwrap();
        assert_eq!(names, vec!["p0"]);
        let nested = identity_lambda(r#type::Kind::String(r#type::String::default()), 1);
        let mut cancelled = Box::pin(consumer.consume_lambda(&nested, &schema, &context));
        assert!(poll!(cancelled.as_mut()).is_pending());
        drop(cancelled);
        assert_eq!(consumer.active.load(Ordering::SeqCst), 0);
        let mut invalid = nested.clone();
        invalid.body = None;
        assert!(
            consumer
                .consume_lambda(&invalid, &schema, &context)
                .await
                .is_err()
        );
        consumer.ready.add_permits(1);
        let result = consumer
            .consume_lambda(&nested, &schema, &context)
            .await
            .unwrap();
        let Expr::Lambda(ref lambda) = result else {
            panic!("expected lambda")
        };
        assert_eq!(lambda.params, vec!["p1"]);
        assert_lambda_body(result, "p0", DataType::Int64);
        assert!(context.lambda_variable(1, 0).is_err());
        assert!(root.lambda_variable(0, 0).is_err());
    }

    #[tokio::test]
    async fn concurrent_inputs_overlap_and_preserve_input_order() {
        let mut consumer = input_consumer();
        consumer.gates = Arc::new(HashMap::from([
            ("a".into(), Semaphore::new(0)),
            ("b".into(), Semaphore::new(0)),
        ]));
        let rels = [input_read("a"), input_read("b")];
        let inputs = rels.iter().collect::<Vec<_>>();
        let context = SubstraitConsumerContext::default();
        let mut pending = Box::pin(consumer.consume_rel_inputs(&inputs, &context));
        assert!(poll!(&mut pending).is_pending());
        assert_eq!(*consumer.reads.lock().unwrap(), ["a", "b"]);
        assert_eq!(consumer.active.load(Ordering::SeqCst), 2);
        consumer.gates["b"].add_permits(1);
        assert!(poll!(&mut pending).is_pending());
        assert_eq!(consumer.active.load(Ordering::SeqCst), 1);
        consumer.gates["a"].add_permits(1);
        let plans = pending.await.unwrap();
        assert_eq!(plans.len(), 2);
        assert!(
            plans[0]
                .display_indent()
                .to_string()
                .contains("TableScan: a")
        );
        assert!(
            plans[1]
                .display_indent()
                .to_string()
                .contains("TableScan: b")
        );
        assert_eq!(consumer.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn concurrent_inputs_cancel_pending_siblings_on_error() {
        let mut consumer = input_consumer();
        consumer.gates = Arc::new(HashMap::from([("fail".into(), Semaphore::new(0))]));
        // Exercise more inputs than the small-future optimization in try_join_all.
        let mut rels = (0..40)
            .map(|i| input_read(&format!("a{i}")))
            .collect::<Vec<_>>();
        rels.push(input_read("fail"));
        let inputs = rels.iter().collect::<Vec<_>>();
        let context = SubstraitConsumerContext::default();
        let mut pending = Box::pin(consumer.consume_rel_inputs(&inputs, &context));
        assert!(poll!(&mut pending).is_pending());
        assert_eq!(consumer.active.load(Ordering::SeqCst), 41);
        consumer.gates["fail"].add_permits(1);
        let error = pending.await.unwrap_err();
        assert!(error.to_string().contains("input failed"));
        assert_eq!(consumer.active.load(Ordering::SeqCst), 0);
    }

    struct HookConsumer {
        inner: InputConsumer,
        calls: Mutex<Vec<usize>>,
        truncate: bool,
    }

    #[async_trait]
    impl SubstraitConsumer for HookConsumer {
        fn get_extensions(&self) -> &Extensions {
            &self.inner.extensions
        }
        fn get_function_registry(&self) -> &impl FunctionRegistry {
            self.inner.state.as_ref()
        }
        async fn resolve_table_ref(
            &self,
            _: &TableReference,
        ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
            unreachable!("test consumes reads directly")
        }
        async fn consume_rel_inputs(
            &self,
            rels: &[&Rel],
            context: &SubstraitConsumerContext,
        ) -> datafusion::common::Result<Vec<LogicalPlan>> {
            self.calls.lock().unwrap().push(rels.len());
            let mut plans = self.inner.consume_rel_inputs(rels, context).await?;
            if self.truncate {
                plans.pop();
            }
            Ok(plans)
        }
    }

    #[tokio::test]
    async fn join_and_union_use_the_input_hook() {
        for truncate in [false, true] {
            let consumer = HookConsumer {
                inner: input_consumer(),
                calls: Mutex::default(),
                truncate,
            };
            consumer.inner.ready.add_permits(6);
            let join = JoinRel {
                left: Some(Box::new(input_read("a"))),
                right: Some(Box::new(input_read("b"))),
                r#type: proto::join_rel::JoinType::Inner as i32,
                ..Default::default()
            };
            let result = consumer
                .consume_join(&join, &SubstraitConsumerContext::default())
                .await;
            if truncate {
                assert!(
                    result
                        .unwrap_err()
                        .to_string()
                        .contains("wrong number of inputs")
                );
            } else {
                let LogicalPlan::Join(join) = result.unwrap() else {
                    panic!("expected join")
                };
                assert!(
                    join.left
                        .display_indent()
                        .to_string()
                        .contains("TableScan: a")
                );
                assert!(
                    join.right
                        .display_indent()
                        .to_string()
                        .contains("TableScan: b")
                );
            }
            for op in [
                proto::set_rel::SetOp::UnionAll,
                proto::set_rel::SetOp::UnionDistinct,
            ] {
                let set = SetRel {
                    inputs: vec![input_read("a"), input_read("b")],
                    op: op as i32,
                    ..Default::default()
                };
                let result = consumer
                    .consume_set(&set, &SubstraitConsumerContext::default())
                    .await;
                if truncate {
                    assert!(
                        result
                            .unwrap_err()
                            .to_string()
                            .contains("wrong number of inputs")
                    );
                } else {
                    let plan = result.unwrap().display_indent().to_string();
                    assert!(
                        plan.find("TableScan: a").unwrap()
                            < plan.find("TableScan: b").unwrap()
                    );
                }
            }
            assert_eq!(*consumer.calls.lock().unwrap(), [2, 2, 2]);
        }
    }
}
