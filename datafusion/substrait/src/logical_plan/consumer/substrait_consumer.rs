// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
    use crate::logical_plan::consumer::utils::tests::test_consumer;
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::common::assert_contains;
    use datafusion::logical_expr::ExprSchemable;
    use datafusion::logical_expr::logical_plan::builder::table_scan;
    use futures::poll;
    use tokio::sync::Semaphore;

    fn schema(name: &str, data_type: DataType) -> Arc<DFSchema> {
        Arc::new(
            DFSchema::try_from(Schema::new(vec![Field::new(name, data_type, true)]))
                .unwrap(),
        )
    }

    #[test]
    fn outer_scopes_inherit_without_changing_the_parent() {
        let root = SubstraitConsumerContext::default();
        let a = schema("a", DataType::Int64);
        let b = schema("b", DataType::Utf8);
        let parent = root.with_outer_schema(Arc::clone(&a));
        let child = parent.with_outer_schema(Arc::clone(&b));

        assert!(root.get_outer_schema(1).is_none());
        assert_eq!(parent.get_outer_schema(1), Some(Arc::clone(&a)));
        assert!(parent.get_outer_schema(2).is_none());
        assert_eq!(child.get_outer_schema(1), Some(b));
        assert_eq!(child.get_outer_schema(2), Some(a));
        assert!(child.get_outer_schema(0).is_none());
        assert!(child.get_outer_schema(3).is_none());
    }

    #[test]
    fn lambda_scopes_inherit_without_changing_parents_or_siblings() {
        let consumer = test_consumer();
        let root = SubstraitConsumerContext::default();
        let schema = DFSchema::empty();
        let integer = [Type {
            kind: Some(r#type::Kind::I64(r#type::I64::default())),
        }];
        let string = [Type {
            kind: Some(r#type::Kind::String(r#type::String::default())),
        }];
        let (a, a_names) = root
            .with_lambda_parameters(&consumer, &integer, &schema)
            .unwrap();
        let (b, b_names) = root
            .with_lambda_parameters(&consumer, &string, &schema)
            .unwrap();
        let (nested, nested_names) = a
            .with_lambda_parameters(&consumer, &string, &schema)
            .unwrap();

        assert_eq!(a_names, vec!["p0"]);
        assert_eq!(b_names, vec!["p0"]);
        assert_eq!(nested_names, vec!["p1"]);
        assert_eq!(
            a.lambda_variable(0, 0).unwrap().get_type(&schema).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            b.lambda_variable(0, 0).unwrap().get_type(&schema).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            nested
                .lambda_variable(0, 0)
                .unwrap()
                .get_type(&schema)
                .unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            nested.lambda_variable(1, 0).unwrap(),
            a.lambda_variable(0, 0).unwrap()
        );
        assert!(root.lambda_variable(0, 0).is_err());
        assert!(a.lambda_variable(1, 0).is_err());
        assert!(b.lambda_variable(1, 0).is_err());
        assert!(nested.lambda_variable(0, 1).is_err());
    }

    // Only input completion is controlled; JOIN/UNION conversion and batch
    // scheduling use the standard consumer implementations.
    struct GatedConsumer {
        consumer: DefaultSubstraitConsumer<'static>,
        a_ready: Semaphore,
        b_ready: Semaphore,
        // Held permits count pending inputs and are released even on cancellation.
        pending: Semaphore,
    }

    impl GatedConsumer {
        fn new(inputs: usize) -> Self {
            Self {
                consumer: test_consumer(),
                a_ready: Semaphore::new(0),
                b_ready: Semaphore::new(0),
                pending: Semaphore::new(inputs),
            }
        }
    }

    #[async_trait]
    impl SubstraitConsumer for GatedConsumer {
        fn get_extensions(&self) -> &Extensions {
            self.consumer.get_extensions()
        }
        fn get_function_registry(&self) -> &impl FunctionRegistry {
            self.consumer.get_function_registry()
        }
        async fn resolve_table_ref(
            &self,
            _: &TableReference,
        ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
            unreachable!("test consumes reads directly")
        }
        async fn consume_rel(
            &self,
            rel: &Rel,
            _context: &SubstraitConsumerContext,
        ) -> datafusion::common::Result<LogicalPlan> {
            let Some(proto::rel::RelType::Read(read)) = &rel.rel_type else {
                panic!("expected read")
            };
            let Some(proto::read_rel::ReadType::NamedTable(table)) = &read.read_type
            else {
                panic!("expected named table")
            };
            let name = table.names[0].as_str();
            let _pending = self.pending.acquire().await.unwrap();
            let gate = if name == "a" {
                &self.a_ready
            } else {
                &self.b_ready
            };
            gate.acquire().await.unwrap().forget();
            if name == "fail" {
                return substrait_err!("input failed");
            }
            table_scan(
                Some(name),
                &Schema::new(vec![Field::new("x", DataType::Int64, false)]),
                None,
            )?
            .build()
        }
    }

    fn read(name: &str) -> Rel {
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

    #[tokio::test]
    async fn join_and_unions_consume_concurrently_in_input_order() {
        let relations = [
            proto::rel::RelType::Join(Box::new(JoinRel {
                left: Some(Box::new(read("a"))),
                right: Some(Box::new(read("b"))),
                r#type: proto::join_rel::JoinType::Inner as i32,
                ..Default::default()
            })),
            proto::rel::RelType::Set(SetRel {
                inputs: vec![read("a"), read("b")],
                op: proto::set_rel::SetOp::UnionAll as i32,
                ..Default::default()
            }),
            proto::rel::RelType::Set(SetRel {
                inputs: vec![read("a"), read("b")],
                op: proto::set_rel::SetOp::UnionDistinct as i32,
                ..Default::default()
            }),
        ];
        for rel_type in relations {
            let consumer = GatedConsumer::new(2);
            let context = SubstraitConsumerContext::default();
            let rel = Rel {
                rel_type: Some(rel_type),
            };
            let mut result = Box::pin(from_substrait_rel(&consumer, &rel, &context));
            assert!(poll!(result.as_mut()).is_pending());
            assert_eq!(consumer.pending.available_permits(), 0);
            consumer.b_ready.add_permits(1); // B finishes first.
            assert!(poll!(result.as_mut()).is_pending());
            assert_eq!(consumer.pending.available_permits(), 1);
            consumer.a_ready.add_permits(1);
            let plan = result.await.unwrap().display_indent().to_string();
            assert!(
                plan.find("TableScan: a").unwrap() < plan.find("TableScan: b").unwrap()
            );
            assert_eq!(consumer.pending.available_permits(), 2);
        }
    }

    #[tokio::test]
    async fn input_failure_cancels_pending_siblings() {
        let consumer = GatedConsumer::new(41);
        // A later failure must not wait for earlier pending inputs, even in a wide UNION.
        let mut inputs = vec![read("a"); 40];
        inputs.push(read("fail"));
        let inputs = inputs.iter().collect::<Vec<_>>();
        let context = SubstraitConsumerContext::default();
        let mut result = Box::pin(consumer.consume_rel_inputs(&inputs, &context));
        assert!(poll!(result.as_mut()).is_pending());
        assert_eq!(consumer.pending.available_permits(), 0);
        consumer.b_ready.add_permits(1);
        assert_contains!(result.await.unwrap_err().to_string(), "input failed");
        assert_eq!(consumer.pending.available_permits(), 41);
    }
}
