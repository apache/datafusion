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
    DFSchema, ScalarValue, TableReference, internal_datafusion_err, not_impl_err,
    substrait_err,
};
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::logical_expr::expr::LambdaVariable;
use datafusion::logical_expr::{Expr, Extension, LogicalPlan};
use futures::TryStreamExt;
use futures::stream::FuturesUnordered;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
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
use tokio::sync::{OnceCell, Semaphore};

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
/// # use substrait::proto::{ExtensionLeafRel, FilterRel, ProjectRel, Type};
/// # use datafusion::arrow::datatypes::DataType;
/// # use datafusion::logical_expr::expr::ScalarFunction;
/// # use datafusion_substrait::extensions::Extensions;
/// # use datafusion_substrait::logical_plan::consumer::{
/// #     from_project_rel, from_substrait_rel, from_substrait_rex, SubstraitConsumer, DefaultSubstraitLambdaConsumer
/// # };
///
/// struct CustomSubstraitConsumer {
///     extensions: Arc<Extensions>,
///     state: Arc<SessionState>,
///     // You can reuse existing consumer code related to lambdas
///     lambda_consumer: DefaultSubstraitLambdaConsumer,
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
///     fn push_lambda_parameters(
///        &self,
///        lambda_parameters: &[Type],
///        input_schema: &DFSchema,
///    ) -> datafusion::common::Result<Vec<String>> {
///        self.lambda_consumer.push_lambda_parameters(
///            self,
///            lambda_parameters,
///            input_schema,
///        )
///    }
///
///     fn pop_lambda_parameters(&self) {
///        self.lambda_consumer.pop_lambda_parameters();
///    }
///
///    fn lambda_variable(
///        &self,
///        steps_out: usize,
///        field_idx: usize,
///    ) -> datafusion::common::Result<Expr> {
///        self.lambda_consumer.lambda_variable(steps_out, field_idx)
///    }
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

    /// Consumes independent inputs of a JOIN or UNION, returning plans in input order.
    ///
    /// Implementations must return exactly one plan per input, in the same order.
    /// On error, implementations must drop any unfinished input futures.
    ///
    /// The default preserves sequential consumption. Implementations may override it
    /// when they can isolate mutable traversal state between inputs.
    async fn consume_rel_inputs(
        &self,
        rels: &[&Rel],
    ) -> datafusion::common::Result<Vec<LogicalPlan>> {
        let mut plans = Vec::with_capacity(rels.len());
        for rel in rels {
            plans.push(self.consume_rel(rel).await?);
        }
        Ok(plans)
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
        expr: &DynamicParameter,
        _input_schema: &DFSchema,
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
    ) -> datafusion::common::Result<Expr> {
        from_lambda(self, expr, input_schema).await
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

    // Lambda related methods

    /// Push the given lambda parameters onto the stack when entering a lambda and
    /// returns the names they got assigned
    ///
    /// Note for custom implementations it's possible to embed a [DefaultSubstraitLambdaConsumer] and forward this method to it
    fn push_lambda_parameters(
        &self,
        _lambda_parameters: &[Type],
        _input_schema: &DFSchema,
    ) -> datafusion::common::Result<Vec<String>> {
        not_impl_err!("SubstraitConsumer::push_lambda_parameters")
    }

    /// Pop lambda parameters from the stack when leaving a lambda.
    fn pop_lambda_parameters(&self) {}

    /// Returns an expression corresponding to the lambda variable with the given field_idx within the lambda it originates from,
    /// at the lambda `step_outs` of the current scope
    ///
    /// Note for custom implementations it's possible to embed a [DefaultSubstraitLambdaConsumer] and forward this method to it
    fn lambda_variable(
        &self,
        _steps_out: usize,
        _field_idx: usize,
    ) -> datafusion::common::Result<Expr> {
        not_impl_err!("SubstraitConsumer::lambda_variable")
    }
}

const MAX_CONCURRENT_TABLE_LOADS: usize = 32;

type TableCell = Arc<OnceCell<Option<Arc<dyn TableProvider>>>>;

/// Table resolutions are shared across a consumer tree; traversal scopes are not.
struct TableResolutionState {
    providers: RwLock<HashMap<TableReference, TableCell>>,
    loads: Semaphore,
}

/// Default SubstraitConsumer for converting standard Substrait without user-defined extensions.
///
/// Used as the consumer in [crate::logical_plan::consumer::from_substrait_plan].
/// Independent JOIN and UNION inputs are consumed concurrently, with at most 32
/// catalog table loads in flight across the consumer tree. Successful resolutions
/// (including missing tables) are reused within that tree. Errors are not cached.
/// A failing lookup waiting for a permit can report its error only after admission.
pub struct DefaultSubstraitConsumer<'a> {
    pub(super) extensions: &'a Extensions,
    pub(super) state: &'a SessionState,
    outer_schemas: RwLock<Vec<Arc<DFSchema>>>,
    lambda_consumer: DefaultSubstraitLambdaConsumer,
    tables: Arc<TableResolutionState>,
}

impl<'a> DefaultSubstraitConsumer<'a> {
    pub fn new(extensions: &'a Extensions, state: &'a SessionState) -> Self {
        DefaultSubstraitConsumer {
            extensions,
            state,
            outer_schemas: RwLock::new(Vec::new()),
            lambda_consumer: DefaultSubstraitLambdaConsumer::new(),
            tables: Arc::new(TableResolutionState {
                providers: RwLock::default(),
                loads: Semaphore::new(MAX_CONCURRENT_TABLE_LOADS),
            }),
        }
    }

    // Siblings inherit the enclosing lexical scope but must not push/pop each
    // other's stacks. Only catalog resolution state is shared. Keep this field
    // list explicit so new consumer state requires a sharing decision.
    fn fork(&self) -> Self {
        Self {
            extensions: self.extensions,
            state: self.state,
            outer_schemas: RwLock::new(self.outer_schemas.read().unwrap().clone()),
            lambda_consumer: self.lambda_consumer.fork(),
            tables: Arc::clone(&self.tables),
        }
    }
}

#[async_trait]
impl SubstraitConsumer for DefaultSubstraitConsumer<'_> {
    async fn resolve_table_ref(
        &self,
        table_ref: &TableReference,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let cell = Arc::clone(
            self.tables
                .providers
                .write()
                .unwrap()
                .entry(table_ref.clone())
                .or_default(),
        );
        cell.get_or_try_init(|| async {
            let schema = self.state.schema_for_ref(table_ref.clone())?;
            // Only one lookup per table takes a permit; nested relations hold none.
            let _permit = self.tables.loads.acquire().await.map_err(|error| {
                internal_datafusion_err!("Table load limiter closed: {error}")
            })?;
            schema.table(table_ref.table()).await
        })
        .await
        .cloned()
    }

    async fn consume_rel_inputs(
        &self,
        rels: &[&Rel],
    ) -> datafusion::common::Result<Vec<LogicalPlan>> {
        let pending = FuturesUnordered::new();
        for (index, rel) in rels.iter().copied().enumerate() {
            let branch = self.fork();
            pending.push(async move {
                branch.consume_rel(rel).await.map(|plan| (index, plan))
            });
        }
        let mut completed = pending.try_collect::<Vec<_>>().await?;
        completed.sort_unstable_by_key(|(index, _)| *index);
        Ok(completed.into_iter().map(|(_, plan)| plan).collect())
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

    fn push_lambda_parameters(
        &self,
        lambda_parameters: &[Type],
        input_schema: &DFSchema,
    ) -> datafusion::common::Result<Vec<String>> {
        self.lambda_consumer
            .push_lambda_parameters(self, lambda_parameters, input_schema)
    }

    fn pop_lambda_parameters(&self) {
        self.lambda_consumer.pop_lambda_parameters()
    }

    fn lambda_variable(
        &self,
        steps_out: usize,
        field_idx: usize,
    ) -> datafusion::common::Result<Expr> {
        self.lambda_consumer.lambda_variable(steps_out, field_idx)
    }
}

/// Default implementation of lambda related methods of the [SubstraitConsumer] trait
///
/// Can be embedded into a custom [SubstraitConsumer] to implement them
pub struct DefaultSubstraitLambdaConsumer {
    inner: RwLock<DefaultSubstraitLambdaConsumerInner>,
}

#[derive(Clone)]
struct DefaultSubstraitLambdaConsumerInner {
    /// Parameters of the lambdas currently in scope, ordered from innermost
    /// to outermost. Index 0 is the lambda being consumed; higher indices
    /// are enclosing lambdas, matching the `steps_out` value used by
    /// [`DefaultSubstraitLambdaConsumer::lambda_variable`] and `LambdaParameterReference`.
    lambda_parameters: VecDeque<Vec<FieldRef>>,
    next_lambda_parameter: usize,
}

impl Default for DefaultSubstraitLambdaConsumer {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultSubstraitLambdaConsumer {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(DefaultSubstraitLambdaConsumerInner {
                lambda_parameters: VecDeque::new(),
                next_lambda_parameter: 0,
            }),
        }
    }

    fn fork(&self) -> Self {
        Self {
            inner: RwLock::new(self.inner.read().unwrap().clone()),
        }
    }

    pub fn push_lambda_parameters(
        &self,
        consumer: &impl SubstraitConsumer,
        lambda_parameters: &[Type],
        input_schema: &DFSchema,
    ) -> datafusion::common::Result<Vec<String>> {
        let mut inner = self.inner.write().unwrap();

        let lambda_parameters = lambda_parameters
            .iter()
            .map(|ty| {
                let (assigned_number, default_name) =
                    next_lambda_parameter_name(inner.next_lambda_parameter, input_schema);

                inner.next_lambda_parameter = assigned_number + 1;

                Ok(field_from_substrait_type_without_names(consumer, ty)?
                    .renamed(&default_name))
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?;

        let names = lambda_parameters.iter().map(|f| f.name().clone()).collect();

        inner.lambda_parameters.push_front(lambda_parameters);

        Ok(names)
    }

    pub fn pop_lambda_parameters(&self) {
        self.inner.write().unwrap().lambda_parameters.pop_front();
    }

    pub fn lambda_variable(
        &self,
        steps_out: usize,
        field_idx: usize,
    ) -> datafusion::common::Result<Expr> {
        let lambda_parameters = &self.inner.read().unwrap().lambda_parameters;

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
    use datafusion::catalog::empty::EmptyTable;
    use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider, SchemaProvider};
    use datafusion::common::{Result, exec_err};
    use datafusion::execution::SessionStateBuilder;
    use futures::poll;
    use futures::task::{ArcWake, waker_ref};
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::task::{Context, Poll};
    use substrait::proto::read_rel::{NamedTable, ReadType};
    use substrait::proto::rel::RelType;
    use substrait::proto::set_rel::SetOp;
    use substrait::proto::{NamedStruct, r#type};

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

    #[derive(Default)]
    struct WakeFlag(AtomicBool);

    impl ArcWake for WakeFlag {
        fn wake_by_ref(flag: &Arc<Self>) {
            flag.0.store(true, Ordering::SeqCst);
        }
    }

    async fn poll_until_stalled<F: Future>(mut future: Pin<&mut F>) -> Poll<F::Output> {
        let flag = Arc::new(WakeFlag::default());
        loop {
            flag.0.store(false, Ordering::SeqCst);
            let result = future
                .as_mut()
                .poll(&mut Context::from_waker(&waker_ref(&flag)));
            if result.is_ready() || !flag.0.load(Ordering::SeqCst) {
                return result;
            }
            tokio::task::yield_now().await;
        }
    }

    #[derive(Debug)]
    struct GatedSchema {
        started: Mutex<Vec<String>>,
        ready: Semaphore,
    }

    #[async_trait]
    impl SchemaProvider for GatedSchema {
        fn table_names(&self) -> Vec<String> {
            vec![]
        }

        fn table_exist(&self, _name: &str) -> bool {
            true
        }

        async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
            self.started.lock().unwrap().push(name.to_owned());
            if name == "fail" {
                return exec_err!("catalog load failed");
            }
            self.ready.acquire().await.unwrap().forget();
            let schema = Schema::new(vec![Field::new("x", DataType::Int64, false)]);
            Ok(Some(Arc::new(EmptyTable::new(Arc::new(schema)))))
        }
    }

    fn gated_state() -> (SessionState, Arc<GatedSchema>) {
        let schema = Arc::new(GatedSchema {
            started: Mutex::new(vec![]),
            ready: Semaphore::new(0),
        });
        let catalog = MemoryCatalogProvider::new();
        catalog.register_schema("public", schema.clone()).unwrap();
        let state = SessionStateBuilder::new_with_default_features().build();
        state
            .catalog_list()
            .register_catalog("datafusion".into(), Arc::new(catalog));
        (state, schema)
    }

    fn read(name: &str) -> Rel {
        let ty = Type {
            kind: Some(r#type::Kind::I64(r#type::I64 {
                nullability: r#type::Nullability::Required as i32,
                ..Default::default()
            })),
        };
        Rel {
            rel_type: Some(RelType::Read(Box::new(ReadRel {
                base_schema: Some(NamedStruct {
                    names: vec!["x".into()],
                    r#struct: Some(r#type::Struct {
                        types: vec![ty],
                        nullability: r#type::Nullability::Required as i32,
                        ..Default::default()
                    }),
                }),
                read_type: Some(ReadType::NamedTable(NamedTable {
                    names: vec![name.into()],
                    ..Default::default()
                })),
                ..Default::default()
            }))),
        }
    }

    #[tokio::test]
    async fn join_starts_both_table_loads_before_either_finishes() {
        let (state, schema) = gated_state();
        let extensions = Extensions::default();
        let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
        let join = JoinRel {
            left: Some(Box::new(read("a"))),
            right: Some(Box::new(read("b"))),
            r#type: proto::join_rel::JoinType::Inner as i32,
            ..Default::default()
        };
        let mut pending = Box::pin(consumer.consume_join(&join));
        assert!(poll_until_stalled(pending.as_mut()).await.is_pending());
        assert_eq!(*schema.started.lock().unwrap(), ["a", "b"]);
        schema.ready.add_permits(2);
        assert!(matches!(pending.await.unwrap(), LogicalPlan::Join(_)));
    }

    #[tokio::test]
    async fn wide_union_bounds_table_loads_and_keeps_input_order() {
        let (state, schema) = gated_state();
        let extensions = Extensions::default();
        let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
        let set = SetRel {
            inputs: (0..40).map(|i| read(&format!("table{i}"))).collect(),
            op: SetOp::UnionAll as i32,
            ..Default::default()
        };
        let mut pending = Box::pin(consumer.consume_set(&set));
        assert!(poll_until_stalled(pending.as_mut()).await.is_pending());
        assert_eq!(
            schema.started.lock().unwrap().len(),
            MAX_CONCURRENT_TABLE_LOADS
        );
        schema.ready.add_permits(40);
        fn table_names(plan: &LogicalPlan, names: &mut Vec<String>) {
            match plan {
                LogicalPlan::Union(union) => {
                    for input in &union.inputs {
                        table_names(input, names);
                    }
                }
                LogicalPlan::TableScan(scan) => {
                    names.push(scan.table_name.table().to_owned())
                }
                other => panic!("expected a union or table scan, got {other:?}"),
            }
        }
        let plan = pending.await.unwrap();
        assert_eq!(schema.started.lock().unwrap().len(), 40);
        let mut names = vec![];
        table_names(&plan, &mut names);
        assert_eq!(
            names,
            (0..40).map(|i| format!("table{i}")).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn nested_unions_share_the_table_load_limit() {
        let (state, schema) = gated_state();
        let extensions = Extensions::default();
        let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
        let set = SetRel {
            inputs: (0..40)
                .map(|i| Rel {
                    rel_type: Some(RelType::Set(SetRel {
                        inputs: vec![read(&format!("a{i}")), read(&format!("b{i}"))],
                        op: SetOp::UnionAll as i32,
                        ..Default::default()
                    })),
                })
                .collect(),
            op: SetOp::UnionAll as i32,
            ..Default::default()
        };
        let mut pending = Box::pin(consumer.consume_set(&set));
        assert!(poll_until_stalled(pending.as_mut()).await.is_pending());
        assert_eq!(
            schema.started.lock().unwrap().len(),
            MAX_CONCURRENT_TABLE_LOADS
        );
        schema.ready.add_permits(80);
        assert!(matches!(pending.await.unwrap(), LogicalPlan::Union(_)));
        assert_eq!(schema.started.lock().unwrap().len(), 80);
    }

    #[tokio::test]
    async fn repeated_table_loads_are_shared_across_union_inputs() {
        let (state, schema) = gated_state();
        let extensions = Extensions::default();
        let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
        let set = SetRel {
            inputs: vec![read("a"), read("a")],
            op: SetOp::UnionAll as i32,
            ..Default::default()
        };
        let mut pending = Box::pin(consumer.consume_set(&set));
        assert!(poll_until_stalled(pending.as_mut()).await.is_pending());
        assert_eq!(*schema.started.lock().unwrap(), ["a"]);
        schema.ready.add_permits(1);
        assert!(matches!(pending.await.unwrap(), LogicalPlan::Union(_)));
        assert_eq!(*schema.started.lock().unwrap(), ["a"]);
    }

    #[tokio::test]
    async fn union_error_cancels_pending_siblings_and_allows_retry() {
        let (state, schema) = gated_state();
        let extensions = Extensions::default();
        let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
        let set = SetRel {
            inputs: vec![read("pending"), read("fail")],
            op: SetOp::UnionAll as i32,
            ..Default::default()
        };
        let mut pending = Box::pin(consumer.consume_set(&set));
        let Poll::Ready(Err(error)) = poll!(&mut pending) else {
            panic!("the later failure must not wait for the first input");
        };
        assert!(error.to_string().contains("catalog load failed"));
        assert_eq!(*schema.started.lock().unwrap(), ["pending", "fail"]);
        drop(pending);
        schema.ready.add_permits(1);
        assert!(
            consumer
                .resolve_table_ref(&TableReference::bare("pending"))
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(
            *schema.started.lock().unwrap(),
            ["pending", "fail", "pending"]
        );
    }

    #[tokio::test]
    async fn queued_failure_is_reported_after_a_permit_becomes_available() {
        let (state, schema) = gated_state();
        let extensions = Extensions::default();
        let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
        let mut inputs = (0..MAX_CONCURRENT_TABLE_LOADS)
            .map(|i| read(&format!("pending{i}")))
            .collect::<Vec<_>>();
        inputs.push(read("fail"));
        let set = SetRel {
            inputs,
            op: SetOp::UnionAll as i32,
            ..Default::default()
        };
        let mut pending = Box::pin(consumer.consume_set(&set));
        assert!(poll_until_stalled(pending.as_mut()).await.is_pending());
        assert_eq!(
            schema.started.lock().unwrap().len(),
            MAX_CONCURRENT_TABLE_LOADS
        );
        // One completion admits the failing lookup. The remaining pending reads
        // must then be cancelled without needing their catalog responses.
        schema.ready.add_permits(1);
        let Poll::Ready(Err(error)) = poll_until_stalled(pending.as_mut()).await else {
            panic!("an admitted failure must cancel pending siblings");
        };
        assert!(error.to_string().contains("catalog load failed"));
    }

    #[tokio::test]
    async fn cancelled_initialization_can_be_retried_by_a_duplicate() {
        let (state, schema) = gated_state();
        let extensions = Extensions::default();
        let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
        let table = TableReference::bare("a");
        let mut owner = Box::pin(consumer.resolve_table_ref(&table));
        let mut waiter = Box::pin(consumer.resolve_table_ref(&table));
        assert!(poll!(&mut owner).is_pending());
        assert!(poll!(&mut waiter).is_pending());
        assert_eq!(*schema.started.lock().unwrap(), ["a"]);
        drop(owner);
        assert!(poll!(&mut waiter).is_pending());
        assert_eq!(*schema.started.lock().unwrap(), ["a", "a"]);
        schema.ready.add_permits(1);
        assert!(waiter.await.unwrap().is_some());
    }

    #[test]
    fn fork_keeps_outer_and_lambda_scopes_independent() {
        let state = SessionStateBuilder::new_with_default_features().build();
        let extensions = Extensions::default();
        let consumer = DefaultSubstraitConsumer::new(&extensions, &state);
        consumer.push_outer_schema(make_schema(&[("outer", DataType::Int64)]));
        let ty = Type {
            kind: Some(r#type::Kind::I64(r#type::I64::default())),
        };
        consumer
            .push_lambda_parameters(&[ty], &DFSchema::empty())
            .unwrap();
        let fork = consumer.fork();
        assert_eq!(fork.get_outer_schema(1), consumer.get_outer_schema(1));
        assert_eq!(
            fork.lambda_variable(0, 0).unwrap(),
            consumer.lambda_variable(0, 0).unwrap()
        );
        fork.pop_outer_schema();
        fork.pop_lambda_parameters();
        assert!(consumer.get_outer_schema(1).is_some());
        assert!(consumer.lambda_variable(0, 0).is_ok());
    }
    // A custom consumer exercises the trait default independently of the default
    // consumer's scheduling policy.
    struct InputConsumer {
        extensions: Extensions,
        state: SessionState,
        reads: Mutex<Vec<String>>,
        ready: Semaphore,
    }

    #[async_trait]
    impl SubstraitConsumer for InputConsumer {
        fn get_extensions(&self) -> &Extensions {
            &self.extensions
        }

        fn get_function_registry(&self) -> &impl FunctionRegistry {
            &self.state
        }

        async fn resolve_table_ref(
            &self,
            _: &TableReference,
        ) -> Result<Option<Arc<dyn TableProvider>>> {
            unreachable!("test consumes reads directly")
        }

        async fn consume_rel(&self, rel: &Rel) -> Result<LogicalPlan> {
            let Some(RelType::Read(read)) = &rel.rel_type else {
                panic!("expected a read")
            };
            let Some(ReadType::NamedTable(table)) = &read.read_type else {
                panic!("expected a named table")
            };
            self.reads.lock().unwrap().push(table.names[0].clone());
            self.ready.acquire().await.unwrap().forget();
            if table.names[0] == "fail" {
                return substrait_err!("input failed");
            }
            let provider = EmptyTable::new(Arc::new(Schema::new(vec![Field::new(
                "x",
                DataType::Int64,
                false,
            )])));
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
            extensions: Extensions::default(),
            state: SessionStateBuilder::new_with_default_features().build(),
            reads: Mutex::default(),
            ready: Semaphore::new(0),
        }
    }

    fn input_read(name: &str) -> Rel {
        Rel {
            rel_type: Some(RelType::Read(Box::new(ReadRel {
                read_type: Some(ReadType::NamedTable(NamedTable {
                    names: vec![name.into()],
                    ..Default::default()
                })),
                ..Default::default()
            }))),
        }
    }

    #[tokio::test]
    async fn input_hook_default_is_sequential_and_stops_on_error() {
        use std::future::{Future, poll_fn};
        use std::task::Poll;
        let consumer = input_consumer();
        let rels = [input_read("a"), input_read("fail"), input_read("unreached")];
        let inputs = rels.iter().collect::<Vec<_>>();
        let mut pending = Box::pin(consumer.consume_rel_inputs(&inputs));
        poll_fn(|cx| {
            assert!(pending.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        assert_eq!(*consumer.reads.lock().unwrap(), ["a"]);
        consumer.ready.add_permits(3);
        assert!(
            pending
                .await
                .unwrap_err()
                .to_string()
                .contains("input failed")
        );
        assert_eq!(*consumer.reads.lock().unwrap(), ["a", "fail"]);
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
            &self.inner.state
        }
        async fn resolve_table_ref(
            &self,
            _: &TableReference,
        ) -> Result<Option<Arc<dyn TableProvider>>> {
            unreachable!("test consumes reads directly")
        }
        async fn consume_rel_inputs(&self, rels: &[&Rel]) -> Result<Vec<LogicalPlan>> {
            self.calls.lock().unwrap().push(rels.len());
            let mut plans = self.inner.consume_rel_inputs(rels).await?;
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
            let result = consumer.consume_join(&join).await;
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
            for op in [SetOp::UnionAll, SetOp::UnionDistinct] {
                let set = SetRel {
                    inputs: vec![input_read("a"), input_read("b")],
                    op: op as i32,
                    ..Default::default()
                };
                let result = consumer.consume_set(&set).await;
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
