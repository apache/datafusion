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
    from_project_rel, from_read_rel, from_scalar_function, from_set_rel,
    from_singular_or_list, from_sort_rel, from_subquery, from_substrait_rel,
    from_substrait_rex, from_window_function,
};
use crate::extensions::Extensions;
use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::catalog::TableProvider;
use datafusion::common::{
    not_impl_err, substrait_err, DFSchema, ScalarValue, TableReference,
};
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan};
use std::sync::Arc;
use substrait::proto;
use substrait::proto::expression as substrait_expression;
use substrait::proto::expression::{
    Enum, FieldReference, IfThen, Literal, MultiOrList, Nested, ScalarFunction,
    SingularOrList, SwitchExpression, WindowFunction,
};
use substrait::proto::{
    r#type, AggregateRel, ConsistentPartitionWindowRel, CrossRel, DynamicParameter,
    ExchangeRel, Expression, ExtensionLeafRel, ExtensionMultiRel, ExtensionSingleRel,
    FetchRel, FilterRel, JoinRel, ProjectRel, ReadRel, Rel, SetRel, SortRel,
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
        _expr: &Nested,
        _input_schema: &DFSchema,
    ) -> datafusion::common::Result<Expr> {
        not_impl_err!("Nested expression not supported")
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
        substrait_err!(
            "Missing handler for user-defined literals {}",
            user_defined_literal.type_reference
        )
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
