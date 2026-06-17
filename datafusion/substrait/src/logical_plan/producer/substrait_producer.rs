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

use crate::extensions::Extensions;
use crate::logical_plan::producer::{
    from_aggregate, from_aggregate_function, from_alias, from_between, from_binary_expr,
    from_case, from_cast, from_column, from_distinct, from_empty_relation, from_exists,
    from_filter, from_higher_order_function, from_in_list, from_in_subquery, from_join,
    from_lambda, from_lambda_variable, from_like, from_limit, from_literal,
    from_placeholder, from_projection, from_repartition, from_scalar_function,
    from_scalar_subquery, from_set_comparison, from_sort, from_subquery_alias,
    from_table_scan, from_try_cast, from_unary_expr, from_union, from_values,
    from_window, from_window_function, to_substrait_rel, to_substrait_rex,
    to_substrait_type_from_field,
};
use datafusion::arrow::datatypes::FieldRef;
use datafusion::common::{
    Column, DFSchemaRef, HashMap, ScalarValue, not_impl_err, substrait_err,
};
use datafusion::execution::SessionState;
use datafusion::execution::registry::SerializerRegistry;
use datafusion::logical_expr::Subquery;
use datafusion::logical_expr::expr::{
    Alias, Exists, InList, InSubquery, Lambda, LambdaVariable, Placeholder,
    SetComparison, WindowFunction,
};
use datafusion::logical_expr::{
    Aggregate, Between, BinaryExpr, Case, Cast, Distinct, EmptyRelation, Expr, Extension,
    Filter, Join, Like, Limit, LogicalPlan, Projection, Repartition, Sort, SubqueryAlias,
    TableScan, TryCast, Union, Values, Window, expr,
};
use pbjson_types::Any as ProtoAny;
use substrait::proto::aggregate_rel::Measure;
use substrait::proto::rel::RelType;
use substrait::proto::{
    Expression, ExtensionLeafRel, ExtensionMultiRel, ExtensionSingleRel, Rel,
};

/// This trait is used to produce Substrait plans, converting them from DataFusion Logical Plans.
/// It can be implemented by users to allow for custom handling of relations, expressions, etc.
///
/// Combined with the [crate::logical_plan::consumer::SubstraitConsumer] this allows for fully
/// customizable Substrait serde.
///
/// # Example Usage
///
/// ```
/// # use std::sync::Arc;
/// # use substrait::proto::{Expression, Rel};
/// # use substrait::proto::rel::RelType;
/// # use datafusion::arrow::datatypes::FieldRef;
/// # use datafusion::common::DFSchemaRef;
/// # use datafusion::error::Result;
/// # use datafusion::execution::SessionState;
/// # use datafusion::logical_expr::{Between, Extension, Projection};
/// # use datafusion_substrait::extensions::Extensions;
/// # use datafusion_substrait::logical_plan::producer::{from_projection, SubstraitProducer, DefaultSubstraitLambdaProducer, lambda_parameters_map};
///
/// struct CustomSubstraitProducer {
///     extensions: Extensions,
///     state: Arc<SessionState>,
///     // You can reuse existing producer code related to lambdas
///     lambda_producer: DefaultSubstraitLambdaProducer,
/// }
///
/// impl SubstraitProducer for CustomSubstraitProducer {
///
///     fn register_function(&mut self, signature: String) -> u32 {
///        self.extensions.register_function(&signature)
///     }
///
///     fn register_type(&mut self, type_name: String) -> u32 {
///         self.extensions.register_type(&type_name)
///     }
///
///     fn get_extensions(self) -> Extensions {
///         self.extensions
///     }
///
///    fn push_lambda_parameters(
///        &mut self,
///        lambda_parameters: Vec<FieldRef>,
///    ) -> datafusion::common::Result<()> {
///        let lambda_parameters_map = lambda_parameters_map(self, lambda_parameters)?;
///
///        self.lambda_producer
///            .push_lambda_parameters(lambda_parameters_map);
///
///        Ok(())
///    }
///
///    fn pop_lambda_parameters(&mut self) -> datafusion::common::Result<()> {
///        self.lambda_producer.pop_lambda_parameters()
///    }
///
///    fn lambda_variable(&self, name: &str) -> datafusion::common::Result<(u32, i32)> {
///        self.lambda_producer.lambda_variable(name)
///    }
///
///    fn lambda_parameter_type(
///        &self,
///        name: &str,
///    ) -> datafusion::common::Result<substrait::proto::Type> {
///        self.lambda_producer.lambda_parameter_type(name)
///    }
///
///     // You can set additional metadata on the Rels you produce
///     fn handle_projection(&mut self, plan: &Projection) -> Result<Box<Rel>> {
///         let mut rel = from_projection(self, plan)?;
///         match rel.rel_type {
///             Some(RelType::Project(mut project)) => {
///                 let mut project = project.clone();
///                 // set common metadata or advanced extension
///                 project.common = None;
///                 project.advanced_extension = None;
///                 Ok(Box::new(Rel {
///                     rel_type: Some(RelType::Project(project)),
///                 }))
///             }
///             rel_type => Ok(Box::new(Rel { rel_type })),
///        }
///     }
///
///     // You can tweak how you convert expressions for your target system
///     fn handle_between(&mut self, between: &Between, schema: &DFSchemaRef) -> Result<Expression> {
///        // add your own encoding for Between
///        todo!()
///    }
///
///     // You can fully control how you convert UserDefinedLogicalNodes into Substrait
///     fn handle_extension(&mut self, _plan: &Extension) -> Result<Box<Rel>> {
///         // implement your own serializer into Substrait
///        todo!()
///    }
/// }
/// ```
pub trait SubstraitProducer: Send + Sync + Sized {
    /// Within a Substrait plan, functions are referenced using function anchors that are stored at
    /// the top level of the [Plan](substrait::proto::Plan) within
    /// [ExtensionFunction](substrait::proto::extensions::simple_extension_declaration::ExtensionFunction)
    /// messages.
    ///
    /// When given a function signature, this method should return the existing anchor for it if
    /// there is one. Otherwise, it should generate a new anchor.
    fn register_function(&mut self, signature: String) -> u32;

    /// Within a Substrait plan, user defined types are referenced using type anchors that are stored at
    /// the top level of the [Plan](substrait::proto::Plan) within
    /// [ExtensionType](substrait::proto::extensions::simple_extension_declaration::ExtensionType)
    /// messages.
    ///
    /// When given a type name, this method should return the existing anchor for it if
    /// there is one. Otherwise, it should generate a new anchor.
    fn register_type(&mut self, name: String) -> u32;

    /// Consume the producer to generate the [Extensions] for the Substrait plan based on the
    /// functions that have been registered
    fn get_extensions(self) -> Extensions;

    // Logical Plan Methods
    // There is one method per LogicalPlan to allow for easy overriding of producer behaviour.
    // These methods have default implementations calling the common handler code, to allow for users
    // to re-use common handling logic.

    fn handle_plan(
        &mut self,
        plan: &LogicalPlan,
    ) -> datafusion::common::Result<Box<Rel>> {
        to_substrait_rel(self, plan)
    }

    fn handle_projection(
        &mut self,
        plan: &Projection,
    ) -> datafusion::common::Result<Box<Rel>> {
        from_projection(self, plan)
    }

    fn handle_filter(&mut self, plan: &Filter) -> datafusion::common::Result<Box<Rel>> {
        from_filter(self, plan)
    }

    fn handle_window(&mut self, plan: &Window) -> datafusion::common::Result<Box<Rel>> {
        from_window(self, plan)
    }

    fn handle_aggregate(
        &mut self,
        plan: &Aggregate,
    ) -> datafusion::common::Result<Box<Rel>> {
        from_aggregate(self, plan)
    }

    fn handle_sort(&mut self, plan: &Sort) -> datafusion::common::Result<Box<Rel>> {
        from_sort(self, plan)
    }

    fn handle_join(&mut self, plan: &Join) -> datafusion::common::Result<Box<Rel>> {
        from_join(self, plan)
    }

    fn handle_repartition(
        &mut self,
        plan: &Repartition,
    ) -> datafusion::common::Result<Box<Rel>> {
        from_repartition(self, plan)
    }

    fn handle_union(&mut self, plan: &Union) -> datafusion::common::Result<Box<Rel>> {
        from_union(self, plan)
    }

    fn handle_table_scan(
        &mut self,
        plan: &TableScan,
    ) -> datafusion::common::Result<Box<Rel>> {
        from_table_scan(self, plan)
    }

    fn handle_empty_relation(
        &mut self,
        plan: &EmptyRelation,
    ) -> datafusion::common::Result<Box<Rel>> {
        from_empty_relation(self, plan)
    }

    fn handle_subquery_alias(
        &mut self,
        plan: &SubqueryAlias,
    ) -> datafusion::common::Result<Box<Rel>> {
        from_subquery_alias(self, plan)
    }

    fn handle_limit(&mut self, plan: &Limit) -> datafusion::common::Result<Box<Rel>> {
        from_limit(self, plan)
    }

    fn handle_values(&mut self, plan: &Values) -> datafusion::common::Result<Box<Rel>> {
        from_values(self, plan)
    }

    fn handle_distinct(
        &mut self,
        plan: &Distinct,
    ) -> datafusion::common::Result<Box<Rel>> {
        from_distinct(self, plan)
    }

    fn handle_extension(
        &mut self,
        _plan: &Extension,
    ) -> datafusion::common::Result<Box<Rel>> {
        substrait_err!(
            "Specify handling for LogicalPlan::Extension by implementing the SubstraitProducer trait"
        )
    }

    // Expression Methods
    // There is one method per DataFusion Expr to allow for easy overriding of producer behaviour
    // These methods have default implementations calling the common handler code, to allow for users
    // to re-use common handling logic.

    fn handle_expr(
        &mut self,
        expr: &Expr,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        to_substrait_rex(self, expr, schema)
    }

    fn handle_alias(
        &mut self,
        alias: &Alias,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_alias(self, alias, schema)
    }

    fn handle_column(
        &mut self,
        column: &Column,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_column(column, schema)
    }

    fn handle_literal(
        &mut self,
        value: &ScalarValue,
    ) -> datafusion::common::Result<Expression> {
        from_literal(self, value)
    }

    fn handle_binary_expr(
        &mut self,
        expr: &BinaryExpr,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_binary_expr(self, expr, schema)
    }

    fn handle_like(
        &mut self,
        like: &Like,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_like(self, like, schema)
    }

    /// For handling Not, IsNotNull, IsNull, IsTrue, IsFalse, IsUnknown, IsNotTrue, IsNotFalse, IsNotUnknown, Negative
    fn handle_unary_expr(
        &mut self,
        expr: &Expr,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_unary_expr(self, expr, schema)
    }

    fn handle_between(
        &mut self,
        between: &Between,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_between(self, between, schema)
    }

    fn handle_case(
        &mut self,
        case: &Case,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_case(self, case, schema)
    }

    fn handle_cast(
        &mut self,
        cast: &Cast,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_cast(self, cast, schema)
    }

    fn handle_try_cast(
        &mut self,
        cast: &TryCast,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_try_cast(self, cast, schema)
    }

    fn handle_scalar_function(
        &mut self,
        scalar_fn: &expr::ScalarFunction,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_scalar_function(self, scalar_fn, schema)
    }

    fn handle_higher_order_function(
        &mut self,
        scalar_fn: &expr::HigherOrderFunction,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_higher_order_function(self, scalar_fn, schema)
    }

    fn handle_aggregate_function(
        &mut self,
        agg_fn: &expr::AggregateFunction,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Measure> {
        from_aggregate_function(self, agg_fn, schema)
    }

    fn handle_window_function(
        &mut self,
        window_fn: &WindowFunction,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_window_function(self, window_fn, schema)
    }

    fn handle_in_list(
        &mut self,
        in_list: &InList,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_in_list(self, in_list, schema)
    }

    fn handle_in_subquery(
        &mut self,
        in_subquery: &InSubquery,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_in_subquery(self, in_subquery, schema)
    }

    fn handle_set_comparison(
        &mut self,
        set_comparison: &SetComparison,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_set_comparison(self, set_comparison, schema)
    }
    fn handle_scalar_subquery(
        &mut self,
        subquery: &Subquery,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_scalar_subquery(self, subquery, schema)
    }

    fn handle_exists(
        &mut self,
        exists: &Exists,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_exists(self, exists, schema)
    }

    fn handle_placeholder(
        &mut self,
        placeholder: &Placeholder,
        _schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_placeholder(self, placeholder)
    }

    fn handle_lambda(
        &mut self,
        lambda: &Lambda,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_lambda(self, lambda, schema)
    }

    fn handle_lambda_variable(
        &mut self,
        lambda_variable: &LambdaVariable,
        schema: &DFSchemaRef,
    ) -> datafusion::common::Result<Expression> {
        from_lambda_variable(self, lambda_variable, schema)
    }

    // Lambda related methods

    /// Push the given `lambda_parameters` into this producer so they can be referenced by lambda variables
    ///
    /// Note for custom implementations it's possible to embed a [DefaultSubstraitLambdaProducer] and forward this method to it
    fn push_lambda_parameters(
        &mut self,
        _lambda_parameters: Vec<FieldRef>,
    ) -> datafusion::common::Result<()> {
        not_impl_err!("SubstraitProducer::push_lambda_parameters")
    }

    /// Pop the last pushed `lambda_parameters` so that it unshadow any previously shadowed lambda parameter
    ///
    /// Note for custom implementations it's possible to embed a [DefaultSubstraitLambdaProducer] and forward this method to it
    fn pop_lambda_parameters(&mut self) -> datafusion::common::Result<()> {
        not_impl_err!("SubstraitProducer::pop_lambda_parameters")
    }

    /// Get the (`steps_out`, `field_idx`) of the lambda variable with the given `name`. `steps_out` refers to the number
    /// of lambda boundaries to traverse (0 = current lambda), and `field_idx` refers to the index within the lambda parameters
    ///
    /// Note for custom implementations it's possible to embed a [DefaultSubstraitLambdaProducer] and forward this method to it
    fn lambda_variable(&self, _name: &str) -> datafusion::common::Result<(u32, i32)> {
        not_impl_err!("SubstraitProducer::lambda_variable")
    }

    /// Get the type of the lambda parameter with the given `name`
    ///
    /// Note for custom implementations it's possible to embed a [DefaultSubstraitLambdaProducer] and forward this method to it
    fn lambda_parameter_type(
        &self,
        _name: &str,
    ) -> datafusion::common::Result<substrait::proto::Type> {
        not_impl_err!("SubstraitProducer::lambda_parameter_type")
    }
}

pub struct DefaultSubstraitProducer<'a> {
    extensions: Extensions,
    serializer_registry: &'a dyn SerializerRegistry,
    lambda_producer: DefaultSubstraitLambdaProducer,
}

impl<'a> DefaultSubstraitProducer<'a> {
    pub fn new(state: &'a SessionState) -> Self {
        DefaultSubstraitProducer {
            extensions: Extensions::default(),
            serializer_registry: state.serializer_registry().as_ref(),
            lambda_producer: DefaultSubstraitLambdaProducer::new(),
        }
    }
}

impl SubstraitProducer for DefaultSubstraitProducer<'_> {
    fn register_function(&mut self, fn_name: String) -> u32 {
        self.extensions.register_function(&fn_name)
    }

    fn register_type(&mut self, type_name: String) -> u32 {
        self.extensions.register_type(&type_name)
    }

    fn get_extensions(self) -> Extensions {
        self.extensions
    }

    fn handle_extension(
        &mut self,
        plan: &Extension,
    ) -> datafusion::common::Result<Box<Rel>> {
        let extension_bytes = self
            .serializer_registry
            .serialize_logical_plan(plan.node.as_ref())?;
        let detail = ProtoAny {
            type_url: plan.node.name().to_string(),
            value: extension_bytes.into(),
        };
        let mut inputs_rel = plan
            .node
            .inputs()
            .into_iter()
            .map(|plan| self.handle_plan(plan))
            .collect::<datafusion::common::Result<Vec<_>>>()?;
        let rel_type = match inputs_rel.len() {
            0 => RelType::ExtensionLeaf(ExtensionLeafRel {
                common: None,
                detail: Some(detail),
            }),
            1 => RelType::ExtensionSingle(Box::new(ExtensionSingleRel {
                common: None,
                detail: Some(detail),
                input: Some(inputs_rel.pop().unwrap()),
            })),
            _ => RelType::ExtensionMulti(ExtensionMultiRel {
                common: None,
                detail: Some(detail),
                inputs: inputs_rel.into_iter().map(|r| *r).collect(),
            }),
        };
        Ok(Box::new(Rel {
            rel_type: Some(rel_type),
        }))
    }

    fn push_lambda_parameters(
        &mut self,
        lambda_parameters: Vec<FieldRef>,
    ) -> datafusion::common::Result<()> {
        let lambda_parameters_map = lambda_parameters_map(self, lambda_parameters)?;

        self.lambda_producer
            .push_lambda_parameters(lambda_parameters_map);

        Ok(())
    }

    fn pop_lambda_parameters(&mut self) -> datafusion::common::Result<()> {
        self.lambda_producer.pop_lambda_parameters()
    }

    fn lambda_variable(&self, name: &str) -> datafusion::common::Result<(u32, i32)> {
        self.lambda_producer.lambda_variable(name)
    }

    fn lambda_parameter_type(
        &self,
        name: &str,
    ) -> datafusion::common::Result<substrait::proto::Type> {
        self.lambda_producer.lambda_parameter_type(name)
    }
}

/// Default implementation of lambda related methods of the [SubstraitProducer] trait
///
/// Can be embedded into a custom [SubstraitProducer] to implement them
pub struct DefaultSubstraitLambdaProducer {
    lambdas_variables: Vec<HashMap<String, (usize, substrait::proto::Type)>>,
}

impl Default for DefaultSubstraitLambdaProducer {
    fn default() -> Self {
        Self::new()
    }
}

impl DefaultSubstraitLambdaProducer {
    pub fn new() -> Self {
        Self {
            lambdas_variables: Vec::new(),
        }
    }

    /// Note you can construct the `lambda_parameters` argument using [lambda_parameters_map]
    pub fn push_lambda_parameters(
        &mut self,
        lambda_parameters: HashMap<String, (usize, substrait::proto::Type)>,
    ) {
        self.lambdas_variables.push(lambda_parameters);
    }

    pub fn pop_lambda_parameters(&mut self) -> datafusion::common::Result<()> {
        match self.lambdas_variables.pop() {
            Some(_) => Ok(()),
            None => substrait_err!("no lambda_parameters to pop"),
        }
    }

    pub fn lambda_variable(&self, name: &str) -> datafusion::common::Result<(u32, i32)> {
        for (steps_out, lambda_parameters) in
            self.lambdas_variables.iter().rev().enumerate()
        {
            if let Some((field_idx, _type)) = lambda_parameters.get(name) {
                return Ok((steps_out as u32, *field_idx as i32));
            }
        }

        substrait_err!("unknown lambda variable {name}")
    }

    pub fn lambda_parameter_type(
        &self,
        name: &str,
    ) -> datafusion::common::Result<substrait::proto::Type> {
        for lambda_parameters in self.lambdas_variables.iter().rev() {
            if let Some((_field_idx, type_)) = lambda_parameters.get(name) {
                return Ok(type_.clone());
            }
        }

        substrait_err!("unknown lambda variable {name}")
    }
}

/// Produces a map of lambda parameters as expected by [DefaultSubstraitLambdaProducer::push_lambda_parameters]
pub fn lambda_parameters_map(
    producer: &mut impl SubstraitProducer,
    lambda_parameters: Vec<FieldRef>,
) -> datafusion::common::Result<HashMap<String, (usize, substrait::proto::Type)>> {
    lambda_parameters
        .into_iter()
        .enumerate()
        .map(|(field_idx, field)| {
            Ok((
                field.name().clone(),
                (field_idx, to_substrait_type_from_field(producer, &field)?),
            ))
        })
        .collect::<datafusion::common::Result<_>>()
}
