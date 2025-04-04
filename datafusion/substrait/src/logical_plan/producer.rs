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
use substrait::proto::expression_reference::ExprType;

use datafusion::arrow::datatypes::{Field, IntervalUnit};
use datafusion::logical_expr::{
    Aggregate, Distinct, EmptyRelation, Extension, Filter, Join, Like, Limit,
    Partitioning, Projection, Repartition, Sort, SortExpr, SubqueryAlias, TableScan,
    TryCast, Union, Values, Window, WindowFrameUnits,
};
use datafusion::{
    arrow::datatypes::{DataType, TimeUnit},
    error::{DataFusionError, Result},
    logical_expr::{WindowFrame, WindowFrameBound},
    prelude::JoinType,
    scalar::ScalarValue,
};

use crate::extensions::Extensions;
use crate::variation_const::{
    DATE_32_TYPE_VARIATION_REF, DATE_64_TYPE_VARIATION_REF,
    DECIMAL_128_TYPE_VARIATION_REF, DECIMAL_256_TYPE_VARIATION_REF,
    DEFAULT_CONTAINER_TYPE_VARIATION_REF, DEFAULT_TYPE_VARIATION_REF,
    LARGE_CONTAINER_TYPE_VARIATION_REF, UNSIGNED_INTEGER_TYPE_VARIATION_REF,
    VIEW_CONTAINER_TYPE_VARIATION_REF,
};
use datafusion::arrow::array::{Array, GenericListArray, OffsetSizeTrait};
use datafusion::arrow::temporal_conversions::NANOSECONDS;
use datafusion::common::{
    exec_err, internal_err, not_impl_err, plan_err, substrait_datafusion_err,
    substrait_err, Column, DFSchema, DFSchemaRef, ToDFSchema,
};
use datafusion::execution::registry::SerializerRegistry;
use datafusion::execution::SessionState;
use datafusion::logical_expr::expr::{
    AggregateFunctionParams, Alias, BinaryExpr, Case, Cast, GroupingSet, InList,
    InSubquery, WindowFunction, WindowFunctionParams,
};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{expr, Between, JoinConstraint, LogicalPlan, Operator};
use datafusion::prelude::Expr;
use pbjson_types::Any as ProtoAny;
use substrait::proto::exchange_rel::{ExchangeKind, RoundRobin, ScatterFields};
use substrait::proto::expression::cast::FailureBehavior;
use substrait::proto::expression::field_reference::{RootReference, RootType};
use substrait::proto::expression::literal::interval_day_to_second::PrecisionMode;
use substrait::proto::expression::literal::map::KeyValue;
use substrait::proto::expression::literal::{
    IntervalCompound, IntervalDayToSecond, IntervalYearToMonth, List, Map,
    PrecisionTimestamp, Struct,
};
use substrait::proto::expression::subquery::InPredicate;
use substrait::proto::expression::window_function::BoundsType;
use substrait::proto::expression::ScalarFunction;
use substrait::proto::read_rel::VirtualTable;
use substrait::proto::rel_common::EmitKind;
use substrait::proto::rel_common::EmitKind::Emit;
use substrait::proto::{
    fetch_rel, rel_common, ExchangeRel, ExpressionReference, ExtendedExpression,
    RelCommon,
};
use substrait::{
    proto::{
        aggregate_function::AggregationInvocation,
        aggregate_rel::{Grouping, Measure},
        expression::{
            field_reference::ReferenceType,
            if_then::IfClause,
            literal::{Decimal, LiteralType},
            mask_expression::{StructItem, StructSelect},
            reference_segment,
            window_function::bound as SubstraitBound,
            window_function::bound::Kind as BoundKind,
            window_function::Bound,
            FieldReference, IfThen, Literal, MaskExpression, ReferenceSegment, RexType,
            SingularOrList, WindowFunction as SubstraitWindowFunction,
        },
        function_argument::ArgType,
        join_rel, plan_rel, r#type,
        read_rel::{NamedTable, ReadType},
        rel::RelType,
        set_rel,
        sort_field::{SortDirection, SortKind},
        AggregateFunction, AggregateRel, AggregationPhase, Expression, ExtensionLeafRel,
        ExtensionMultiRel, ExtensionSingleRel, FetchRel, FilterRel, FunctionArgument,
        JoinRel, NamedStruct, Plan, PlanRel, ProjectRel, ReadRel, Rel, RelRoot, SetRel,
        SortField, SortRel,
    },
    version,
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
/// # use datafusion::common::DFSchemaRef;
/// # use datafusion::error::Result;
/// # use datafusion::execution::SessionState;
/// # use datafusion::logical_expr::{Between, Extension, Projection};
/// # use datafusion_substrait::extensions::Extensions;
/// # use datafusion_substrait::logical_plan::producer::{from_projection, SubstraitProducer};
///
/// struct CustomSubstraitProducer {
///     extensions: Extensions,
///     state: Arc<SessionState>,
/// }
///
/// impl SubstraitProducer for CustomSubstraitProducer {
///
///     fn register_function(&mut self, signature: String) -> u32 {
///        self.extensions.register_function(signature)
///     }
///
///     fn get_extensions(self) -> Extensions {
///         self.extensions
///     }
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
    /// the top level of the [Plan] within
    /// [ExtensionFunction](substrait::proto::extensions::simple_extension_declaration::ExtensionFunction)
    /// messages.
    ///
    /// When given a function signature, this method should return the existing anchor for it if
    /// there is one. Otherwise, it should generate a new anchor.
    fn register_function(&mut self, signature: String) -> u32;

    /// Consume the producer to generate the [Extensions] for the Substrait plan based on the
    /// functions that have been registered
    fn get_extensions(self) -> Extensions;

    // Logical Plan Methods
    // There is one method per LogicalPlan to allow for easy overriding of producer behaviour.
    // These methods have default implementations calling the common handler code, to allow for users
    // to re-use common handling logic.

    fn handle_plan(&mut self, plan: &LogicalPlan) -> Result<Box<Rel>> {
        to_substrait_rel(self, plan)
    }

    fn handle_projection(&mut self, plan: &Projection) -> Result<Box<Rel>> {
        from_projection(self, plan)
    }

    fn handle_filter(&mut self, plan: &Filter) -> Result<Box<Rel>> {
        from_filter(self, plan)
    }

    fn handle_window(&mut self, plan: &Window) -> Result<Box<Rel>> {
        from_window(self, plan)
    }

    fn handle_aggregate(&mut self, plan: &Aggregate) -> Result<Box<Rel>> {
        from_aggregate(self, plan)
    }

    fn handle_sort(&mut self, plan: &Sort) -> Result<Box<Rel>> {
        from_sort(self, plan)
    }

    fn handle_join(&mut self, plan: &Join) -> Result<Box<Rel>> {
        from_join(self, plan)
    }

    fn handle_repartition(&mut self, plan: &Repartition) -> Result<Box<Rel>> {
        from_repartition(self, plan)
    }

    fn handle_union(&mut self, plan: &Union) -> Result<Box<Rel>> {
        from_union(self, plan)
    }

    fn handle_table_scan(&mut self, plan: &TableScan) -> Result<Box<Rel>> {
        from_table_scan(self, plan)
    }

    fn handle_empty_relation(&mut self, plan: &EmptyRelation) -> Result<Box<Rel>> {
        from_empty_relation(plan)
    }

    fn handle_subquery_alias(&mut self, plan: &SubqueryAlias) -> Result<Box<Rel>> {
        from_subquery_alias(self, plan)
    }

    fn handle_limit(&mut self, plan: &Limit) -> Result<Box<Rel>> {
        from_limit(self, plan)
    }

    fn handle_values(&mut self, plan: &Values) -> Result<Box<Rel>> {
        from_values(self, plan)
    }

    fn handle_distinct(&mut self, plan: &Distinct) -> Result<Box<Rel>> {
        from_distinct(self, plan)
    }

    fn handle_extension(&mut self, _plan: &Extension) -> Result<Box<Rel>> {
        substrait_err!("Specify handling for LogicalPlan::Extension by implementing the SubstraitProducer trait")
    }

    // Expression Methods
    // There is one method per DataFusion Expr to allow for easy overriding of producer behaviour
    // These methods have default implementations calling the common handler code, to allow for users
    // to re-use common handling logic.

    fn handle_expr(&mut self, expr: &Expr, schema: &DFSchemaRef) -> Result<Expression> {
        to_substrait_rex(self, expr, schema)
    }

    fn handle_alias(
        &mut self,
        alias: &Alias,
        schema: &DFSchemaRef,
    ) -> Result<Expression> {
        from_alias(self, alias, schema)
    }

    fn handle_column(
        &mut self,
        column: &Column,
        schema: &DFSchemaRef,
    ) -> Result<Expression> {
        from_column(column, schema)
    }

    fn handle_literal(&mut self, value: &ScalarValue) -> Result<Expression> {
        from_literal(self, value)
    }

    fn handle_binary_expr(
        &mut self,
        expr: &BinaryExpr,
        schema: &DFSchemaRef,
    ) -> Result<Expression> {
        from_binary_expr(self, expr, schema)
    }

    fn handle_like(&mut self, like: &Like, schema: &DFSchemaRef) -> Result<Expression> {
        from_like(self, like, schema)
    }

    /// For handling Not, IsNotNull, IsNull, IsTrue, IsFalse, IsUnknown, IsNotTrue, IsNotFalse, IsNotUnknown, Negative
    fn handle_unary_expr(
        &mut self,
        expr: &Expr,
        schema: &DFSchemaRef,
    ) -> Result<Expression> {
        from_unary_expr(self, expr, schema)
    }

    fn handle_between(
        &mut self,
        between: &Between,
        schema: &DFSchemaRef,
    ) -> Result<Expression> {
        from_between(self, between, schema)
    }

    fn handle_case(&mut self, case: &Case, schema: &DFSchemaRef) -> Result<Expression> {
        from_case(self, case, schema)
    }

    fn handle_cast(&mut self, cast: &Cast, schema: &DFSchemaRef) -> Result<Expression> {
        from_cast(self, cast, schema)
    }

    fn handle_try_cast(
        &mut self,
        cast: &TryCast,
        schema: &DFSchemaRef,
    ) -> Result<Expression> {
        from_try_cast(self, cast, schema)
    }

    fn handle_scalar_function(
        &mut self,
        scalar_fn: &expr::ScalarFunction,
        schema: &DFSchemaRef,
    ) -> Result<Expression> {
        from_scalar_function(self, scalar_fn, schema)
    }

    fn handle_aggregate_function(
        &mut self,
        agg_fn: &expr::AggregateFunction,
        schema: &DFSchemaRef,
    ) -> Result<Measure> {
        from_aggregate_function(self, agg_fn, schema)
    }

    fn handle_window_function(
        &mut self,
        window_fn: &WindowFunction,
        schema: &DFSchemaRef,
    ) -> Result<Expression> {
        from_window_function(self, window_fn, schema)
    }

    fn handle_in_list(
        &mut self,
        in_list: &InList,
        schema: &DFSchemaRef,
    ) -> Result<Expression> {
        from_in_list(self, in_list, schema)
    }

    fn handle_in_subquery(
        &mut self,
        in_subquery: &InSubquery,
        schema: &DFSchemaRef,
    ) -> Result<Expression> {
        from_in_subquery(self, in_subquery, schema)
    }
}

pub struct DefaultSubstraitProducer<'a> {
    extensions: Extensions,
    serializer_registry: &'a dyn SerializerRegistry,
}

impl<'a> DefaultSubstraitProducer<'a> {
    pub fn new(state: &'a SessionState) -> Self {
        DefaultSubstraitProducer {
            extensions: Extensions::default(),
            serializer_registry: state.serializer_registry().as_ref(),
        }
    }
}

impl SubstraitProducer for DefaultSubstraitProducer<'_> {
    fn register_function(&mut self, fn_name: String) -> u32 {
        self.extensions.register_function(fn_name)
    }

    fn get_extensions(self) -> Extensions {
        self.extensions
    }

    fn handle_extension(&mut self, plan: &Extension) -> Result<Box<Rel>> {
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
            .collect::<Result<Vec<_>>>()?;
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
}

/// Convert DataFusion LogicalPlan to Substrait Plan
pub fn to_substrait_plan(plan: &LogicalPlan, state: &SessionState) -> Result<Box<Plan>> {
    // Parse relation nodes
    // Generate PlanRel(s)
    // Note: Only 1 relation tree is currently supported

    let mut producer: DefaultSubstraitProducer = DefaultSubstraitProducer::new(state);
    let plan_rels = vec![PlanRel {
        rel_type: Some(plan_rel::RelType::Root(RelRoot {
            input: Some(*producer.handle_plan(plan)?),
            names: to_substrait_named_struct(plan.schema())?.names,
        })),
    }];

    // Return parsed plan
    let extensions = producer.get_extensions();
    Ok(Box::new(Plan {
        version: Some(version::version_with_producer("datafusion")),
        extension_uris: vec![],
        extensions: extensions.into(),
        relations: plan_rels,
        advanced_extensions: None,
        expected_type_urls: vec![],
        parameter_bindings: vec![],
    }))
}

/// Serializes a collection of expressions to a Substrait ExtendedExpression message
///
/// The ExtendedExpression message is a top-level message that can be used to send
/// expressions (not plans) between systems.
///
/// Each expression is also given names for the output type.  These are provided as a
/// field and not a String (since the names may be nested, e.g. a struct).  The data
/// type and nullability of this field is redundant (those can be determined by the
/// Expr) and will be ignored.
///
/// Substrait also requires the input schema of the expressions to be included in the
/// message.  The field names of the input schema will be serialized.
pub fn to_substrait_extended_expr(
    exprs: &[(&Expr, &Field)],
    schema: &DFSchemaRef,
    state: &SessionState,
) -> Result<Box<ExtendedExpression>> {
    let mut producer = DefaultSubstraitProducer::new(state);
    let substrait_exprs = exprs
        .iter()
        .map(|(expr, field)| {
            let substrait_expr = producer.handle_expr(expr, schema)?;
            let mut output_names = Vec::new();
            flatten_names(field, false, &mut output_names)?;
            Ok(ExpressionReference {
                output_names,
                expr_type: Some(ExprType::Expression(substrait_expr)),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let substrait_schema = to_substrait_named_struct(schema)?;

    let extensions = producer.get_extensions();
    Ok(Box::new(ExtendedExpression {
        advanced_extensions: None,
        expected_type_urls: vec![],
        extension_uris: vec![],
        extensions: extensions.into(),
        version: Some(version::version_with_producer("datafusion")),
        referred_expr: substrait_exprs,
        base_schema: Some(substrait_schema),
    }))
}

pub fn to_substrait_rel(
    producer: &mut impl SubstraitProducer,
    plan: &LogicalPlan,
) -> Result<Box<Rel>> {
    match plan {
        LogicalPlan::Projection(plan) => producer.handle_projection(plan),
        LogicalPlan::Filter(plan) => producer.handle_filter(plan),
        LogicalPlan::Window(plan) => producer.handle_window(plan),
        LogicalPlan::Aggregate(plan) => producer.handle_aggregate(plan),
        LogicalPlan::Sort(plan) => producer.handle_sort(plan),
        LogicalPlan::Join(plan) => producer.handle_join(plan),
        LogicalPlan::Repartition(plan) => producer.handle_repartition(plan),
        LogicalPlan::Union(plan) => producer.handle_union(plan),
        LogicalPlan::TableScan(plan) => producer.handle_table_scan(plan),
        LogicalPlan::EmptyRelation(plan) => producer.handle_empty_relation(plan),
        LogicalPlan::Subquery(plan) => not_impl_err!("Unsupported plan type: {plan:?}")?,
        LogicalPlan::SubqueryAlias(plan) => producer.handle_subquery_alias(plan),
        LogicalPlan::Limit(plan) => producer.handle_limit(plan),
        LogicalPlan::Statement(plan) => not_impl_err!("Unsupported plan type: {plan:?}")?,
        LogicalPlan::Values(plan) => producer.handle_values(plan),
        LogicalPlan::Explain(plan) => not_impl_err!("Unsupported plan type: {plan:?}")?,
        LogicalPlan::Analyze(plan) => not_impl_err!("Unsupported plan type: {plan:?}")?,
        LogicalPlan::Extension(plan) => producer.handle_extension(plan),
        LogicalPlan::Distinct(plan) => producer.handle_distinct(plan),
        LogicalPlan::Dml(plan) => not_impl_err!("Unsupported plan type: {plan:?}")?,
        LogicalPlan::Ddl(plan) => not_impl_err!("Unsupported plan type: {plan:?}")?,
        LogicalPlan::Copy(plan) => not_impl_err!("Unsupported plan type: {plan:?}")?,
        LogicalPlan::DescribeTable(plan) => {
            not_impl_err!("Unsupported plan type: {plan:?}")?
        }
        LogicalPlan::Unnest(plan) => not_impl_err!("Unsupported plan type: {plan:?}")?,
        LogicalPlan::RecursiveQuery(plan) => {
            not_impl_err!("Unsupported plan type: {plan:?}")?
        }
    }
}

pub fn from_table_scan(
    producer: &mut impl SubstraitProducer,
    scan: &TableScan,
) -> Result<Box<Rel>> {
    let projection = scan.projection.as_ref().map(|p| {
        p.iter()
            .map(|i| StructItem {
                field: *i as i32,
                child: None,
            })
            .collect()
    });

    let projection = projection.map(|struct_items| MaskExpression {
        select: Some(StructSelect { struct_items }),
        maintain_singular_struct: false,
    });

    let table_schema = scan.source.schema().to_dfschema_ref()?;
    let base_schema = to_substrait_named_struct(&table_schema)?;

    let filter_option = if scan.filters.is_empty() {
        None
    } else {
        let table_schema_qualified = Arc::new(
            DFSchema::try_from_qualified_schema(
                scan.table_name.clone(),
                &(scan.source.schema()),
            )
            .unwrap(),
        );

        let combined_expr = conjunction(scan.filters.clone()).unwrap();
        let filter_expr =
            producer.handle_expr(&combined_expr, &table_schema_qualified)?;
        Some(Box::new(filter_expr))
    };

    Ok(Box::new(Rel {
        rel_type: Some(RelType::Read(Box::new(ReadRel {
            common: None,
            base_schema: Some(base_schema),
            filter: filter_option,
            best_effort_filter: None,
            projection,
            advanced_extension: None,
            read_type: Some(ReadType::NamedTable(NamedTable {
                names: scan.table_name.to_vec(),
                advanced_extension: None,
            })),
        }))),
    }))
}

pub fn from_empty_relation(e: &EmptyRelation) -> Result<Box<Rel>> {
    if e.produce_one_row {
        return not_impl_err!("Producing a row from empty relation is unsupported");
    }
    #[allow(deprecated)]
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Read(Box::new(ReadRel {
            common: None,
            base_schema: Some(to_substrait_named_struct(&e.schema)?),
            filter: None,
            best_effort_filter: None,
            projection: None,
            advanced_extension: None,
            read_type: Some(ReadType::VirtualTable(VirtualTable {
                values: vec![],
                expressions: vec![],
            })),
        }))),
    }))
}

pub fn from_values(
    producer: &mut impl SubstraitProducer,
    v: &Values,
) -> Result<Box<Rel>> {
    let values = v
        .values
        .iter()
        .map(|row| {
            let fields = row
                .iter()
                .map(|v| match v {
                    Expr::Literal(sv) => to_substrait_literal(producer, sv),
                    Expr::Alias(alias) => match alias.expr.as_ref() {
                        // The schema gives us the names, so we can skip aliases
                        Expr::Literal(sv) => to_substrait_literal(producer, sv),
                        _ => Err(substrait_datafusion_err!(
                                    "Only literal types can be aliased in Virtual Tables, got: {}", alias.expr.variant_name()
                                )),
                    },
                    _ => Err(substrait_datafusion_err!(
                                "Only literal types and aliases are supported in Virtual Tables, got: {}", v.variant_name()
                            )),
                })
                .collect::<Result<_>>()?;
            Ok(Struct { fields })
        })
        .collect::<Result<_>>()?;
    #[allow(deprecated)]
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Read(Box::new(ReadRel {
            common: None,
            base_schema: Some(to_substrait_named_struct(&v.schema)?),
            filter: None,
            best_effort_filter: None,
            projection: None,
            advanced_extension: None,
            read_type: Some(ReadType::VirtualTable(VirtualTable {
                values,
                expressions: vec![],
            })),
        }))),
    }))
}

pub fn from_projection(
    producer: &mut impl SubstraitProducer,
    p: &Projection,
) -> Result<Box<Rel>> {
    let expressions = p
        .expr
        .iter()
        .map(|e| producer.handle_expr(e, p.input.schema()))
        .collect::<Result<Vec<_>>>()?;

    let emit_kind = create_project_remapping(
        expressions.len(),
        p.input.as_ref().schema().fields().len(),
    );
    let common = RelCommon {
        emit_kind: Some(emit_kind),
        hint: None,
        advanced_extension: None,
    };

    Ok(Box::new(Rel {
        rel_type: Some(RelType::Project(Box::new(ProjectRel {
            common: Some(common),
            input: Some(producer.handle_plan(p.input.as_ref())?),
            expressions,
            advanced_extension: None,
        }))),
    }))
}

pub fn from_filter(
    producer: &mut impl SubstraitProducer,
    filter: &Filter,
) -> Result<Box<Rel>> {
    let input = producer.handle_plan(filter.input.as_ref())?;
    let filter_expr = producer.handle_expr(&filter.predicate, filter.input.schema())?;
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Filter(Box::new(FilterRel {
            common: None,
            input: Some(input),
            condition: Some(Box::new(filter_expr)),
            advanced_extension: None,
        }))),
    }))
}

pub fn from_limit(
    producer: &mut impl SubstraitProducer,
    limit: &Limit,
) -> Result<Box<Rel>> {
    let input = producer.handle_plan(limit.input.as_ref())?;
    let empty_schema = Arc::new(DFSchema::empty());
    let offset_mode = limit
        .skip
        .as_ref()
        .map(|expr| producer.handle_expr(expr.as_ref(), &empty_schema))
        .transpose()?
        .map(Box::new)
        .map(fetch_rel::OffsetMode::OffsetExpr);
    let count_mode = limit
        .fetch
        .as_ref()
        .map(|expr| producer.handle_expr(expr.as_ref(), &empty_schema))
        .transpose()?
        .map(Box::new)
        .map(fetch_rel::CountMode::CountExpr);
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Fetch(Box::new(FetchRel {
            common: None,
            input: Some(input),
            offset_mode,
            count_mode,
            advanced_extension: None,
        }))),
    }))
}

pub fn from_sort(producer: &mut impl SubstraitProducer, sort: &Sort) -> Result<Box<Rel>> {
    let Sort { expr, input, fetch } = sort;
    let sort_fields = expr
        .iter()
        .map(|e| substrait_sort_field(producer, e, input.schema()))
        .collect::<Result<Vec<_>>>()?;

    let input = producer.handle_plan(input.as_ref())?;

    let sort_rel = Box::new(Rel {
        rel_type: Some(RelType::Sort(Box::new(SortRel {
            common: None,
            input: Some(input),
            sorts: sort_fields,
            advanced_extension: None,
        }))),
    });

    match fetch {
        Some(amount) => {
            let count_mode =
                Some(fetch_rel::CountMode::CountExpr(Box::new(Expression {
                    rex_type: Some(RexType::Literal(Literal {
                        nullable: false,
                        type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                        literal_type: Some(LiteralType::I64(*amount as i64)),
                    })),
                })));
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Fetch(Box::new(FetchRel {
                    common: None,
                    input: Some(sort_rel),
                    offset_mode: None,
                    count_mode,
                    advanced_extension: None,
                }))),
            }))
        }
        None => Ok(sort_rel),
    }
}

pub fn from_aggregate(
    producer: &mut impl SubstraitProducer,
    agg: &Aggregate,
) -> Result<Box<Rel>> {
    let input = producer.handle_plan(agg.input.as_ref())?;
    let (grouping_expressions, groupings) =
        to_substrait_groupings(producer, &agg.group_expr, agg.input.schema())?;
    let measures = agg
        .aggr_expr
        .iter()
        .map(|e| to_substrait_agg_measure(producer, e, agg.input.schema()))
        .collect::<Result<Vec<_>>>()?;

    Ok(Box::new(Rel {
        rel_type: Some(RelType::Aggregate(Box::new(AggregateRel {
            common: None,
            input: Some(input),
            grouping_expressions,
            groupings,
            measures,
            advanced_extension: None,
        }))),
    }))
}

pub fn from_distinct(
    producer: &mut impl SubstraitProducer,
    distinct: &Distinct,
) -> Result<Box<Rel>> {
    match distinct {
        Distinct::All(plan) => {
            // Use Substrait's AggregateRel with empty measures to represent `select distinct`
            let input = producer.handle_plan(plan.as_ref())?;
            // Get grouping keys from the input relation's number of output fields
            let grouping = (0..plan.schema().fields().len())
                .map(substrait_field_ref)
                .collect::<Result<Vec<_>>>()?;

            #[allow(deprecated)]
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Aggregate(Box::new(AggregateRel {
                    common: None,
                    input: Some(input),
                    grouping_expressions: vec![],
                    groupings: vec![Grouping {
                        grouping_expressions: grouping,
                        expression_references: vec![],
                    }],
                    measures: vec![],
                    advanced_extension: None,
                }))),
            }))
        }
        Distinct::On(_) => not_impl_err!("Cannot convert Distinct::On"),
    }
}

pub fn from_join(producer: &mut impl SubstraitProducer, join: &Join) -> Result<Box<Rel>> {
    let left = producer.handle_plan(join.left.as_ref())?;
    let right = producer.handle_plan(join.right.as_ref())?;
    let join_type = to_substrait_jointype(join.join_type);
    // we only support basic joins so return an error for anything not yet supported
    match join.join_constraint {
        JoinConstraint::On => {}
        JoinConstraint::Using => return not_impl_err!("join constraint: `using`"),
    }
    let in_join_schema = Arc::new(join.left.schema().join(join.right.schema())?);

    // convert filter if present
    let join_filter = match &join.filter {
        Some(filter) => Some(producer.handle_expr(filter, &in_join_schema)?),
        None => None,
    };

    // map the left and right columns to binary expressions in the form `l = r`
    // build a single expression for the ON condition, such as `l.a = r.a AND l.b = r.b`
    let eq_op = if join.null_equals_null {
        Operator::IsNotDistinctFrom
    } else {
        Operator::Eq
    };
    let join_on = to_substrait_join_expr(producer, &join.on, eq_op, &in_join_schema)?;

    // create conjunction between `join_on` and `join_filter` to embed all join conditions,
    // whether equal or non-equal in a single expression
    let join_expr = match &join_on {
        Some(on_expr) => match &join_filter {
            Some(filter) => Some(Box::new(make_binary_op_scalar_func(
                producer,
                on_expr,
                filter,
                Operator::And,
            ))),
            None => join_on.map(Box::new), // the join expression will only contain `join_on` if filter doesn't exist
        },
        None => match &join_filter {
            Some(_) => join_filter.map(Box::new), // the join expression will only contain `join_filter` if the `on` condition doesn't exist
            None => None,
        },
    };

    Ok(Box::new(Rel {
        rel_type: Some(RelType::Join(Box::new(JoinRel {
            common: None,
            left: Some(left),
            right: Some(right),
            r#type: join_type as i32,
            expression: join_expr,
            post_join_filter: None,
            advanced_extension: None,
        }))),
    }))
}

pub fn from_subquery_alias(
    producer: &mut impl SubstraitProducer,
    alias: &SubqueryAlias,
) -> Result<Box<Rel>> {
    // Do nothing if encounters SubqueryAlias
    // since there is no corresponding relation type in Substrait
    producer.handle_plan(alias.input.as_ref())
}

pub fn from_union(
    producer: &mut impl SubstraitProducer,
    union: &Union,
) -> Result<Box<Rel>> {
    let input_rels = union
        .inputs
        .iter()
        .map(|input| producer.handle_plan(input.as_ref()))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|ptr| *ptr)
        .collect();
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Set(SetRel {
            common: None,
            inputs: input_rels,
            op: set_rel::SetOp::UnionAll as i32, // UNION DISTINCT gets translated to AGGREGATION + UNION ALL
            advanced_extension: None,
        })),
    }))
}

pub fn from_window(
    producer: &mut impl SubstraitProducer,
    window: &Window,
) -> Result<Box<Rel>> {
    let input = producer.handle_plan(window.input.as_ref())?;

    // create a field reference for each input field
    let mut expressions = (0..window.input.schema().fields().len())
        .map(substrait_field_ref)
        .collect::<Result<Vec<_>>>()?;

    // process and add each window function expression
    for expr in &window.window_expr {
        expressions.push(producer.handle_expr(expr, window.input.schema())?);
    }

    let emit_kind =
        create_project_remapping(expressions.len(), window.input.schema().fields().len());
    let common = RelCommon {
        emit_kind: Some(emit_kind),
        hint: None,
        advanced_extension: None,
    };
    let project_rel = Box::new(ProjectRel {
        common: Some(common),
        input: Some(input),
        expressions,
        advanced_extension: None,
    });

    Ok(Box::new(Rel {
        rel_type: Some(RelType::Project(project_rel)),
    }))
}

pub fn from_repartition(
    producer: &mut impl SubstraitProducer,
    repartition: &Repartition,
) -> Result<Box<Rel>> {
    let input = producer.handle_plan(repartition.input.as_ref())?;
    let partition_count = match repartition.partitioning_scheme {
        Partitioning::RoundRobinBatch(num) => num,
        Partitioning::Hash(_, num) => num,
        Partitioning::DistributeBy(_) => {
            return not_impl_err!(
                "Physical plan does not support DistributeBy partitioning"
            )
        }
    };
    // ref: https://substrait.io/relations/physical_relations/#exchange-types
    let exchange_kind = match &repartition.partitioning_scheme {
        Partitioning::RoundRobinBatch(_) => {
            ExchangeKind::RoundRobin(RoundRobin::default())
        }
        Partitioning::Hash(exprs, _) => {
            let fields = exprs
                .iter()
                .map(|e| try_to_substrait_field_reference(e, repartition.input.schema()))
                .collect::<Result<Vec<_>>>()?;
            ExchangeKind::ScatterByFields(ScatterFields { fields })
        }
        Partitioning::DistributeBy(_) => {
            return not_impl_err!(
                "Physical plan does not support DistributeBy partitioning"
            )
        }
    };
    let exchange_rel = ExchangeRel {
        common: None,
        input: Some(input),
        exchange_kind: Some(exchange_kind),
        advanced_extension: None,
        partition_count: partition_count as i32,
        targets: vec![],
    };
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Exchange(Box::new(exchange_rel))),
    }))
}

/// By default, a Substrait Project outputs all input fields followed by all expressions.
/// A DataFusion Projection only outputs expressions. In order to keep the Substrait
/// plan consistent with DataFusion, we must apply an output mapping that skips the input
/// fields so that the Substrait Project will only output the expression fields.
fn create_project_remapping(expr_count: usize, input_field_count: usize) -> EmitKind {
    let expression_field_start = input_field_count;
    let expression_field_end = expression_field_start + expr_count;
    let output_mapping = (expression_field_start..expression_field_end)
        .map(|i| i as i32)
        .collect();
    Emit(rel_common::Emit { output_mapping })
}

// Substrait wants a list of all field names, including nested fields from structs,
// also from within e.g. lists and maps. However, it does not want the list and map field names
// themselves - only proper structs fields are considered to have useful names.
fn flatten_names(field: &Field, skip_self: bool, names: &mut Vec<String>) -> Result<()> {
    if !skip_self {
        names.push(field.name().to_string());
    }
    match field.data_type() {
        DataType::Struct(fields) => {
            for field in fields {
                flatten_names(field, false, names)?;
            }
            Ok(())
        }
        DataType::List(l) => flatten_names(l, true, names),
        DataType::LargeList(l) => flatten_names(l, true, names),
        DataType::Map(m, _) => match m.data_type() {
            DataType::Struct(key_and_value) if key_and_value.len() == 2 => {
                flatten_names(&key_and_value[0], true, names)?;
                flatten_names(&key_and_value[1], true, names)
            }
            _ => plan_err!("Map fields must contain a Struct with exactly 2 fields"),
        },
        _ => Ok(()),
    }?;
    Ok(())
}

fn to_substrait_named_struct(schema: &DFSchemaRef) -> Result<NamedStruct> {
    let mut names = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        flatten_names(field, false, &mut names)?;
    }

    let field_types = r#type::Struct {
        types: schema
            .fields()
            .iter()
            .map(|f| to_substrait_type(f.data_type(), f.is_nullable()))
            .collect::<Result<_>>()?,
        type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
        nullability: r#type::Nullability::Required as i32,
    };

    Ok(NamedStruct {
        names,
        r#struct: Some(field_types),
    })
}

fn to_substrait_join_expr(
    producer: &mut impl SubstraitProducer,
    join_conditions: &Vec<(Expr, Expr)>,
    eq_op: Operator,
    join_schema: &DFSchemaRef,
) -> Result<Option<Expression>> {
    // Only support AND conjunction for each binary expression in join conditions
    let mut exprs: Vec<Expression> = vec![];
    for (left, right) in join_conditions {
        let l = producer.handle_expr(left, join_schema)?;
        let r = producer.handle_expr(right, join_schema)?;
        // AND with existing expression
        exprs.push(make_binary_op_scalar_func(producer, &l, &r, eq_op));
    }

    let join_expr: Option<Expression> =
        exprs.into_iter().reduce(|acc: Expression, e: Expression| {
            make_binary_op_scalar_func(producer, &acc, &e, Operator::And)
        });
    Ok(join_expr)
}

fn to_substrait_jointype(join_type: JoinType) -> join_rel::JoinType {
    match join_type {
        JoinType::Inner => join_rel::JoinType::Inner,
        JoinType::Left => join_rel::JoinType::Left,
        JoinType::Right => join_rel::JoinType::Right,
        JoinType::Full => join_rel::JoinType::Outer,
        JoinType::LeftAnti => join_rel::JoinType::LeftAnti,
        JoinType::LeftSemi => join_rel::JoinType::LeftSemi,
        JoinType::LeftMark => join_rel::JoinType::LeftMark,
        JoinType::RightAnti | JoinType::RightSemi => {
            unimplemented!()
        }
    }
}

pub fn operator_to_name(op: Operator) -> &'static str {
    match op {
        Operator::Eq => "equal",
        Operator::NotEq => "not_equal",
        Operator::Lt => "lt",
        Operator::LtEq => "lte",
        Operator::Gt => "gt",
        Operator::GtEq => "gte",
        Operator::Plus => "add",
        Operator::Minus => "subtract",
        Operator::Multiply => "multiply",
        Operator::Divide => "divide",
        Operator::Modulo => "modulus",
        Operator::And => "and",
        Operator::Or => "or",
        Operator::IsDistinctFrom => "is_distinct_from",
        Operator::IsNotDistinctFrom => "is_not_distinct_from",
        Operator::RegexMatch => "regex_match",
        Operator::RegexIMatch => "regex_imatch",
        Operator::RegexNotMatch => "regex_not_match",
        Operator::RegexNotIMatch => "regex_not_imatch",
        Operator::LikeMatch => "like_match",
        Operator::ILikeMatch => "like_imatch",
        Operator::NotLikeMatch => "like_not_match",
        Operator::NotILikeMatch => "like_not_imatch",
        Operator::BitwiseAnd => "bitwise_and",
        Operator::BitwiseOr => "bitwise_or",
        Operator::StringConcat => "str_concat",
        Operator::AtArrow => "at_arrow",
        Operator::ArrowAt => "arrow_at",
        Operator::Arrow => "arrow",
        Operator::LongArrow => "long_arrow",
        Operator::HashArrow => "hash_arrow",
        Operator::HashLongArrow => "hash_long_arrow",
        Operator::AtAt => "at_at",
        Operator::IntegerDivide => "integer_divide",
        Operator::HashMinus => "hash_minus",
        Operator::AtQuestion => "at_question",
        Operator::Question => "question",
        Operator::QuestionAnd => "question_and",
        Operator::QuestionPipe => "question_pipe",
        Operator::BitwiseXor => "bitwise_xor",
        Operator::BitwiseShiftRight => "bitwise_shift_right",
        Operator::BitwiseShiftLeft => "bitwise_shift_left",
    }
}

pub fn parse_flat_grouping_exprs(
    producer: &mut impl SubstraitProducer,
    exprs: &[Expr],
    schema: &DFSchemaRef,
    ref_group_exprs: &mut Vec<Expression>,
) -> Result<Grouping> {
    let mut expression_references = vec![];
    let mut grouping_expressions = vec![];

    for e in exprs {
        let rex = producer.handle_expr(e, schema)?;
        grouping_expressions.push(rex.clone());
        ref_group_exprs.push(rex);
        expression_references.push((ref_group_exprs.len() - 1) as u32);
    }
    #[allow(deprecated)]
    Ok(Grouping {
        grouping_expressions,
        expression_references,
    })
}

pub fn to_substrait_groupings(
    producer: &mut impl SubstraitProducer,
    exprs: &[Expr],
    schema: &DFSchemaRef,
) -> Result<(Vec<Expression>, Vec<Grouping>)> {
    let mut ref_group_exprs = vec![];
    let groupings = match exprs.len() {
        1 => match &exprs[0] {
            Expr::GroupingSet(gs) => match gs {
                GroupingSet::Cube(_) => Err(DataFusionError::NotImplemented(
                    "GroupingSet CUBE is not yet supported".to_string(),
                )),
                GroupingSet::GroupingSets(sets) => Ok(sets
                    .iter()
                    .map(|set| {
                        parse_flat_grouping_exprs(
                            producer,
                            set,
                            schema,
                            &mut ref_group_exprs,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?),
                GroupingSet::Rollup(set) => {
                    let mut sets: Vec<Vec<Expr>> = vec![vec![]];
                    for i in 0..set.len() {
                        sets.push(set[..=i].to_vec());
                    }
                    Ok(sets
                        .iter()
                        .rev()
                        .map(|set| {
                            parse_flat_grouping_exprs(
                                producer,
                                set,
                                schema,
                                &mut ref_group_exprs,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?)
                }
            },
            _ => Ok(vec![parse_flat_grouping_exprs(
                producer,
                exprs,
                schema,
                &mut ref_group_exprs,
            )?]),
        },
        _ => Ok(vec![parse_flat_grouping_exprs(
            producer,
            exprs,
            schema,
            &mut ref_group_exprs,
        )?]),
    }?;
    Ok((ref_group_exprs, groupings))
}

pub fn from_aggregate_function(
    producer: &mut impl SubstraitProducer,
    agg_fn: &expr::AggregateFunction,
    schema: &DFSchemaRef,
) -> Result<Measure> {
    let expr::AggregateFunction {
        func,
        params:
            AggregateFunctionParams {
                args,
                distinct,
                filter,
                order_by,
                null_treatment: _null_treatment,
            },
    } = agg_fn;
    let sorts = if let Some(order_by) = order_by {
        order_by
            .iter()
            .map(|expr| to_substrait_sort_field(producer, expr, schema))
            .collect::<Result<Vec<_>>>()?
    } else {
        vec![]
    };
    let mut arguments: Vec<FunctionArgument> = vec![];
    for arg in args {
        arguments.push(FunctionArgument {
            arg_type: Some(ArgType::Value(producer.handle_expr(arg, schema)?)),
        });
    }
    let function_anchor = producer.register_function(func.name().to_string());
    #[allow(deprecated)]
    Ok(Measure {
        measure: Some(AggregateFunction {
            function_reference: function_anchor,
            arguments,
            sorts,
            output_type: None,
            invocation: match distinct {
                true => AggregationInvocation::Distinct as i32,
                false => AggregationInvocation::All as i32,
            },
            phase: AggregationPhase::Unspecified as i32,
            args: vec![],
            options: vec![],
        }),
        filter: match filter {
            Some(f) => Some(producer.handle_expr(f, schema)?),
            None => None,
        },
    })
}

pub fn to_substrait_agg_measure(
    producer: &mut impl SubstraitProducer,
    expr: &Expr,
    schema: &DFSchemaRef,
) -> Result<Measure> {
    match expr {
        Expr::AggregateFunction(agg_fn) => from_aggregate_function(producer, agg_fn, schema),
        Expr::Alias(Alias { expr, .. }) => {
            to_substrait_agg_measure(producer, expr, schema)
        }
        _ => internal_err!(
            "Expression must be compatible with aggregation. Unsupported expression: {:?}. ExpressionType: {:?}",
            expr,
            expr.variant_name()
        ),
    }
}

/// Converts sort expression to corresponding substrait `SortField`
fn to_substrait_sort_field(
    producer: &mut impl SubstraitProducer,
    sort: &expr::Sort,
    schema: &DFSchemaRef,
) -> Result<SortField> {
    let sort_kind = match (sort.asc, sort.nulls_first) {
        (true, true) => SortDirection::AscNullsFirst,
        (true, false) => SortDirection::AscNullsLast,
        (false, true) => SortDirection::DescNullsFirst,
        (false, false) => SortDirection::DescNullsLast,
    };
    Ok(SortField {
        expr: Some(producer.handle_expr(&sort.expr, schema)?),
        sort_kind: Some(SortKind::Direction(sort_kind.into())),
    })
}

/// Return Substrait scalar function with two arguments
pub fn make_binary_op_scalar_func(
    producer: &mut impl SubstraitProducer,
    lhs: &Expression,
    rhs: &Expression,
    op: Operator,
) -> Expression {
    let function_anchor = producer.register_function(operator_to_name(op).to_string());
    #[allow(deprecated)]
    Expression {
        rex_type: Some(RexType::ScalarFunction(ScalarFunction {
            function_reference: function_anchor,
            arguments: vec![
                FunctionArgument {
                    arg_type: Some(ArgType::Value(lhs.clone())),
                },
                FunctionArgument {
                    arg_type: Some(ArgType::Value(rhs.clone())),
                },
            ],
            output_type: None,
            args: vec![],
            options: vec![],
        })),
    }
}

/// Convert DataFusion Expr to Substrait Rex
///
/// # Arguments
/// * `producer` - SubstraitProducer implementation which the handles the actual conversion
/// * `expr` - DataFusion expression to convert into a Substrait expression
/// * `schema` - DataFusion input schema for looking up columns
pub fn to_substrait_rex(
    producer: &mut impl SubstraitProducer,
    expr: &Expr,
    schema: &DFSchemaRef,
) -> Result<Expression> {
    match expr {
        Expr::Alias(expr) => producer.handle_alias(expr, schema),
        Expr::Column(expr) => producer.handle_column(expr, schema),
        Expr::ScalarVariable(_, _) => {
            not_impl_err!("Cannot convert {expr:?} to Substrait")
        }
        Expr::Literal(expr) => producer.handle_literal(expr),
        Expr::BinaryExpr(expr) => producer.handle_binary_expr(expr, schema),
        Expr::Like(expr) => producer.handle_like(expr, schema),
        Expr::SimilarTo(_) => not_impl_err!("Cannot convert {expr:?} to Substrait"),
        Expr::Not(_) => producer.handle_unary_expr(expr, schema),
        Expr::IsNotNull(_) => producer.handle_unary_expr(expr, schema),
        Expr::IsNull(_) => producer.handle_unary_expr(expr, schema),
        Expr::IsTrue(_) => producer.handle_unary_expr(expr, schema),
        Expr::IsFalse(_) => producer.handle_unary_expr(expr, schema),
        Expr::IsUnknown(_) => producer.handle_unary_expr(expr, schema),
        Expr::IsNotTrue(_) => producer.handle_unary_expr(expr, schema),
        Expr::IsNotFalse(_) => producer.handle_unary_expr(expr, schema),
        Expr::IsNotUnknown(_) => producer.handle_unary_expr(expr, schema),
        Expr::Negative(_) => producer.handle_unary_expr(expr, schema),
        Expr::Between(expr) => producer.handle_between(expr, schema),
        Expr::Case(expr) => producer.handle_case(expr, schema),
        Expr::Cast(expr) => producer.handle_cast(expr, schema),
        Expr::TryCast(expr) => producer.handle_try_cast(expr, schema),
        Expr::ScalarFunction(expr) => producer.handle_scalar_function(expr, schema),
        Expr::AggregateFunction(_) => {
            internal_err!(
                "AggregateFunction should only be encountered as part of a LogicalPlan::Aggregate"
            )
        }
        Expr::WindowFunction(expr) => producer.handle_window_function(expr, schema),
        Expr::InList(expr) => producer.handle_in_list(expr, schema),
        Expr::Exists(expr) => not_impl_err!("Cannot convert {expr:?} to Substrait"),
        Expr::InSubquery(expr) => producer.handle_in_subquery(expr, schema),
        Expr::ScalarSubquery(expr) => {
            not_impl_err!("Cannot convert {expr:?} to Substrait")
        }
        #[expect(deprecated)]
        Expr::Wildcard { .. } => not_impl_err!("Cannot convert {expr:?} to Substrait"),
        Expr::GroupingSet(expr) => not_impl_err!("Cannot convert {expr:?} to Substrait"),
        Expr::Placeholder(expr) => not_impl_err!("Cannot convert {expr:?} to Substrait"),
        Expr::OuterReferenceColumn(_, _) => {
            not_impl_err!("Cannot convert {expr:?} to Substrait")
        }
        Expr::Unnest(expr) => not_impl_err!("Cannot convert {expr:?} to Substrait"),
    }
}

pub fn from_in_list(
    producer: &mut impl SubstraitProducer,
    in_list: &InList,
    schema: &DFSchemaRef,
) -> Result<Expression> {
    let InList {
        expr,
        list,
        negated,
    } = in_list;
    let substrait_list = list
        .iter()
        .map(|x| producer.handle_expr(x, schema))
        .collect::<Result<Vec<Expression>>>()?;
    let substrait_expr = producer.handle_expr(expr, schema)?;

    let substrait_or_list = Expression {
        rex_type: Some(RexType::SingularOrList(Box::new(SingularOrList {
            value: Some(Box::new(substrait_expr)),
            options: substrait_list,
        }))),
    };

    if *negated {
        let function_anchor = producer.register_function("not".to_string());

        #[allow(deprecated)]
        Ok(Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                function_reference: function_anchor,
                arguments: vec![FunctionArgument {
                    arg_type: Some(ArgType::Value(substrait_or_list)),
                }],
                output_type: None,
                args: vec![],
                options: vec![],
            })),
        })
    } else {
        Ok(substrait_or_list)
    }
}

pub fn from_scalar_function(
    producer: &mut impl SubstraitProducer,
    fun: &expr::ScalarFunction,
    schema: &DFSchemaRef,
) -> Result<Expression> {
    let mut arguments: Vec<FunctionArgument> = vec![];
    for arg in &fun.args {
        arguments.push(FunctionArgument {
            arg_type: Some(ArgType::Value(producer.handle_expr(arg, schema)?)),
        });
    }

    let function_anchor = producer.register_function(fun.name().to_string());
    #[allow(deprecated)]
    Ok(Expression {
        rex_type: Some(RexType::ScalarFunction(ScalarFunction {
            function_reference: function_anchor,
            arguments,
            output_type: None,
            options: vec![],
            args: vec![],
        })),
    })
}

pub fn from_between(
    producer: &mut impl SubstraitProducer,
    between: &Between,
    schema: &DFSchemaRef,
) -> Result<Expression> {
    let Between {
        expr,
        negated,
        low,
        high,
    } = between;
    if *negated {
        // `expr NOT BETWEEN low AND high` can be translated into (expr < low OR high < expr)
        let substrait_expr = producer.handle_expr(expr.as_ref(), schema)?;
        let substrait_low = producer.handle_expr(low.as_ref(), schema)?;
        let substrait_high = producer.handle_expr(high.as_ref(), schema)?;

        let l_expr = make_binary_op_scalar_func(
            producer,
            &substrait_expr,
            &substrait_low,
            Operator::Lt,
        );
        let r_expr = make_binary_op_scalar_func(
            producer,
            &substrait_high,
            &substrait_expr,
            Operator::Lt,
        );

        Ok(make_binary_op_scalar_func(
            producer,
            &l_expr,
            &r_expr,
            Operator::Or,
        ))
    } else {
        // `expr BETWEEN low AND high` can be translated into (low <= expr AND expr <= high)
        let substrait_expr = producer.handle_expr(expr.as_ref(), schema)?;
        let substrait_low = producer.handle_expr(low.as_ref(), schema)?;
        let substrait_high = producer.handle_expr(high.as_ref(), schema)?;

        let l_expr = make_binary_op_scalar_func(
            producer,
            &substrait_low,
            &substrait_expr,
            Operator::LtEq,
        );
        let r_expr = make_binary_op_scalar_func(
            producer,
            &substrait_expr,
            &substrait_high,
            Operator::LtEq,
        );

        Ok(make_binary_op_scalar_func(
            producer,
            &l_expr,
            &r_expr,
            Operator::And,
        ))
    }
}
pub fn from_column(col: &Column, schema: &DFSchemaRef) -> Result<Expression> {
    let index = schema.index_of_column(col)?;
    substrait_field_ref(index)
}

pub fn from_binary_expr(
    producer: &mut impl SubstraitProducer,
    expr: &BinaryExpr,
    schema: &DFSchemaRef,
) -> Result<Expression> {
    let BinaryExpr { left, op, right } = expr;
    let l = producer.handle_expr(left, schema)?;
    let r = producer.handle_expr(right, schema)?;
    Ok(make_binary_op_scalar_func(producer, &l, &r, *op))
}
pub fn from_case(
    producer: &mut impl SubstraitProducer,
    case: &Case,
    schema: &DFSchemaRef,
) -> Result<Expression> {
    let Case {
        expr,
        when_then_expr,
        else_expr,
    } = case;
    let mut ifs: Vec<IfClause> = vec![];
    // Parse base
    if let Some(e) = expr {
        // Base expression exists
        ifs.push(IfClause {
            r#if: Some(producer.handle_expr(e, schema)?),
            then: None,
        });
    }
    // Parse `when`s
    for (r#if, then) in when_then_expr {
        ifs.push(IfClause {
            r#if: Some(producer.handle_expr(r#if, schema)?),
            then: Some(producer.handle_expr(then, schema)?),
        });
    }

    // Parse outer `else`
    let r#else: Option<Box<Expression>> = match else_expr {
        Some(e) => Some(Box::new(producer.handle_expr(e, schema)?)),
        None => None,
    };

    Ok(Expression {
        rex_type: Some(RexType::IfThen(Box::new(IfThen { ifs, r#else }))),
    })
}

pub fn from_cast(
    producer: &mut impl SubstraitProducer,
    cast: &Cast,
    schema: &DFSchemaRef,
) -> Result<Expression> {
    let Cast { expr, data_type } = cast;
    Ok(Expression {
        rex_type: Some(RexType::Cast(Box::new(
            substrait::proto::expression::Cast {
                r#type: Some(to_substrait_type(data_type, true)?),
                input: Some(Box::new(producer.handle_expr(expr, schema)?)),
                failure_behavior: FailureBehavior::ThrowException.into(),
            },
        ))),
    })
}

pub fn from_try_cast(
    producer: &mut impl SubstraitProducer,
    cast: &TryCast,
    schema: &DFSchemaRef,
) -> Result<Expression> {
    let TryCast { expr, data_type } = cast;
    Ok(Expression {
        rex_type: Some(RexType::Cast(Box::new(
            substrait::proto::expression::Cast {
                r#type: Some(to_substrait_type(data_type, true)?),
                input: Some(Box::new(producer.handle_expr(expr, schema)?)),
                failure_behavior: FailureBehavior::ReturnNull.into(),
            },
        ))),
    })
}

pub fn from_literal(
    producer: &mut impl SubstraitProducer,
    value: &ScalarValue,
) -> Result<Expression> {
    to_substrait_literal_expr(producer, value)
}

pub fn from_alias(
    producer: &mut impl SubstraitProducer,
    alias: &Alias,
    schema: &DFSchemaRef,
) -> Result<Expression> {
    producer.handle_expr(alias.expr.as_ref(), schema)
}

pub fn from_window_function(
    producer: &mut impl SubstraitProducer,
    window_fn: &WindowFunction,
    schema: &DFSchemaRef,
) -> Result<Expression> {
    let WindowFunction {
        fun,
        params:
            WindowFunctionParams {
                args,
                partition_by,
                order_by,
                window_frame,
                null_treatment: _,
            },
    } = window_fn;
    // function reference
    let function_anchor = producer.register_function(fun.to_string());
    // arguments
    let mut arguments: Vec<FunctionArgument> = vec![];
    for arg in args {
        arguments.push(FunctionArgument {
            arg_type: Some(ArgType::Value(producer.handle_expr(arg, schema)?)),
        });
    }
    // partition by expressions
    let partition_by = partition_by
        .iter()
        .map(|e| producer.handle_expr(e, schema))
        .collect::<Result<Vec<_>>>()?;
    // order by expressions
    let order_by = order_by
        .iter()
        .map(|e| substrait_sort_field(producer, e, schema))
        .collect::<Result<Vec<_>>>()?;
    // window frame
    let bounds = to_substrait_bounds(window_frame)?;
    let bound_type = to_substrait_bound_type(window_frame)?;
    Ok(make_substrait_window_function(
        function_anchor,
        arguments,
        partition_by,
        order_by,
        bounds,
        bound_type,
    ))
}

pub fn from_like(
    producer: &mut impl SubstraitProducer,
    like: &Like,
    schema: &DFSchemaRef,
) -> Result<Expression> {
    let Like {
        negated,
        expr,
        pattern,
        escape_char,
        case_insensitive,
    } = like;
    make_substrait_like_expr(
        producer,
        *case_insensitive,
        *negated,
        expr,
        pattern,
        *escape_char,
        schema,
    )
}

pub fn from_in_subquery(
    producer: &mut impl SubstraitProducer,
    subquery: &InSubquery,
    schema: &DFSchemaRef,
) -> Result<Expression> {
    let InSubquery {
        expr,
        subquery,
        negated,
    } = subquery;
    let substrait_expr = producer.handle_expr(expr, schema)?;

    let subquery_plan = producer.handle_plan(subquery.subquery.as_ref())?;

    let substrait_subquery = Expression {
        rex_type: Some(RexType::Subquery(Box::new(
            substrait::proto::expression::Subquery {
                subquery_type: Some(
                    substrait::proto::expression::subquery::SubqueryType::InPredicate(
                        Box::new(InPredicate {
                            needles: (vec![substrait_expr]),
                            haystack: Some(subquery_plan),
                        }),
                    ),
                ),
            },
        ))),
    };
    if *negated {
        let function_anchor = producer.register_function("not".to_string());

        #[allow(deprecated)]
        Ok(Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                function_reference: function_anchor,
                arguments: vec![FunctionArgument {
                    arg_type: Some(ArgType::Value(substrait_subquery)),
                }],
                output_type: None,
                args: vec![],
                options: vec![],
            })),
        })
    } else {
        Ok(substrait_subquery)
    }
}

pub fn from_unary_expr(
    producer: &mut impl SubstraitProducer,
    expr: &Expr,
    schema: &DFSchemaRef,
) -> Result<Expression> {
    let (fn_name, arg) = match expr {
        Expr::Not(arg) => ("not", arg),
        Expr::IsNull(arg) => ("is_null", arg),
        Expr::IsNotNull(arg) => ("is_not_null", arg),
        Expr::IsTrue(arg) => ("is_true", arg),
        Expr::IsFalse(arg) => ("is_false", arg),
        Expr::IsUnknown(arg) => ("is_unknown", arg),
        Expr::IsNotTrue(arg) => ("is_not_true", arg),
        Expr::IsNotFalse(arg) => ("is_not_false", arg),
        Expr::IsNotUnknown(arg) => ("is_not_unknown", arg),
        Expr::Negative(arg) => ("negate", arg),
        expr => not_impl_err!("Unsupported expression: {expr:?}")?,
    };
    to_substrait_unary_scalar_fn(producer, fn_name, arg, schema)
}

fn to_substrait_type(dt: &DataType, nullable: bool) -> Result<substrait::proto::Type> {
    let nullability = if nullable {
        r#type::Nullability::Nullable as i32
    } else {
        r#type::Nullability::Required as i32
    };
    match dt {
        DataType::Null => internal_err!("Null cast is not valid"),
        DataType::Boolean => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Bool(r#type::Boolean {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Int8 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I8(r#type::I8 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::UInt8 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I8(r#type::I8 {
                type_variation_reference: UNSIGNED_INTEGER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Int16 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I16(r#type::I16 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::UInt16 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I16(r#type::I16 {
                type_variation_reference: UNSIGNED_INTEGER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Int32 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I32(r#type::I32 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::UInt32 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I32(r#type::I32 {
                type_variation_reference: UNSIGNED_INTEGER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Int64 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I64(r#type::I64 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::UInt64 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::I64(r#type::I64 {
                type_variation_reference: UNSIGNED_INTEGER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        // Float16 is not supported in Substrait
        DataType::Float32 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Fp32(r#type::Fp32 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Float64 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Fp64(r#type::Fp64 {
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Timestamp(unit, tz) => {
            let precision = match unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 3,
                TimeUnit::Microsecond => 6,
                TimeUnit::Nanosecond => 9,
            };
            let kind = match tz {
                None => r#type::Kind::PrecisionTimestamp(r#type::PrecisionTimestamp {
                    type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                    nullability,
                    precision,
                }),
                Some(_) => {
                    // If timezone is present, no matter what the actual tz value is, it indicates the
                    // value of the timestamp is tied to UTC epoch. That's all that Substrait cares about.
                    // As the timezone is lost, this conversion may be lossy for downstream use of the value.
                    r#type::Kind::PrecisionTimestampTz(r#type::PrecisionTimestampTz {
                        type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                        nullability,
                        precision,
                    })
                }
            };
            Ok(substrait::proto::Type { kind: Some(kind) })
        }
        DataType::Date32 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Date(r#type::Date {
                type_variation_reference: DATE_32_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Date64 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Date(r#type::Date {
                type_variation_reference: DATE_64_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Interval(interval_unit) => {
            match interval_unit {
                IntervalUnit::YearMonth => Ok(substrait::proto::Type {
                    kind: Some(r#type::Kind::IntervalYear(r#type::IntervalYear {
                        type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                        nullability,
                    })),
                }),
                IntervalUnit::DayTime => Ok(substrait::proto::Type {
                    kind: Some(r#type::Kind::IntervalDay(r#type::IntervalDay {
                        type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                        nullability,
                        precision: Some(3), // DayTime precision is always milliseconds
                    })),
                }),
                IntervalUnit::MonthDayNano => {
                    Ok(substrait::proto::Type {
                        kind: Some(r#type::Kind::IntervalCompound(
                            r#type::IntervalCompound {
                                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                                nullability,
                                precision: 9, // nanos
                            },
                        )),
                    })
                }
            }
        }
        DataType::Binary => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Binary(r#type::Binary {
                type_variation_reference: DEFAULT_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::FixedSizeBinary(length) => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::FixedBinary(r#type::FixedBinary {
                length: *length,
                type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::LargeBinary => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Binary(r#type::Binary {
                type_variation_reference: LARGE_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::BinaryView => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Binary(r#type::Binary {
                type_variation_reference: VIEW_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Utf8 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::String(r#type::String {
                type_variation_reference: DEFAULT_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::LargeUtf8 => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::String(r#type::String {
                type_variation_reference: LARGE_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::Utf8View => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::String(r#type::String {
                type_variation_reference: VIEW_CONTAINER_TYPE_VARIATION_REF,
                nullability,
            })),
        }),
        DataType::List(inner) => {
            let inner_type = to_substrait_type(inner.data_type(), inner.is_nullable())?;
            Ok(substrait::proto::Type {
                kind: Some(r#type::Kind::List(Box::new(r#type::List {
                    r#type: Some(Box::new(inner_type)),
                    type_variation_reference: DEFAULT_CONTAINER_TYPE_VARIATION_REF,
                    nullability,
                }))),
            })
        }
        DataType::LargeList(inner) => {
            let inner_type = to_substrait_type(inner.data_type(), inner.is_nullable())?;
            Ok(substrait::proto::Type {
                kind: Some(r#type::Kind::List(Box::new(r#type::List {
                    r#type: Some(Box::new(inner_type)),
                    type_variation_reference: LARGE_CONTAINER_TYPE_VARIATION_REF,
                    nullability,
                }))),
            })
        }
        DataType::Map(inner, _) => match inner.data_type() {
            DataType::Struct(key_and_value) if key_and_value.len() == 2 => {
                let key_type = to_substrait_type(
                    key_and_value[0].data_type(),
                    key_and_value[0].is_nullable(),
                )?;
                let value_type = to_substrait_type(
                    key_and_value[1].data_type(),
                    key_and_value[1].is_nullable(),
                )?;
                Ok(substrait::proto::Type {
                    kind: Some(r#type::Kind::Map(Box::new(r#type::Map {
                        key: Some(Box::new(key_type)),
                        value: Some(Box::new(value_type)),
                        type_variation_reference: DEFAULT_CONTAINER_TYPE_VARIATION_REF,
                        nullability,
                    }))),
                })
            }
            _ => plan_err!("Map fields must contain a Struct with exactly 2 fields"),
        },
        DataType::Struct(fields) => {
            let field_types = fields
                .iter()
                .map(|field| to_substrait_type(field.data_type(), field.is_nullable()))
                .collect::<Result<Vec<_>>>()?;
            Ok(substrait::proto::Type {
                kind: Some(r#type::Kind::Struct(r#type::Struct {
                    types: field_types,
                    type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
                    nullability,
                })),
            })
        }
        DataType::Decimal128(p, s) => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Decimal(r#type::Decimal {
                type_variation_reference: DECIMAL_128_TYPE_VARIATION_REF,
                nullability,
                scale: *s as i32,
                precision: *p as i32,
            })),
        }),
        DataType::Decimal256(p, s) => Ok(substrait::proto::Type {
            kind: Some(r#type::Kind::Decimal(r#type::Decimal {
                type_variation_reference: DECIMAL_256_TYPE_VARIATION_REF,
                nullability,
                scale: *s as i32,
                precision: *p as i32,
            })),
        }),
        _ => not_impl_err!("Unsupported cast type: {dt:?}"),
    }
}

fn make_substrait_window_function(
    function_reference: u32,
    arguments: Vec<FunctionArgument>,
    partitions: Vec<Expression>,
    sorts: Vec<SortField>,
    bounds: (Bound, Bound),
    bounds_type: BoundsType,
) -> Expression {
    #[allow(deprecated)]
    Expression {
        rex_type: Some(RexType::WindowFunction(SubstraitWindowFunction {
            function_reference,
            arguments,
            partitions,
            sorts,
            options: vec![],
            output_type: None,
            phase: 0,      // default to AGGREGATION_PHASE_UNSPECIFIED
            invocation: 0, // TODO: fix
            lower_bound: Some(bounds.0),
            upper_bound: Some(bounds.1),
            args: vec![],
            bounds_type: bounds_type as i32,
        })),
    }
}

fn make_substrait_like_expr(
    producer: &mut impl SubstraitProducer,
    ignore_case: bool,
    negated: bool,
    expr: &Expr,
    pattern: &Expr,
    escape_char: Option<char>,
    schema: &DFSchemaRef,
) -> Result<Expression> {
    let function_anchor = if ignore_case {
        producer.register_function("ilike".to_string())
    } else {
        producer.register_function("like".to_string())
    };
    let expr = producer.handle_expr(expr, schema)?;
    let pattern = producer.handle_expr(pattern, schema)?;
    let escape_char = to_substrait_literal_expr(
        producer,
        &ScalarValue::Utf8(escape_char.map(|c| c.to_string())),
    )?;
    let arguments = vec![
        FunctionArgument {
            arg_type: Some(ArgType::Value(expr)),
        },
        FunctionArgument {
            arg_type: Some(ArgType::Value(pattern)),
        },
        FunctionArgument {
            arg_type: Some(ArgType::Value(escape_char)),
        },
    ];

    #[allow(deprecated)]
    let substrait_like = Expression {
        rex_type: Some(RexType::ScalarFunction(ScalarFunction {
            function_reference: function_anchor,
            arguments,
            output_type: None,
            args: vec![],
            options: vec![],
        })),
    };

    if negated {
        let function_anchor = producer.register_function("not".to_string());

        #[allow(deprecated)]
        Ok(Expression {
            rex_type: Some(RexType::ScalarFunction(ScalarFunction {
                function_reference: function_anchor,
                arguments: vec![FunctionArgument {
                    arg_type: Some(ArgType::Value(substrait_like)),
                }],
                output_type: None,
                args: vec![],
                options: vec![],
            })),
        })
    } else {
        Ok(substrait_like)
    }
}

fn to_substrait_bound_offset(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::UInt8(Some(v)) => Some(*v as i64),
        ScalarValue::UInt16(Some(v)) => Some(*v as i64),
        ScalarValue::UInt32(Some(v)) => Some(*v as i64),
        ScalarValue::UInt64(Some(v)) => Some(*v as i64),
        ScalarValue::Int8(Some(v)) => Some(*v as i64),
        ScalarValue::Int16(Some(v)) => Some(*v as i64),
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::Int64(Some(v)) => Some(*v),
        _ => None,
    }
}

fn to_substrait_bound(bound: &WindowFrameBound) -> Bound {
    match bound {
        WindowFrameBound::CurrentRow => Bound {
            kind: Some(BoundKind::CurrentRow(SubstraitBound::CurrentRow {})),
        },
        WindowFrameBound::Preceding(s) => match to_substrait_bound_offset(s) {
            Some(offset) => Bound {
                kind: Some(BoundKind::Preceding(SubstraitBound::Preceding { offset })),
            },
            None => Bound {
                kind: Some(BoundKind::Unbounded(SubstraitBound::Unbounded {})),
            },
        },
        WindowFrameBound::Following(s) => match to_substrait_bound_offset(s) {
            Some(offset) => Bound {
                kind: Some(BoundKind::Following(SubstraitBound::Following { offset })),
            },
            None => Bound {
                kind: Some(BoundKind::Unbounded(SubstraitBound::Unbounded {})),
            },
        },
    }
}

fn to_substrait_bound_type(window_frame: &WindowFrame) -> Result<BoundsType> {
    match window_frame.units {
        WindowFrameUnits::Rows => Ok(BoundsType::Rows), // ROWS
        WindowFrameUnits::Range => Ok(BoundsType::Range), // RANGE
        // TODO: Support GROUPS
        unit => not_impl_err!("Unsupported window frame unit: {unit:?}"),
    }
}

fn to_substrait_bounds(window_frame: &WindowFrame) -> Result<(Bound, Bound)> {
    Ok((
        to_substrait_bound(&window_frame.start_bound),
        to_substrait_bound(&window_frame.end_bound),
    ))
}

fn to_substrait_literal(
    producer: &mut impl SubstraitProducer,
    value: &ScalarValue,
) -> Result<Literal> {
    if value.is_null() {
        return Ok(Literal {
            nullable: true,
            type_variation_reference: DEFAULT_TYPE_VARIATION_REF,
            literal_type: Some(LiteralType::Null(to_substrait_type(
                &value.data_type(),
                true,
            )?)),
        });
    }
    let (literal_type, type_variation_reference) = match value {
        ScalarValue::Boolean(Some(b)) => {
            (LiteralType::Boolean(*b), DEFAULT_TYPE_VARIATION_REF)
        }
        ScalarValue::Int8(Some(n)) => {
            (LiteralType::I8(*n as i32), DEFAULT_TYPE_VARIATION_REF)
        }
        ScalarValue::UInt8(Some(n)) => (
            LiteralType::I8(*n as i32),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Int16(Some(n)) => {
            (LiteralType::I16(*n as i32), DEFAULT_TYPE_VARIATION_REF)
        }
        ScalarValue::UInt16(Some(n)) => (
            LiteralType::I16(*n as i32),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Int32(Some(n)) => (LiteralType::I32(*n), DEFAULT_TYPE_VARIATION_REF),
        ScalarValue::UInt32(Some(n)) => (
            LiteralType::I32(*n as i32),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Int64(Some(n)) => (LiteralType::I64(*n), DEFAULT_TYPE_VARIATION_REF),
        ScalarValue::UInt64(Some(n)) => (
            LiteralType::I64(*n as i64),
            UNSIGNED_INTEGER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Float32(Some(f)) => {
            (LiteralType::Fp32(*f), DEFAULT_TYPE_VARIATION_REF)
        }
        ScalarValue::Float64(Some(f)) => {
            (LiteralType::Fp64(*f), DEFAULT_TYPE_VARIATION_REF)
        }
        ScalarValue::TimestampSecond(Some(t), None) => (
            LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                precision: 0,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampMillisecond(Some(t), None) => (
            LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                precision: 3,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampMicrosecond(Some(t), None) => (
            LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                precision: 6,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampNanosecond(Some(t), None) => (
            LiteralType::PrecisionTimestamp(PrecisionTimestamp {
                precision: 9,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        // If timezone is present, no matter what the actual tz value is, it indicates the
        // value of the timestamp is tied to UTC epoch. That's all that Substrait cares about.
        // As the timezone is lost, this conversion may be lossy for downstream use of the value.
        ScalarValue::TimestampSecond(Some(t), Some(_)) => (
            LiteralType::PrecisionTimestampTz(PrecisionTimestamp {
                precision: 0,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampMillisecond(Some(t), Some(_)) => (
            LiteralType::PrecisionTimestampTz(PrecisionTimestamp {
                precision: 3,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampMicrosecond(Some(t), Some(_)) => (
            LiteralType::PrecisionTimestampTz(PrecisionTimestamp {
                precision: 6,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::TimestampNanosecond(Some(t), Some(_)) => (
            LiteralType::PrecisionTimestampTz(PrecisionTimestamp {
                precision: 9,
                value: *t,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::Date32(Some(d)) => {
            (LiteralType::Date(*d), DATE_32_TYPE_VARIATION_REF)
        }
        // Date64 literal is not supported in Substrait
        ScalarValue::IntervalYearMonth(Some(i)) => (
            LiteralType::IntervalYearToMonth(IntervalYearToMonth {
                // DF only tracks total months, but there should always be 12 months in a year
                years: *i / 12,
                months: *i % 12,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::IntervalMonthDayNano(Some(i)) => (
            LiteralType::IntervalCompound(IntervalCompound {
                interval_year_to_month: Some(IntervalYearToMonth {
                    years: i.months / 12,
                    months: i.months % 12,
                }),
                interval_day_to_second: Some(IntervalDayToSecond {
                    days: i.days,
                    seconds: (i.nanoseconds / NANOSECONDS) as i32,
                    subseconds: i.nanoseconds % NANOSECONDS,
                    precision_mode: Some(PrecisionMode::Precision(9)), // nanoseconds
                }),
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::IntervalDayTime(Some(i)) => (
            LiteralType::IntervalDayToSecond(IntervalDayToSecond {
                days: i.days,
                seconds: i.milliseconds / 1000,
                subseconds: (i.milliseconds % 1000) as i64,
                precision_mode: Some(PrecisionMode::Precision(3)), // 3 for milliseconds
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::Binary(Some(b)) => (
            LiteralType::Binary(b.clone()),
            DEFAULT_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::LargeBinary(Some(b)) => (
            LiteralType::Binary(b.clone()),
            LARGE_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::BinaryView(Some(b)) => (
            LiteralType::Binary(b.clone()),
            VIEW_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::FixedSizeBinary(_, Some(b)) => (
            LiteralType::FixedBinary(b.clone()),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        ScalarValue::Utf8(Some(s)) => (
            LiteralType::String(s.clone()),
            DEFAULT_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::LargeUtf8(Some(s)) => (
            LiteralType::String(s.clone()),
            LARGE_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Utf8View(Some(s)) => (
            LiteralType::String(s.clone()),
            VIEW_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Decimal128(v, p, s) if v.is_some() => (
            LiteralType::Decimal(Decimal {
                value: v.unwrap().to_le_bytes().to_vec(),
                precision: *p as i32,
                scale: *s as i32,
            }),
            DECIMAL_128_TYPE_VARIATION_REF,
        ),
        ScalarValue::List(l) => (
            convert_array_to_literal_list(producer, l)?,
            DEFAULT_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::LargeList(l) => (
            convert_array_to_literal_list(producer, l)?,
            LARGE_CONTAINER_TYPE_VARIATION_REF,
        ),
        ScalarValue::Map(m) => {
            let map = if m.is_empty() || m.value(0).is_empty() {
                let mt = to_substrait_type(m.data_type(), m.is_nullable())?;
                let mt = match mt {
                    substrait::proto::Type {
                        kind: Some(r#type::Kind::Map(mt)),
                    } => Ok(mt.as_ref().to_owned()),
                    _ => exec_err!("Unexpected type for a map: {mt:?}"),
                }?;
                LiteralType::EmptyMap(mt)
            } else {
                let keys = (0..m.keys().len())
                    .map(|i| {
                        to_substrait_literal(
                            producer,
                            &ScalarValue::try_from_array(&m.keys(), i)?,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let values = (0..m.values().len())
                    .map(|i| {
                        to_substrait_literal(
                            producer,
                            &ScalarValue::try_from_array(&m.values(), i)?,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                let key_values = keys
                    .into_iter()
                    .zip(values.into_iter())
                    .map(|(k, v)| {
                        Ok(KeyValue {
                            key: Some(k),
                            value: Some(v),
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                LiteralType::Map(Map { key_values })
            };
            (map, DEFAULT_CONTAINER_TYPE_VARIATION_REF)
        }
        ScalarValue::Struct(s) => (
            LiteralType::Struct(Struct {
                fields: s
                    .columns()
                    .iter()
                    .map(|col| {
                        to_substrait_literal(
                            producer,
                            &ScalarValue::try_from_array(col, 0)?,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?,
            }),
            DEFAULT_TYPE_VARIATION_REF,
        ),
        _ => (
            not_impl_err!("Unsupported literal: {value:?}")?,
            DEFAULT_TYPE_VARIATION_REF,
        ),
    };

    Ok(Literal {
        nullable: false,
        type_variation_reference,
        literal_type: Some(literal_type),
    })
}

fn convert_array_to_literal_list<T: OffsetSizeTrait>(
    producer: &mut impl SubstraitProducer,
    array: &GenericListArray<T>,
) -> Result<LiteralType> {
    assert_eq!(array.len(), 1);
    let nested_array = array.value(0);

    let values = (0..nested_array.len())
        .map(|i| {
            to_substrait_literal(
                producer,
                &ScalarValue::try_from_array(&nested_array, i)?,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    if values.is_empty() {
        let lt = match to_substrait_type(array.data_type(), array.is_nullable())? {
            substrait::proto::Type {
                kind: Some(r#type::Kind::List(lt)),
            } => lt.as_ref().to_owned(),
            _ => unreachable!(),
        };
        Ok(LiteralType::EmptyList(lt))
    } else {
        Ok(LiteralType::List(List { values }))
    }
}

fn to_substrait_literal_expr(
    producer: &mut impl SubstraitProducer,
    value: &ScalarValue,
) -> Result<Expression> {
    let literal = to_substrait_literal(producer, value)?;
    Ok(Expression {
        rex_type: Some(RexType::Literal(literal)),
    })
}

/// Util to generate substrait [RexType::ScalarFunction] with one argument
fn to_substrait_unary_scalar_fn(
    producer: &mut impl SubstraitProducer,
    fn_name: &str,
    arg: &Expr,
    schema: &DFSchemaRef,
) -> Result<Expression> {
    let function_anchor = producer.register_function(fn_name.to_string());
    let substrait_expr = producer.handle_expr(arg, schema)?;

    Ok(Expression {
        rex_type: Some(RexType::ScalarFunction(ScalarFunction {
            function_reference: function_anchor,
            arguments: vec![FunctionArgument {
                arg_type: Some(ArgType::Value(substrait_expr)),
            }],
            output_type: None,
            options: vec![],
            ..Default::default()
        })),
    })
}

/// Try to convert an [Expr] to a [FieldReference].
/// Returns `Err` if the [Expr] is not a [Expr::Column].
fn try_to_substrait_field_reference(
    expr: &Expr,
    schema: &DFSchemaRef,
) -> Result<FieldReference> {
    match expr {
        Expr::Column(col) => {
            let index = schema.index_of_column(col)?;
            Ok(FieldReference {
                reference_type: Some(ReferenceType::DirectReference(ReferenceSegment {
                    reference_type: Some(reference_segment::ReferenceType::StructField(
                        Box::new(reference_segment::StructField {
                            field: index as i32,
                            child: None,
                        }),
                    )),
                })),
                root_type: Some(RootType::RootReference(RootReference {})),
            })
        }
        _ => substrait_err!("Expect a `Column` expr, but found {expr:?}"),
    }
}

fn substrait_sort_field(
    producer: &mut impl SubstraitProducer,
    sort: &SortExpr,
    schema: &DFSchemaRef,
) -> Result<SortField> {
    let SortExpr {
        expr,
        asc,
        nulls_first,
    } = sort;
    let e = producer.handle_expr(expr, schema)?;
    let d = match (asc, nulls_first) {
        (true, true) => SortDirection::AscNullsFirst,
        (true, false) => SortDirection::AscNullsLast,
        (false, true) => SortDirection::DescNullsFirst,
        (false, false) => SortDirection::DescNullsLast,
    };
    Ok(SortField {
        expr: Some(e),
        sort_kind: Some(SortKind::Direction(d as i32)),
    })
}

fn substrait_field_ref(index: usize) -> Result<Expression> {
    Ok(Expression {
        rex_type: Some(RexType::Selection(Box::new(FieldReference {
            reference_type: Some(ReferenceType::DirectReference(ReferenceSegment {
                reference_type: Some(reference_segment::ReferenceType::StructField(
                    Box::new(reference_segment::StructField {
                        field: index as i32,
                        child: None,
                    }),
                )),
            })),
            root_type: Some(RootType::RootReference(RootReference {})),
        }))),
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::logical_plan::consumer::{
        from_substrait_extended_expr, from_substrait_literal_without_names,
        from_substrait_named_struct, from_substrait_type_without_names,
        DefaultSubstraitConsumer,
    };
    use arrow::array::types::{IntervalDayTime, IntervalMonthDayNano};
    use datafusion::arrow;
    use datafusion::arrow::array::{
        GenericListArray, Int64Builder, MapBuilder, StringBuilder,
    };
    use datafusion::arrow::datatypes::{Field, Fields, Schema};
    use datafusion::common::scalar::ScalarStructBuilder;
    use datafusion::common::DFSchema;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::prelude::SessionContext;
    use std::sync::LazyLock;

    static TEST_SESSION_STATE: LazyLock<SessionState> =
        LazyLock::new(|| SessionContext::default().state());
    static TEST_EXTENSIONS: LazyLock<Extensions> = LazyLock::new(Extensions::default);
    fn test_consumer() -> DefaultSubstraitConsumer<'static> {
        let extensions = &TEST_EXTENSIONS;
        let state = &TEST_SESSION_STATE;
        DefaultSubstraitConsumer::new(extensions, state)
    }

    #[test]
    fn round_trip_literals() -> Result<()> {
        round_trip_literal(ScalarValue::Boolean(None))?;
        round_trip_literal(ScalarValue::Boolean(Some(true)))?;
        round_trip_literal(ScalarValue::Boolean(Some(false)))?;

        round_trip_literal(ScalarValue::Int8(None))?;
        round_trip_literal(ScalarValue::Int8(Some(i8::MIN)))?;
        round_trip_literal(ScalarValue::Int8(Some(i8::MAX)))?;
        round_trip_literal(ScalarValue::UInt8(None))?;
        round_trip_literal(ScalarValue::UInt8(Some(u8::MIN)))?;
        round_trip_literal(ScalarValue::UInt8(Some(u8::MAX)))?;

        round_trip_literal(ScalarValue::Int16(None))?;
        round_trip_literal(ScalarValue::Int16(Some(i16::MIN)))?;
        round_trip_literal(ScalarValue::Int16(Some(i16::MAX)))?;
        round_trip_literal(ScalarValue::UInt16(None))?;
        round_trip_literal(ScalarValue::UInt16(Some(u16::MIN)))?;
        round_trip_literal(ScalarValue::UInt16(Some(u16::MAX)))?;

        round_trip_literal(ScalarValue::Int32(None))?;
        round_trip_literal(ScalarValue::Int32(Some(i32::MIN)))?;
        round_trip_literal(ScalarValue::Int32(Some(i32::MAX)))?;
        round_trip_literal(ScalarValue::UInt32(None))?;
        round_trip_literal(ScalarValue::UInt32(Some(u32::MIN)))?;
        round_trip_literal(ScalarValue::UInt32(Some(u32::MAX)))?;

        round_trip_literal(ScalarValue::Int64(None))?;
        round_trip_literal(ScalarValue::Int64(Some(i64::MIN)))?;
        round_trip_literal(ScalarValue::Int64(Some(i64::MAX)))?;
        round_trip_literal(ScalarValue::UInt64(None))?;
        round_trip_literal(ScalarValue::UInt64(Some(u64::MIN)))?;
        round_trip_literal(ScalarValue::UInt64(Some(u64::MAX)))?;

        for (ts, tz) in [
            (Some(12345), None),
            (None, None),
            (Some(12345), Some("UTC".into())),
            (None, Some("UTC".into())),
        ] {
            round_trip_literal(ScalarValue::TimestampSecond(ts, tz.clone()))?;
            round_trip_literal(ScalarValue::TimestampMillisecond(ts, tz.clone()))?;
            round_trip_literal(ScalarValue::TimestampMicrosecond(ts, tz.clone()))?;
            round_trip_literal(ScalarValue::TimestampNanosecond(ts, tz))?;
        }

        round_trip_literal(ScalarValue::List(ScalarValue::new_list_nullable(
            &[ScalarValue::Float32(Some(1.0))],
            &DataType::Float32,
        )))?;
        round_trip_literal(ScalarValue::List(ScalarValue::new_list_nullable(
            &[],
            &DataType::Float32,
        )))?;
        round_trip_literal(ScalarValue::List(Arc::new(GenericListArray::new_null(
            Field::new_list_field(DataType::Float32, true).into(),
            1,
        ))))?;
        round_trip_literal(ScalarValue::LargeList(ScalarValue::new_large_list(
            &[ScalarValue::Float32(Some(1.0))],
            &DataType::Float32,
        )))?;
        round_trip_literal(ScalarValue::LargeList(ScalarValue::new_large_list(
            &[],
            &DataType::Float32,
        )))?;
        round_trip_literal(ScalarValue::LargeList(Arc::new(
            GenericListArray::new_null(
                Field::new_list_field(DataType::Float32, true).into(),
                1,
            ),
        )))?;

        // Null map
        let mut map_builder =
            MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        map_builder.append(false)?;
        round_trip_literal(ScalarValue::Map(Arc::new(map_builder.finish())))?;

        // Empty map
        let mut map_builder =
            MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        map_builder.append(true)?;
        round_trip_literal(ScalarValue::Map(Arc::new(map_builder.finish())))?;

        // Valid map
        let mut map_builder =
            MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
        map_builder.keys().append_value("key1");
        map_builder.keys().append_value("key2");
        map_builder.values().append_value(1);
        map_builder.values().append_value(2);
        map_builder.append(true)?;
        round_trip_literal(ScalarValue::Map(Arc::new(map_builder.finish())))?;

        let c0 = Field::new("c0", DataType::Boolean, true);
        let c1 = Field::new("c1", DataType::Int32, true);
        let c2 = Field::new("c2", DataType::Utf8, true);
        round_trip_literal(
            ScalarStructBuilder::new()
                .with_scalar(c0.to_owned(), ScalarValue::Boolean(Some(true)))
                .with_scalar(c1.to_owned(), ScalarValue::Int32(Some(1)))
                .with_scalar(c2.to_owned(), ScalarValue::Utf8(None))
                .build()?,
        )?;
        round_trip_literal(ScalarStructBuilder::new_null(vec![c0, c1, c2]))?;

        round_trip_literal(ScalarValue::IntervalYearMonth(Some(17)))?;
        round_trip_literal(ScalarValue::IntervalMonthDayNano(Some(
            IntervalMonthDayNano::new(17, 25, 1234567890),
        )))?;
        round_trip_literal(ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(
            57, 123456,
        ))))?;

        Ok(())
    }

    fn round_trip_literal(scalar: ScalarValue) -> Result<()> {
        println!("Checking round trip of {scalar:?}");
        let state = SessionContext::default().state();
        let mut producer = DefaultSubstraitProducer::new(&state);
        let substrait_literal = to_substrait_literal(&mut producer, &scalar)?;
        let roundtrip_scalar =
            from_substrait_literal_without_names(&test_consumer(), &substrait_literal)?;
        assert_eq!(scalar, roundtrip_scalar);
        Ok(())
    }

    #[test]
    fn round_trip_types() -> Result<()> {
        round_trip_type(DataType::Boolean)?;
        round_trip_type(DataType::Int8)?;
        round_trip_type(DataType::UInt8)?;
        round_trip_type(DataType::Int16)?;
        round_trip_type(DataType::UInt16)?;
        round_trip_type(DataType::Int32)?;
        round_trip_type(DataType::UInt32)?;
        round_trip_type(DataType::Int64)?;
        round_trip_type(DataType::UInt64)?;
        round_trip_type(DataType::Float32)?;
        round_trip_type(DataType::Float64)?;

        for tz in [None, Some("UTC".into())] {
            round_trip_type(DataType::Timestamp(TimeUnit::Second, tz.clone()))?;
            round_trip_type(DataType::Timestamp(TimeUnit::Millisecond, tz.clone()))?;
            round_trip_type(DataType::Timestamp(TimeUnit::Microsecond, tz.clone()))?;
            round_trip_type(DataType::Timestamp(TimeUnit::Nanosecond, tz))?;
        }

        round_trip_type(DataType::Date32)?;
        round_trip_type(DataType::Date64)?;
        round_trip_type(DataType::Binary)?;
        round_trip_type(DataType::FixedSizeBinary(10))?;
        round_trip_type(DataType::LargeBinary)?;
        round_trip_type(DataType::BinaryView)?;
        round_trip_type(DataType::Utf8)?;
        round_trip_type(DataType::LargeUtf8)?;
        round_trip_type(DataType::Utf8View)?;
        round_trip_type(DataType::Decimal128(10, 2))?;
        round_trip_type(DataType::Decimal256(30, 2))?;

        round_trip_type(DataType::List(
            Field::new_list_field(DataType::Int32, true).into(),
        ))?;
        round_trip_type(DataType::LargeList(
            Field::new_list_field(DataType::Int32, true).into(),
        ))?;

        round_trip_type(DataType::Map(
            Field::new_struct(
                "entries",
                [
                    Field::new("key", DataType::Utf8, false).into(),
                    Field::new("value", DataType::Int32, true).into(),
                ],
                false,
            )
            .into(),
            false,
        ))?;

        round_trip_type(DataType::Struct(
            vec![
                Field::new("c0", DataType::Int32, true),
                Field::new("c1", DataType::Utf8, true),
            ]
            .into(),
        ))?;

        round_trip_type(DataType::Interval(IntervalUnit::YearMonth))?;
        round_trip_type(DataType::Interval(IntervalUnit::MonthDayNano))?;
        round_trip_type(DataType::Interval(IntervalUnit::DayTime))?;

        Ok(())
    }

    fn round_trip_type(dt: DataType) -> Result<()> {
        println!("Checking round trip of {dt:?}");

        // As DataFusion doesn't consider nullability as a property of the type, but field,
        // it doesn't matter if we set nullability to true or false here.
        let substrait = to_substrait_type(&dt, true)?;
        let consumer = test_consumer();
        let roundtrip_dt = from_substrait_type_without_names(&consumer, &substrait)?;
        assert_eq!(dt, roundtrip_dt);
        Ok(())
    }

    #[test]
    fn to_field_reference() -> Result<()> {
        let expression = substrait_field_ref(2)?;

        match &expression.rex_type {
            Some(RexType::Selection(field_ref)) => {
                assert_eq!(
                    field_ref
                        .root_type
                        .clone()
                        .expect("root type should be set"),
                    RootType::RootReference(RootReference {})
                );
            }

            _ => panic!("Should not be anything other than field reference"),
        }
        Ok(())
    }

    #[test]
    fn named_struct_names() -> Result<()> {
        let schema = DFSchemaRef::new(DFSchema::try_from(Schema::new(vec![
            Field::new("int", DataType::Int32, true),
            Field::new(
                "struct",
                DataType::Struct(Fields::from(vec![Field::new(
                    "inner",
                    DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                    true,
                )])),
                true,
            ),
            Field::new("trailer", DataType::Float64, true),
        ]))?);

        let named_struct = to_substrait_named_struct(&schema)?;

        // Struct field names should be flattened DFS style
        // List field names should be omitted
        assert_eq!(
            named_struct.names,
            vec!["int", "struct", "inner", "trailer"]
        );

        let roundtrip_schema =
            from_substrait_named_struct(&test_consumer(), &named_struct)?;
        assert_eq!(schema.as_ref(), &roundtrip_schema);
        Ok(())
    }

    #[tokio::test]
    async fn extended_expressions() -> Result<()> {
        let state = SessionStateBuilder::default().build();

        // One expression, empty input schema
        let expr = Expr::Literal(ScalarValue::Int32(Some(42)));
        let field = Field::new("out", DataType::Int32, false);
        let empty_schema = DFSchemaRef::new(DFSchema::empty());
        let substrait =
            to_substrait_extended_expr(&[(&expr, &field)], &empty_schema, &state)?;
        let roundtrip_expr = from_substrait_extended_expr(&state, &substrait).await?;

        assert_eq!(roundtrip_expr.input_schema, empty_schema);
        assert_eq!(roundtrip_expr.exprs.len(), 1);

        let (rt_expr, rt_field) = roundtrip_expr.exprs.first().unwrap();
        assert_eq!(rt_field, &field);
        assert_eq!(rt_expr, &expr);

        // Multiple expressions, with column references
        let expr1 = Expr::Column("c0".into());
        let expr2 = Expr::Column("c1".into());
        let out1 = Field::new("out1", DataType::Int32, true);
        let out2 = Field::new("out2", DataType::Utf8, true);
        let input_schema = DFSchemaRef::new(DFSchema::try_from(Schema::new(vec![
            Field::new("c0", DataType::Int32, true),
            Field::new("c1", DataType::Utf8, true),
        ]))?);

        let substrait = to_substrait_extended_expr(
            &[(&expr1, &out1), (&expr2, &out2)],
            &input_schema,
            &state,
        )?;
        let roundtrip_expr = from_substrait_extended_expr(&state, &substrait).await?;

        assert_eq!(roundtrip_expr.input_schema, input_schema);
        assert_eq!(roundtrip_expr.exprs.len(), 2);

        let mut exprs = roundtrip_expr.exprs.into_iter();

        let (rt_expr, rt_field) = exprs.next().unwrap();
        assert_eq!(rt_field, out1);
        assert_eq!(rt_expr, expr1);

        let (rt_expr, rt_field) = exprs.next().unwrap();
        assert_eq!(rt_field, out2);
        assert_eq!(rt_expr, expr2);

        Ok(())
    }

    #[tokio::test]
    async fn invalid_extended_expression() {
        let state = SessionStateBuilder::default().build();

        // Not ok if input schema is missing field referenced by expr
        let expr = Expr::Column("missing".into());
        let field = Field::new("out", DataType::Int32, false);
        let empty_schema = DFSchemaRef::new(DFSchema::empty());

        let err = to_substrait_extended_expr(&[(&expr, &field)], &empty_schema, &state);

        assert!(matches!(err, Err(DataFusionError::SchemaError(_, _))));
    }
}
