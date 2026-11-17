// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Resolve anonymous struct field names before constructing DataFusion plans.
//!
//! Type shapes describe only nesting and field-name variables. Leaf types,
//! nullability and coercion remain the responsibility of the existing consumer.
//! References share their producer's shape, so constraints cross projections and
//! relation boundaries without inspecting or rebuilding DataFusion plans.

use super::{
    SubstraitConsumer, SubstraitConsumerContext, substrait_fun_name, substrait_to_df_name,
};
use crate::variation_const::DICTIONARY_MAP_TYPE_VARIATION_REF;
use datafusion::execution::FunctionRegistry;
use datafusion::functions::core::coalesce::CoalesceFunc;
use datafusion::logical_expr::{
    ArrayFunctionArgument, ArrayFunctionSignature, TypeSignature,
};
use std::collections::HashMap;
use std::sync::{Arc, Weak};
use substrait::proto::expression::field_reference::RootType;
use substrait::proto::expression::literal::LiteralType;
use substrait::proto::expression::reference_segment::ReferenceType;
use substrait::proto::expression::{Cast, Literal, RexType};
use substrait::proto::function_argument::ArgType;
use substrait::proto::rel::RelType;
use substrait::proto::rel_common::EmitKind;
use substrait::proto::r#type::Kind;
use substrait::proto::{Expression, Rel, RelCommon, Type};

type ShapeId = usize;
type NameId = usize;

#[derive(Clone)]
enum Shape {
    Leaf,
    Struct(Vec<(NameId, ShapeId)>),
    List(ShapeId),
    Map(ShapeId, ShapeId),
}

struct Name {
    parent: NameId,
    value: Option<String>,
    conflicting: bool,
}

enum FunctionRule {
    CommonResult,
    ArrayArguments(Vec<ArrayFunctionArgument>),
}

// Addresses identify nodes of the borrowed, immutable protobuf tree, never their
// contents: identical literals can require different names. They are never
// dereferenced. The annotation table is used only while consuming that same tree.
#[derive(Debug, Hash, PartialEq, Eq)]
enum NodeId {
    Literal(usize),
    Cast(usize),
}

impl NodeId {
    fn literal(literal: &Literal) -> Self {
        Self::Literal(std::ptr::from_ref(literal).addr())
    }

    fn cast(cast: &Cast) -> Self {
        Self::Cast(std::ptr::from_ref(cast).addr())
    }
}

#[derive(Debug)]
pub(super) struct ResolvedNames {
    names: HashMap<NodeId, Vec<String>>,
    // A custom consumer may save a context beyond this conversion. Such a
    // context must not apply node identities to a later protobuf allocation.
    scope: Weak<()>,
}

impl ResolvedNames {
    pub(super) fn for_rel(
        consumer: &impl SubstraitConsumer,
        rel: &Rel,
        context: &SubstraitConsumerContext,
    ) -> (SubstraitConsumerContext, Arc<()>) {
        let mut inference = Inference::default();
        for (anchor, name) in &consumer.get_extensions().functions {
            let name = substrait_fun_name(name);
            let registry = consumer.get_function_registry();
            if registry.higher_order_function(name).is_ok() {
                continue;
            }
            let function = registry.udf(name).or_else(|error| {
                substrait_to_df_name(name)
                    .map(|name| registry.udf(name))
                    .unwrap_or(Err(error))
            });
            let Ok(function) = function else { continue };
            let rule = if function.inner().is::<CoalesceFunc>() {
                Some(FunctionRule::CommonResult)
            } else if let TypeSignature::ArraySignature(ArrayFunctionSignature::Array {
                arguments,
                ..
            }) = &function.signature().type_signature
            {
                Some(FunctionRule::ArrayArguments(arguments.clone()))
            } else {
                None
            };
            if let Some(rule) = rule {
                inference.functions.insert(*anchor, rule);
            }
        }
        inference.rel(rel);
        let scope = Arc::new(());
        let resolved = inference.finish(Arc::downgrade(&scope));
        (context.with_extension(Arc::new(resolved)), scope)
    }

    pub(super) fn is_active(&self) -> bool {
        self.scope.strong_count() != 0
    }

    pub(super) fn literal(&self, literal: &Literal) -> Option<&Vec<String>> {
        if self.is_active() {
            self.names.get(&NodeId::literal(literal))
        } else {
            None
        }
    }

    pub(super) fn cast(&self, cast: &Cast) -> Option<&Vec<String>> {
        if self.is_active() {
            self.names.get(&NodeId::cast(cast))
        } else {
            None
        }
    }
}

#[derive(Default)]
struct Inference {
    shapes: Vec<Shape>,
    names: Vec<Name>,
    annotations: Vec<(NodeId, ShapeId)>,
    functions: HashMap<u32, FunctionRule>,
}

impl Inference {
    fn shape(&mut self, shape: Shape) -> ShapeId {
        let id = self.shapes.len();
        self.shapes.push(shape);
        id
    }

    fn name(&mut self, value: Option<String>) -> NameId {
        let id = self.names.len();
        self.names.push(Name {
            parent: id,
            value,
            conflicting: false,
        });
        id
    }

    fn root(&mut self, id: NameId) -> NameId {
        let parent = self.names[id].parent;
        if parent != id {
            self.names[id].parent = self.root(parent);
        }
        self.names[id].parent
    }

    fn equate_names(&mut self, left: NameId, right: NameId) {
        let left = self.root(left);
        let right = self.root(right);
        if left == right {
            return;
        }
        let right_value = self.names[right].value.take();
        self.names[left].conflicting |= self.names[right].conflicting
            || matches!((&self.names[left].value, &right_value), (Some(l), Some(r)) if l != r);
        if self.names[left].value.is_none() {
            self.names[left].value = right_value;
        }
        self.names[right].parent = left;
    }

    fn equate(&mut self, left: ShapeId, right: ShapeId) {
        if left == right {
            return;
        }
        match (self.shapes[left].clone(), self.shapes[right].clone()) {
            (Shape::Struct(left), Shape::Struct(right)) if left.len() == right.len() => {
                for ((ln, lt), (rn, rt)) in left.into_iter().zip(right) {
                    self.equate_names(ln, rn);
                    self.equate(lt, rt);
                }
            }
            (Shape::List(left), Shape::List(right)) => self.equate(left, right),
            (Shape::Map(lk, lv), Shape::Map(rk, rv)) => {
                self.equate(lk, rk);
                self.equate(lv, rv);
            }
            _ => {}
        }
    }

    fn common_result(&mut self, results: &[ShapeId]) -> ShapeId {
        let result = results
            .iter()
            .copied()
            .find(|id| !matches!(self.shapes[*id], Shape::Leaf))
            .or_else(|| results.first().copied())
            .unwrap_or_else(|| self.shape(Shape::Leaf));
        for other in results {
            self.equate(result, *other);
        }
        result
    }

    fn data_type(
        &mut self,
        ty: &Type,
        names: &mut impl Iterator<Item = String>,
    ) -> ShapeId {
        let shape = match ty.kind.as_ref() {
            Some(Kind::Struct(s)) => Shape::Struct(
                s.types
                    .iter()
                    .map(|ty| {
                        let name = self.name(names.next());
                        (name, self.data_type(ty, names))
                    })
                    .collect(),
            ),
            Some(Kind::List(list)) => {
                let element = self.optional_type(list.r#type.as_deref(), names);
                Shape::List(element)
            }
            Some(Kind::Map(map)) => {
                let key = self.optional_type(map.key.as_deref(), names);
                let value = self.optional_type(map.value.as_deref(), names);
                if map.type_variation_reference == DICTIONARY_MAP_TYPE_VARIATION_REF {
                    return value;
                }
                Shape::Map(key, value)
            }
            _ => Shape::Leaf,
        };
        self.shape(shape)
    }

    fn optional_type(
        &mut self,
        ty: Option<&Type>,
        names: &mut impl Iterator<Item = String>,
    ) -> ShapeId {
        ty.map(|ty| self.data_type(ty, names))
            .unwrap_or_else(|| self.shape(Shape::Leaf))
    }

    fn literal(&mut self, literal: &Literal) -> ShapeId {
        let shape = match literal.literal_type.as_ref() {
            Some(LiteralType::Null(ty)) => {
                return self.data_type(ty, &mut std::iter::empty());
            }
            Some(LiteralType::Struct(s)) => Shape::Struct(
                s.fields
                    .iter()
                    .map(|field| (self.name(None), self.literal(field)))
                    .collect(),
            ),
            Some(LiteralType::List(list)) => {
                let mut values = list.values.iter();
                let element = values
                    .next()
                    .map(|v| self.literal(v))
                    .unwrap_or_else(|| self.shape(Shape::Leaf));
                for value in values {
                    let value = self.literal(value);
                    self.equate(element, value);
                }
                Shape::List(element)
            }
            Some(LiteralType::EmptyList(list)) => Shape::List(
                self.optional_type(list.r#type.as_deref(), &mut std::iter::empty()),
            ),
            Some(LiteralType::EmptyMap(map)) => Shape::Map(
                self.optional_type(map.key.as_deref(), &mut std::iter::empty()),
                self.optional_type(map.value.as_deref(), &mut std::iter::empty()),
            ),
            Some(LiteralType::Map(map)) => {
                let keys = map
                    .key_values
                    .iter()
                    .filter_map(|entry| entry.key.as_ref())
                    .map(|key| self.literal(key))
                    .collect::<Vec<_>>();
                let values = map
                    .key_values
                    .iter()
                    .filter_map(|entry| entry.value.as_ref())
                    .map(|value| self.literal(value))
                    .collect::<Vec<_>>();
                Shape::Map(self.common_result(&keys), self.common_result(&values))
            }
            _ => Shape::Leaf,
        };
        self.shape(shape)
    }

    fn expression(&mut self, expr: &Expression, input: &[ShapeId]) -> ShapeId {
        match expr.rex_type.as_ref() {
            Some(RexType::Literal(literal)) => {
                let shape = self.literal(literal);
                self.annotations.push((NodeId::literal(literal), shape));
                shape
            }
            Some(RexType::Selection(selection))
                if matches!(
                    selection.root_type,
                    None | Some(RootType::RootReference(_))
                ) =>
            {
                use substrait::proto::expression::field_reference::ReferenceType::DirectReference;
                if let Some(DirectReference(reference)) = &selection.reference_type
                    && let Some(ReferenceType::StructField(field)) =
                        &reference.reference_type
                    && field.child.is_none()
                    && let Ok(index) = usize::try_from(field.field)
                    && let Some(shape) = input.get(index)
                {
                    return *shape;
                }
                self.shape(Shape::Leaf)
            }
            Some(RexType::Cast(cast)) => {
                let input = cast.input.as_ref().map(|expr| self.expression(expr, input));
                let shape =
                    self.optional_type(cast.r#type.as_ref(), &mut std::iter::empty());
                if let Some(input) = input {
                    self.equate(shape, input);
                }
                self.annotations.push((NodeId::cast(cast), shape));
                shape
            }
            Some(RexType::IfThen(case)) => {
                let mut results = vec![];
                for branch in &case.ifs {
                    if let Some(condition) = &branch.r#if {
                        self.expression(condition, input);
                    }
                    if let Some(result) = &branch.then {
                        results.push(self.expression(result, input));
                    }
                }
                if let Some(result) = &case.r#else {
                    results.push(self.expression(result, input));
                }
                self.common_result(&results)
            }
            Some(RexType::ScalarFunction(function)) => {
                let mut arguments = vec![];
                for arg in &function.arguments {
                    if let Some(ArgType::Value(expr)) = &arg.arg_type {
                        arguments.push(self.expression(expr, input));
                    }
                }
                let output = self.optional_type(
                    function.output_type.as_ref(),
                    &mut std::iter::empty(),
                );
                match self.functions.get(&function.function_reference) {
                    Some(FunctionRule::CommonResult) => {
                        for argument in arguments {
                            self.equate(output, argument);
                        }
                    }
                    Some(FunctionRule::ArrayArguments(signature))
                        if signature.len() == arguments.len() =>
                    {
                        let elements = signature
                            .iter()
                            .zip(arguments)
                            .filter_map(|(kind, shape)| match kind {
                                ArrayFunctionArgument::Array => {
                                    match self.shapes[shape] {
                                        Shape::List(element) => Some(element),
                                        _ => None,
                                    }
                                }
                                ArrayFunctionArgument::Element => Some(shape),
                                _ => None,
                            })
                            .collect::<Vec<_>>();
                        if let Some(first) = elements.first() {
                            for element in &elements {
                                self.equate(*first, *element);
                            }
                        }
                    }
                    _ => {}
                }
                output
            }
            Some(RexType::SingularOrList(list)) => {
                let value = list.value.as_ref().map(|v| self.expression(v, input));
                for option in &list.options {
                    let option = self.expression(option, input);
                    if let Some(value) = value {
                        self.equate(value, option);
                    }
                }
                self.shape(Shape::Leaf)
            }
            _ => self.shape(Shape::Leaf),
        }
    }

    fn input(&mut self, rel: Option<&Rel>) -> Option<Vec<ShapeId>> {
        self.rel(rel?)
    }

    #[expect(deprecated)]
    fn rel(&mut self, rel: &Rel) -> Option<Vec<ShapeId>> {
        let (output, common) = match rel.rel_type.as_ref() {
            Some(RelType::Read(read)) => {
                let mut output = vec![];
                if let Some(schema) = &read.base_schema
                    && let Some(s) = &schema.r#struct
                {
                    let mut names = schema.names.iter().cloned();
                    for ty in &s.types {
                        names.next();
                        output.push(self.data_type(ty, &mut names));
                    }
                }
                if let Some(filter) = &read.filter {
                    self.expression(filter, &output);
                }
                if let Some(select) =
                    read.projection.as_ref().and_then(|p| p.select.as_ref())
                {
                    output = select
                        .struct_items
                        .iter()
                        .filter_map(|item| output.get(item.field as usize).copied())
                        .collect();
                }
                (output, read.common.as_ref())
            }
            Some(RelType::Project(project)) => {
                let input = self.input(project.input.as_deref())?;
                let mut output = input.clone();
                for expr in &project.expressions {
                    output.push(self.expression(expr, &input));
                }
                (output, project.common.as_ref())
            }
            Some(RelType::Filter(filter)) => {
                let input = self.input(filter.input.as_deref())?;
                if let Some(condition) = &filter.condition {
                    self.expression(condition, &input);
                }
                (input, filter.common.as_ref())
            }
            Some(RelType::Fetch(fetch)) => {
                (self.input(fetch.input.as_deref())?, fetch.common.as_ref())
            }
            Some(RelType::Sort(sort)) => {
                let input = self.input(sort.input.as_deref())?;
                for sort in &sort.sorts {
                    if let Some(expr) = &sort.expr {
                        self.expression(expr, &input);
                    }
                }
                (input, sort.common.as_ref())
            }
            Some(RelType::Exchange(exchange)) => (
                self.input(exchange.input.as_deref())?,
                exchange.common.as_ref(),
            ),
            Some(RelType::Set(set)) => {
                let inputs = set
                    .inputs
                    .iter()
                    .map(|input| self.rel(input))
                    .collect::<Option<Vec<_>>>()?;
                let width = inputs.first()?.len();
                if inputs.iter().any(|input| input.len() != width) {
                    return None;
                }
                let output = (0..width)
                    .map(|column| {
                        let results =
                            inputs.iter().map(|input| input[column]).collect::<Vec<_>>();
                        self.common_result(&results)
                    })
                    .collect();
                (output, set.common.as_ref())
            }
            Some(RelType::Aggregate(aggregate)) => {
                let input = self.input(aggregate.input.as_deref())?;
                let groups = aggregate
                    .grouping_expressions
                    .iter()
                    .map(|e| self.expression(e, &input))
                    .collect::<Vec<_>>();
                let mut output = vec![];
                if let [grouping] = aggregate.groupings.as_slice() {
                    if grouping.grouping_expressions.is_empty() {
                        output.extend(
                            grouping
                                .expression_references
                                .iter()
                                .filter_map(|i| groups.get(*i as usize).copied()),
                        );
                    } else {
                        for expr in &grouping.grouping_expressions {
                            output.push(self.expression(expr, &input));
                        }
                    }
                } else if aggregate.groupings.len() > 1 {
                    // Grouping sets have an additional output and deduplicate
                    // expressions; leave their output lineage unresolved.
                    return None;
                }
                for measure in &aggregate.measures {
                    let ty = measure
                        .measure
                        .as_ref()
                        .and_then(|m| m.output_type.as_ref());
                    output.push(self.optional_type(ty, &mut std::iter::empty()));
                }
                (output, aggregate.common.as_ref())
            }
            _ => return None,
        };
        Self::emit(output, common)
    }

    fn emit(output: Vec<ShapeId>, common: Option<&RelCommon>) -> Option<Vec<ShapeId>> {
        match common.and_then(|common| common.emit_kind.as_ref()) {
            Some(EmitKind::Emit(emit)) => emit
                .output_mapping
                .iter()
                .map(|i| output.get(*i as usize).copied())
                .collect(),
            _ => Some(output),
        }
    }

    fn collect_names(&mut self, shape: ShapeId, names: &mut Vec<String>) {
        match self.shapes[shape].clone() {
            Shape::Struct(fields) => {
                for (index, (name, child)) in fields.into_iter().enumerate() {
                    let root = self.root(name);
                    let name = &self.names[root];
                    names.push(
                        if name.conflicting {
                            None
                        } else {
                            name.value.clone()
                        }
                        .unwrap_or_else(|| format!("c{index}")),
                    );
                    self.collect_names(child, names);
                }
            }
            Shape::List(element) => self.collect_names(element, names),
            Shape::Map(key, value) => {
                self.collect_names(key, names);
                self.collect_names(value, names);
            }
            Shape::Leaf => {}
        }
    }

    fn finish(mut self, scope: Weak<()>) -> ResolvedNames {
        let annotations = std::mem::take(&mut self.annotations);
        ResolvedNames {
            names: annotations
                .into_iter()
                .filter_map(|(node, shape)| {
                    let mut names = vec![];
                    self.collect_names(shape, &mut names);
                    (!names.is_empty()).then_some((node, names))
                })
                .collect(),
            scope,
        }
    }
}
