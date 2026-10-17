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

//! Statistics-based pruning for calls to functions with the
//! [`NestedInputAccess`] capability, such as `get_field(s, 'a')`.
//!
//! [`PruningPredicate`](datafusion_pruning::PruningPredicate) only understands
//! columns. So, for each file, every such call in the predicate is replaced by
//! a placeholder column, for example `get_field(s, 'a') > 5` becomes
//! `__datafusion_nested_input_0 > 5`. When pruning asks for the placeholder's
//! statistics, the statistics of the Parquet leaves the call reads are handed
//! to [`NestedInputAccess::output_statistics`], which derives the statistics
//! of the call's output for each container (row group or page).

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{ColumnStatistics, Result, ScalarValue, Statistics};
use datafusion_expr::{FieldPath, ReturnFieldArgs, ScalarUDF};
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use parquet::schema::types::SchemaDescriptor;

/// Prefix of the placeholder columns. Reserved for internal use.
const PLACEHOLDER_PREFIX: &str = "__datafusion_nested_input_";

/// The placeholders introduced for one file's pruning predicate.
#[derive(Debug)]
pub(crate) struct NestedPruning {
    /// The file schema followed by one field per placeholder.
    schema: SchemaRef,
    /// Placeholder name to the call it replaced.
    calls: HashMap<String, NestedPruningCall>,
}

/// A call replaced by a placeholder column.
#[derive(Debug)]
pub(crate) struct NestedPruningCall {
    udf: ScalarUDF,
    arg_fields: Vec<FieldRef>,
    literals: Vec<Option<ScalarValue>>,
    return_type: DataType,
    /// One entry per path from `required_fields`, in order: the Parquet leaf
    /// it names, or `None` if it does not name a primitive leaf.
    inputs: Vec<Option<NestedLeaf>>,
}

/// A primitive Parquet leaf read by a call.
#[derive(Debug)]
pub(crate) struct NestedLeaf {
    pub(crate) parquet_index: usize,
    pub(crate) field: FieldRef,
}

/// Statistics of one call input, one entry per container. Null entries are
/// unknown.
pub(crate) struct LeafStatistics {
    pub(crate) mins: Option<ArrayRef>,
    pub(crate) maxes: Option<ArrayRef>,
    pub(crate) null_counts: Option<UInt64Array>,
}

impl NestedPruning {
    /// Replaces every call to a [`NestedInputAccess`] function in `predicate`
    /// with a placeholder column. Returns `None` if there is no such call.
    ///
    /// [`NestedInputAccess`]: datafusion_expr::NestedInputAccess
    pub(crate) fn rewrite(
        predicate: &Arc<dyn PhysicalExpr>,
        file_schema: &SchemaRef,
        parquet_schema: &SchemaDescriptor,
    ) -> Result<Option<(Arc<dyn PhysicalExpr>, Self)>> {
        let mut fields = file_schema.fields().to_vec();
        let mut calls = HashMap::new();

        let rewritten = Arc::clone(predicate).transform_down(|expr| {
            let Some(func) = expr.downcast_ref::<ScalarFunctionExpr>() else {
                return Ok(Transformed::no(expr));
            };
            let Some(call) =
                NestedPruningCall::try_new(func, file_schema, parquet_schema)
            else {
                return Ok(Transformed::no(expr));
            };
            let name = format!("{PLACEHOLDER_PREFIX}{}", calls.len());
            let placeholder = Arc::new(Column::new(&name, fields.len()));
            fields.push(Arc::new(Field::new(&name, call.return_type.clone(), true)));
            calls.insert(name, call);
            Ok(Transformed::new(
                placeholder as Arc<dyn PhysicalExpr>,
                true,
                TreeNodeRecursion::Jump,
            ))
        })?;

        if calls.is_empty() {
            return Ok(None);
        }
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            file_schema.metadata().clone(),
        ));
        Ok(Some((rewritten.data, Self { schema, calls })))
    }

    /// The schema to build the pruning predicate against: the file schema
    /// followed by the placeholder columns.
    pub(crate) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The call a placeholder column stands for, if `name` is a placeholder.
    pub(crate) fn call(&self, name: &str) -> Option<&NestedPruningCall> {
        self.calls.get(name)
    }
}

impl NestedPruningCall {
    fn try_new(
        func: &ScalarFunctionExpr,
        file_schema: &Schema,
        parquet_schema: &SchemaDescriptor,
    ) -> Option<Self> {
        let udf = func.fun();
        let access = udf.as_nested_input_access()?;
        let args = func.args();
        let arg_fields = args
            .iter()
            .map(|arg| argument_field(arg, file_schema))
            .collect::<Option<Vec<_>>>()?;
        let literals = args
            .iter()
            .map(|arg| arg.downcast_ref::<Literal>().map(|lit| lit.value().clone()))
            .collect::<Vec<_>>();
        let required = {
            let scalar_arguments =
                literals.iter().map(Option::as_ref).collect::<Vec<_>>();
            access.required_fields(&ReturnFieldArgs {
                arg_fields: &arg_fields,
                scalar_arguments: &scalar_arguments,
            })
        };

        let mut inputs = vec![];
        for (arg, paths) in args.iter().zip(&required) {
            let Some(paths) = paths else { continue };
            let column = arg.downcast_ref::<Column>();
            inputs.extend(paths.iter().map(|path| {
                column.and_then(|column| {
                    leaf_for_path(column.name(), path, file_schema, parquet_schema)
                })
            }));
        }
        if inputs.iter().all(Option::is_none) {
            return None;
        }

        Some(Self {
            udf: udf.clone(),
            arg_fields,
            literals,
            return_type: func.return_type().clone(),
            inputs,
        })
    }

    /// The Parquet leaves this call reads, one per required path.
    pub(crate) fn inputs(&self) -> &[Option<NestedLeaf>] {
        &self.inputs
    }

    /// The call's output statistics for each of `num_rows.len()` containers,
    /// given its inputs' statistics (one [`LeafStatistics`] per entry of
    /// [`Self::inputs`]).
    pub(crate) fn output_statistics(
        &self,
        num_rows: &[Option<usize>],
        inputs: &[Option<LeafStatistics>],
    ) -> Vec<Option<ColumnStatistics>> {
        let Some(access) = self.udf.as_nested_input_access() else {
            return vec![None; num_rows.len()];
        };
        let scalar_arguments =
            self.literals.iter().map(Option::as_ref).collect::<Vec<_>>();
        let args = ReturnFieldArgs {
            arg_fields: &self.arg_fields,
            scalar_arguments: &scalar_arguments,
        };
        num_rows
            .iter()
            .enumerate()
            .map(|(container, rows)| {
                let statistics = Statistics {
                    num_rows: rows.map_or(Precision::Absent, Precision::Exact),
                    total_byte_size: Precision::Absent,
                    column_statistics: inputs
                        .iter()
                        .map(|input| match input {
                            Some(input) => input.container(container),
                            None => ColumnStatistics::new_unknown(),
                        })
                        .collect(),
                };
                access.output_statistics(&args, &statistics)
            })
            .collect()
    }

    /// Minimum (`max = false`) or maximum values of `outputs` as an array of
    /// the call's return type, or `None` if none is known.
    pub(crate) fn bound_array(
        &self,
        outputs: &[Option<ColumnStatistics>],
        max: bool,
    ) -> Option<ArrayRef> {
        let null = ScalarValue::try_from(&self.return_type).ok()?;
        let mut any_known = false;
        let values = outputs
            .iter()
            .map(|output| {
                let bound = output.as_ref().map(|output| {
                    if max {
                        &output.max_value
                    } else {
                        &output.min_value
                    }
                });
                match bound {
                    Some(Precision::Exact(value)) if !value.is_null() => {
                        any_known = true;
                        value.cast_to(&self.return_type).ok()
                    }
                    _ => Some(null.clone()),
                }
            })
            .collect::<Option<Vec<_>>>()?;
        if !any_known {
            return None;
        }
        ScalarValue::iter_to_array(values).ok()
    }

    /// Null counts of `outputs`, or `None` if none is known.
    pub(crate) fn null_count_array(
        &self,
        outputs: &[Option<ColumnStatistics>],
    ) -> Option<ArrayRef> {
        let counts = outputs
            .iter()
            .map(|output| match output.as_ref()?.null_count {
                Precision::Exact(count) => Some(count as u64),
                _ => None,
            })
            .collect::<UInt64Array>();
        (counts.null_count() < counts.len()).then(|| Arc::new(counts) as ArrayRef)
    }
}

impl LeafStatistics {
    /// The statistics of one container.
    fn container(&self, container: usize) -> ColumnStatistics {
        let value = |array: &Option<ArrayRef>| {
            array
                .as_ref()
                .filter(|array| array.is_valid(container))
                .and_then(|array| ScalarValue::try_from_array(array, container).ok())
                .map_or(Precision::Absent, Precision::Exact)
        };
        let null_count = self
            .null_counts
            .as_ref()
            .filter(|counts| counts.is_valid(container))
            .map_or(Precision::Absent, |counts| {
                Precision::Exact(counts.value(container) as usize)
            });
        ColumnStatistics {
            min_value: value(&self.mins),
            max_value: value(&self.maxes),
            null_count,
            ..ColumnStatistics::new_unknown()
        }
    }
}

/// The field of a call argument in the file schema. Columns are resolved by
/// name, as their indices may refer to an earlier schema.
fn argument_field(arg: &Arc<dyn PhysicalExpr>, file_schema: &Schema) -> Option<FieldRef> {
    if let Some(column) = arg.downcast_ref::<Column>() {
        let index = file_schema.index_of(column.name()).ok()?;
        return Some(Arc::clone(&file_schema.fields()[index]));
    }
    reassign_expr_columns(Arc::clone(arg), file_schema)
        .and_then(|arg| arg.return_field(file_schema))
        .ok()
}

/// The primitive Parquet leaf at `path` below root column `root`, reached
/// through struct fields only.
fn leaf_for_path(
    root: &str,
    path: &FieldPath,
    file_schema: &Schema,
    parquet_schema: &SchemaDescriptor,
) -> Option<NestedLeaf> {
    let mut field = file_schema.field_with_name(root).ok()?;
    for name in path {
        let DataType::Struct(children) = field.data_type() else {
            return None;
        };
        let mut matches = children.iter().filter(|child| child.name() == name);
        field = matches.next()?.as_ref();
        if matches.next().is_some() {
            return None;
        }
    }
    if field.data_type().is_nested() {
        return None;
    }

    let parquet_index = parquet_schema.columns().iter().position(|column| {
        let parts = column.path().parts();
        parts.len() == path.len() + 1
            && parts[0] == root
            && parts[1..].iter().zip(path).all(|(part, name)| part == name)
    })?;
    Some(NestedLeaf {
        parquet_index,
        field: Arc::new(field.clone()),
    })
}
