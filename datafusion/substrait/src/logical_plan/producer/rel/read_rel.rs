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

use crate::logical_plan::producer::rel::project_rel::create_project_remapping;
use crate::logical_plan::producer::{
    SubstraitProducer, to_substrait_literal, to_substrait_named_struct,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, ToDFSchema, substrait_datafusion_err};
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{EmptyRelation, Expr, TableScan, Values};
use datafusion::scalar::ScalarValue;
use std::collections::BTreeSet;
use std::sync::Arc;
use substrait::proto::expression::MaskExpression;
use substrait::proto::expression::literal::Struct as LiteralStruct;
use substrait::proto::expression::mask_expression::{StructItem, StructSelect};
use substrait::proto::expression::nested::Struct as NestedStruct;
use substrait::proto::read_rel::{NamedTable, ReadType, VirtualTable};
use substrait::proto::rel::RelType;
use substrait::proto::{ProjectRel, ReadRel, Rel, RelCommon};

/// Converts rows of literal expressions into Substrait literal structs.
///
/// Each row is expected to contain only `Expr::Literal` or `Expr::Alias` wrapping literals.
/// Aliases are unwrapped and the underlying literal is converted.
fn convert_literal_rows(
    producer: &mut impl SubstraitProducer,
    rows: &[Vec<Expr>],
) -> datafusion::common::Result<Vec<LiteralStruct>> {
    rows.iter()
        .map(|row| {
            let fields = row
                .iter()
                .map(|expr| match expr {
                    Expr::Literal(sv, _) => to_substrait_literal(producer, sv),
                    Expr::Alias(alias) => match alias.expr.as_ref() {
                        // The schema gives us the names, so we can skip aliases
                        Expr::Literal(sv, _) => to_substrait_literal(producer, sv),
                        _ => Err(substrait_datafusion_err!(
                            "Only literal types can be aliased in Virtual Tables, got: {}",
                            alias.expr.variant_name()
                        )),
                    },
                    _ => Err(substrait_datafusion_err!(
                        "Only literal types and aliases are supported in Virtual Tables, got: {}",
                        expr.variant_name()
                    )),
                })
                .collect::<datafusion::common::Result<_>>()?;
            Ok(LiteralStruct { fields })
        })
        .collect()
}

/// Converts rows of arbitrary expressions into Substrait nested structs.
///
/// Validates that each row has the expected schema length and converts each expression
/// using the producer's expression handler.
fn convert_expression_rows(
    producer: &mut impl SubstraitProducer,
    rows: &[Vec<Expr>],
    schema_len: usize,
    empty_schema: &Arc<DFSchema>,
) -> datafusion::common::Result<Vec<NestedStruct>> {
    rows.iter()
        .map(|row| {
            if row.len() != schema_len {
                return Err(substrait_datafusion_err!(
                    "Names list must match exactly to nested schema, but found {} uses for {} names",
                    row.len(),
                    schema_len
                ));
            }

            let fields = row
                .iter()
                .map(|expr| producer.handle_expr(expr, empty_schema))
                .collect::<datafusion::common::Result<_>>()?;
            Ok(NestedStruct { fields })
        })
        .collect()
}

pub fn from_table_scan(
    producer: &mut impl SubstraitProducer,
    scan: &TableScan,
) -> datafusion::common::Result<Box<Rel>> {
    let source_schema = scan.source.schema();

    // Compute required column indices and remainder projection expressions.
    // This follows the same pattern as the physical planner's compute_scan_projection.
    let (remainder_projection, scan_indices) =
        compute_scan_projection(&scan.projection, &source_schema)?;

    // Build the projection mask from computed scan indices
    let projection = scan_indices.as_ref().map(|indices| {
        let struct_items = indices
            .iter()
            .map(|&i| StructItem {
                field: i as i32,
                child: None,
            })
            .collect();
        MaskExpression {
            select: Some(StructSelect { struct_items }),
            maintain_singular_struct: false,
        }
    });

    let table_schema = scan.source.schema().to_dfschema_ref()?;
    let base_schema = to_substrait_named_struct(producer, &table_schema)?;

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

    let read_rel = Box::new(Rel {
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
    });

    // If we have complex expressions, wrap the ReadRel with a ProjectRel
    if let Some(ref proj_exprs) = remainder_projection {
        // Build a schema for the scanned columns (the output of the ReadRel).
        // The projection expressions reference columns by name, and the schema
        // tells us the position of each column in the scan output.
        // We need to construct this from the source schema and scan indices since
        // `projected_schema` is the final output schema after complex projections.
        let scan_output_schema = {
            let indices = scan_indices.as_ref().expect("scan_indices should be Some when remainder_projection is Some");
            let projected_arrow_schema = source_schema.project(indices)?;
            Arc::new(DFSchema::try_from_qualified_schema(
                scan.table_name.clone(),
                &projected_arrow_schema,
            )?)
        };

        let expressions = proj_exprs
            .iter()
            .map(|e| producer.handle_expr(e, &scan_output_schema))
            .collect::<datafusion::common::Result<Vec<_>>>()?;

        let emit_kind =
            create_project_remapping(expressions.len(), scan_output_schema.fields().len());
        let common = RelCommon {
            emit_kind: Some(emit_kind),
            hint: None,
            advanced_extension: None,
        };

        Ok(Box::new(Rel {
            rel_type: Some(RelType::Project(Box::new(ProjectRel {
                common: Some(common),
                input: Some(read_rel),
                expressions,
                advanced_extension: None,
            }))),
        }))
    } else {
        Ok(read_rel)
    }
}

/// Compute the column indices needed for the scan based on projection expressions.
///
/// Returns a tuple of:
/// - `Option<Vec<Expr>>`: Remainder projection to apply on top of the scan output.
///   `None` if the projection is all simple column references (reordering, dropping, etc.)
/// - `Option<Vec<usize>>`: Column indices to scan from the source. `None` means scan all.
fn compute_scan_projection(
    projection: &Option<Vec<Expr>>,
    source_schema: &SchemaRef,
) -> datafusion::common::Result<(Option<Vec<Expr>>, Option<Vec<usize>>)> {
    let Some(exprs) = projection else {
        // None means scan all columns, no remainder needed
        return Ok((None, None));
    };

    if exprs.is_empty() {
        return Ok((None, Some(vec![])));
    }

    let mut has_complex_expr = false;
    let mut all_required_columns = BTreeSet::new();
    let mut remainder_exprs = vec![];

    for expr in exprs {
        // Collect all column references from this expression
        let mut is_complex_expr = false;
        expr.apply(|e| {
            if let Expr::Column(col) = e {
                if let Ok(index) = source_schema.index_of(col.name()) {
                    // If we made it this far without seeing a non-Column node, this is a simple
                    // column reference. We add it to remainder_exprs in case later expressions
                    // turn out to be complex (requiring us to use remainder projection).
                    if !is_complex_expr {
                        remainder_exprs.push(expr.clone());
                    }
                    all_required_columns.insert(index);
                }
            } else {
                // This expression contains non-column nodes, so it's complex
                is_complex_expr = true;
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if is_complex_expr {
            has_complex_expr = true;
            // Append the full complex expression to remainder_exprs
            remainder_exprs.push(expr.clone());
        }
    }

    Ok((
        has_complex_expr.then_some(remainder_exprs),
        Some(all_required_columns.into_iter().collect()),
    ))
}

/// Encodes an EmptyRelation as a Substrait VirtualTable.
///
/// EmptyRelation represents a relation with no input data. When `produce_one_row` is true,
/// it generates a single row with all fields set to their default values (typically NULL).
/// This is used for queries without a FROM clause, such as "SELECT 1 AS one" or
/// "SELECT current_timestamp()".
///
/// When `produce_one_row` is false, it represents a truly empty relation with no rows,
/// used in optimizations or as a placeholder.
pub fn from_empty_relation(
    producer: &mut impl SubstraitProducer,
    e: &EmptyRelation,
) -> datafusion::common::Result<Box<Rel>> {
    let base_schema = to_substrait_named_struct(producer, &e.schema)?;

    let read_type = if e.produce_one_row {
        // Create one row with default scalar values for each field in the schema.
        // For example, an Int32 field gets Int32(NULL), a Utf8 field gets Utf8(NULL), etc.
        // This represents the "phantom row" that provides a context for evaluating
        // scalar expressions in queries without a FROM clause.
        let fields = e
            .schema
            .fields()
            .iter()
            .map(|f| {
                let scalar = ScalarValue::try_from(f.data_type())?;
                to_substrait_literal(producer, &scalar)
            })
            .collect::<datafusion::common::Result<_>>()?;

        ReadType::VirtualTable(VirtualTable {
            // Use deprecated 'values' field instead of 'expressions' because the consumer's
            // nested expression support (RexType::Nested) is not yet implemented.
            // The 'values' field uses literal::Struct which the consumer can properly
            // deserialize with field name preservation.
            #[expect(deprecated)]
            values: vec![LiteralStruct { fields }],
            expressions: vec![],
        })
    } else {
        ReadType::VirtualTable(VirtualTable {
            #[expect(deprecated)]
            values: vec![],
            expressions: vec![],
        })
    };
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Read(Box::new(ReadRel {
            common: None,
            base_schema: Some(base_schema),
            filter: None,
            best_effort_filter: None,
            projection: None,
            advanced_extension: None,
            read_type: Some(read_type),
        }))),
    }))
}

pub fn from_values(
    producer: &mut impl SubstraitProducer,
    v: &Values,
) -> datafusion::common::Result<Box<Rel>> {
    let schema_len = v.schema.fields().len();
    let empty_schema = Arc::new(DFSchema::empty());

    let use_literals = v.values.iter().all(|row| {
        row.iter().all(|expr| match expr {
            Expr::Literal(_, _) => true,
            Expr::Alias(alias) => matches!(alias.expr.as_ref(), Expr::Literal(_, _)),
            _ => false,
        })
    });

    let (values, expressions) = if use_literals {
        let values = convert_literal_rows(producer, &v.values)?;
        (values, vec![])
    } else {
        let expressions =
            convert_expression_rows(producer, &v.values, schema_len, &empty_schema)?;
        (vec![], expressions)
    };
    Ok(Box::new(Rel {
        rel_type: Some(RelType::Read(Box::new(ReadRel {
            common: None,
            base_schema: Some(to_substrait_named_struct(producer, &v.schema)?),
            filter: None,
            best_effort_filter: None,
            projection: None,
            advanced_extension: None,
            #[expect(deprecated)]
            read_type: Some(ReadType::VirtualTable(VirtualTable {
                values,
                expressions,
            })),
        }))),
    }))
}
