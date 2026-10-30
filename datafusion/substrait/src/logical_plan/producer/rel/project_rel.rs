// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::logical_plan::producer::{SubstraitProducer, substrait_field_ref};
use datafusion::logical_expr::{Projection, Window};
use substrait::proto::rel::RelType;
use substrait::proto::rel_common::EmitKind;
use substrait::proto::rel_common::EmitKind::Emit;
use substrait::proto::{ProjectRel, Rel, RelCommon, rel_common};

pub fn from_projection(
    producer: &mut impl SubstraitProducer,
    p: &Projection,
) -> datafusion::common::Result<Box<Rel>> {
    let expressions = p
        .expr
        .iter()
        .map(|e| producer.handle_expr(e, p.input.schema()))
        .collect::<datafusion::common::Result<Vec<_>>>()?;

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

pub fn from_window(
    producer: &mut impl SubstraitProducer,
    window: &Window,
) -> datafusion::common::Result<Box<Rel>> {
    let input = producer.handle_plan(window.input.as_ref())?;

    // create a field reference for each input field
    let mut expressions = (0..window.input.schema().fields().len())
        .map(substrait_field_ref)
        .collect::<datafusion::common::Result<Vec<_>>>()?;

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
