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

//! type coercion for UNION

use itertools::izip;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::Field;

use datafusion_common::{
    plan_datafusion_err, plan_err, Column, DFSchema, DFSchemaRef, Result,
};
use datafusion_expr::expr::Alias;
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::{Expr, ExprSchemable, LogicalPlan, Projection, Union};

/// Coerce the schema of the inputs to a common schema
fn coerce_union_schema(inputs: Vec<Arc<LogicalPlan>>) -> Result<DFSchema> {
    let base_schema = inputs[0].schema();
    let mut union_datatypes = base_schema
        .fields()
        .iter()
        .map(|f| f.data_type().clone())
        .collect::<Vec<_>>();
    let mut union_nullabilities = base_schema
        .fields()
        .iter()
        .map(|f| f.is_nullable())
        .collect::<Vec<_>>();

    for (i, plan) in inputs.iter().enumerate().skip(1) {
        let plan_schema = plan.schema();
        if plan_schema.fields().len() != base_schema.fields().len() {
            return plan_err!(
                "Union schemas have different number of fields,\
                query 1 has {}, query {} has {}",
                base_schema.fields().len(),
                i + 1,
                plan_schema.fields().len()
            );
        }
        // coerce data type and nullablity for each field
        for (union_datatype, union_nullable, plan_field) in izip!(
            union_datatypes.iter_mut(),
            union_nullabilities.iter_mut(),
            plan_schema.fields()
        ) {
            let coerced_type =
                comparison_coercion(union_datatype, plan_field.data_type()).ok_or_else(
                    || {
                        plan_datafusion_err!(
                    "UNION Column {} (type: {}) is not compatible with other type: {}",
                    plan_field.name(),
                    plan_field.data_type(),
                    union_datatype
                )
                    },
                )?;
            *union_datatype = coerced_type;
            *union_nullable = *union_nullable || plan_field.is_nullable();
        }
    }
    let union_qualified_fields = izip!(
        base_schema.iter(),
        union_datatypes.into_iter(),
        union_nullabilities
    )
    .map(|((qualifier, field), datatype, nullable)| {
        let field = Arc::new(Field::new(field.name().clone(), datatype, nullable));
        (qualifier.cloned(), field)
    })
    .collect::<Vec<_>>();
    DFSchema::new_with_metadata(union_qualified_fields, HashMap::new())
}

/// Make sure that the schemas of all inputs are compatible with each other,
/// which includes having the same field types and names.
pub(crate) fn coerce_union(union_plan: Union) -> Result<LogicalPlan> {
    let union_schema = coerce_union_schema(union_plan.inputs.clone())?;
    let new_inputs = union_plan
        .inputs
        .iter()
        .map(|p| {
            let plan = coerce_plan_expr_for_schema(&p, &union_schema)?;
            match plan {
                LogicalPlan::Projection(Projection { expr, input, .. }) => {
                    Ok(Arc::new(project_with_column_index(
                        expr,
                        input,
                        Arc::new(union_schema.clone()),
                    )?))
                }
                other_plan => Ok(Arc::new(other_plan)),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(LogicalPlan::Union(Union {
        inputs: new_inputs,
        schema: Arc::new(union_schema),
    }))
}

/// Returns plan with expressions coerced to types compatible with
/// schema types
pub(crate) fn coerce_plan_expr_for_schema(
    plan: &LogicalPlan,
    schema: &DFSchema,
) -> Result<LogicalPlan> {
    match plan {
        // special case Projection to avoid adding multiple projections
        LogicalPlan::Projection(Projection { expr, input, .. }) => {
            let new_exprs =
                coerce_exprs_for_schema(expr.clone(), input.schema(), schema)?;
            let projection = Projection::try_new(new_exprs, Arc::clone(input))?;
            Ok(LogicalPlan::Projection(projection))
        }
        _ => {
            let exprs: Vec<Expr> = plan.schema().iter().map(Expr::from).collect();

            let new_exprs = coerce_exprs_for_schema(exprs, plan.schema(), schema)?;
            let add_project = new_exprs.iter().any(|expr| expr.try_as_col().is_none());
            if add_project {
                let projection = Projection::try_new(new_exprs, Arc::new(plan.clone()))?;
                Ok(LogicalPlan::Projection(projection))
            } else {
                Ok(plan.clone())
            }
        }
    }
}

fn coerce_exprs_for_schema(
    exprs: Vec<Expr>,
    src_schema: &DFSchema,
    dst_schema: &DFSchema,
) -> Result<Vec<Expr>> {
    exprs
        .into_iter()
        .enumerate()
        .map(|(idx, expr)| {
            let new_type = dst_schema.field(idx).data_type();
            if new_type != &expr.get_type(src_schema)? {
                match expr {
                    Expr::Alias(Alias { expr, name, .. }) => {
                        Ok(expr.cast_to(new_type, src_schema)?.alias(name))
                    }
                    _ => expr.cast_to(new_type, src_schema),
                }
            } else {
                Ok(expr)
            }
        })
        .collect::<Result<_>>()
}

fn project_with_column_index(
    expr: Vec<Expr>,
    input: Arc<LogicalPlan>,
    schema: DFSchemaRef,
) -> Result<LogicalPlan> {
    let alias_expr = expr
        .into_iter()
        .enumerate()
        .map(|(i, e)| match e {
            Expr::Alias(Alias { ref name, .. }) if name != schema.field(i).name() => {
                e.unalias().alias(schema.field(i).name())
            }
            Expr::Column(Column {
                relation: _,
                ref name,
            }) if name != schema.field(i).name() => e.alias(schema.field(i).name()),
            Expr::Alias { .. } | Expr::Column { .. } => e,
            _ => e.alias(schema.field(i).name()),
        })
        .collect::<Vec<_>>();

    Projection::try_new_with_schema(alias_expr, input, schema)
        .map(LogicalPlan::Projection)
}

#[cfg(test)]
mod test {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{Result, TableReference};
    use datafusion_expr::builder::table_scan;
    use std::sync::Arc;

    use crate::analyzer::TypeCoercion;
    use crate::test::assert_analyzer_check_err;

    fn employee_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("first_name", DataType::Utf8, false),
            Field::new("last_name", DataType::Utf8, false),
            Field::new("state", DataType::Utf8, false),
            Field::new("salary", DataType::Int32, false),
        ])
    }

    #[test]
    fn union_different_num_columns_error() -> Result<()> {
        let plan1 =
            table_scan(TableReference::none(), &employee_schema(), Some(vec![3]))?;
        let plan2 =
            table_scan(TableReference::none(), &employee_schema(), Some(vec![3, 4]))?
                .build()?;

        let expected = "type_coercion\n\
                        caused by\n\
                        Error during planning: Union schemas have different number of fields,\
                        query 1 has 1, query 2 has 2";
        let union_plan = plan1.clone().union(plan2.clone())?.build()?;
        assert_analyzer_check_err(
            vec![Arc::new(TypeCoercion::new())],
            union_plan,
            expected,
        );

        let union_distinct = plan1.union_distinct(plan2)?.build()?;
        assert_analyzer_check_err(
            vec![Arc::new(TypeCoercion::new())],
            union_distinct,
            expected,
        );
        Ok(())
    }
}
