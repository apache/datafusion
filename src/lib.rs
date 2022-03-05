use std::sync::Arc;

use async_recursion::async_recursion;

use datafusion::prelude::{CsvReadOptions, DataFrame, ExecutionContext};
use datafusion::{
    arrow::datatypes::{DataType, Field, Schema},
    error::{DataFusionError, Result},
    logical_plan::{DFSchemaRef, Expr, LogicalPlan},
    prelude::Column,
};

use substrait::protobuf::{
    expression::{
        field_reference::{ReferenceType, ReferenceType::MaskedReference},
        mask_expression::{StructItem, StructSelect},
        FieldReference, MaskExpression, RexType,
    },
    read_rel::{NamedTable, ReadType},
    rel::RelType,
    Expression, NamedStruct, ProjectRel, ReadRel, Rel,
};

/// Convert DataFusion Expr to Substrait Rex
pub fn to_substrait_rex(expr: &Expr, schema: &DFSchemaRef) -> Result<Expression> {
    match expr {
        Expr::Column(col) => Ok(Expression {
            rex_type: Some(RexType::Selection(Box::new(FieldReference {
                reference_type: Some(ReferenceType::MaskedReference(MaskExpression {
                    select: Some(StructSelect {
                        struct_items: vec![StructItem {
                            field: schema.index_of(&col.name)? as i32,
                            child: None,
                        }],
                    }),
                    maintain_singular_struct: false,
                })),
                root_type: None,
            }))),
        }),
        _ => Err(DataFusionError::NotImplemented(
            "Unsupported logical plan expression".to_string(),
        )),
    }
}

/// Convert DataFusion LogicalPlan to Substrait Rel
pub fn to_substrait_rel(plan: &LogicalPlan) -> Result<Box<Rel>> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let projection = scan.projection.as_ref().map(|p| {
                p.iter()
                    .map(|i| StructItem {
                        field: *i as i32,
                        child: None,
                    })
                    .collect()
            });

            Ok(Box::new(Rel {
                rel_type: Some(RelType::Read(Box::new(ReadRel {
                    common: None,
                    base_schema: Some(NamedStruct {
                        names: scan
                            .projected_schema
                            .fields()
                            .iter()
                            .map(|f| f.name().to_owned())
                            .collect(),
                        r#struct: None, // TODO
                    }),
                    filter: None,
                    projection: Some(MaskExpression {
                        select: Some(StructSelect {
                            struct_items: projection.unwrap(),
                        }),
                        maintain_singular_struct: false,
                    }),
                    advanced_extension: None,
                    read_type: Some(ReadType::NamedTable(NamedTable {
                        names: vec![scan.table_name.clone()],
                        advanced_extension: None,
                    })),
                }))),
            }))
        }
        LogicalPlan::Projection(p) => {
            let expressions = p
                .expr
                .iter()
                .map(|e| to_substrait_rex(e, p.input.schema()))
                .collect::<Result<Vec<_>>>()?;
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Project(Box::new(ProjectRel {
                    common: None,
                    input: Some(to_substrait_rel(p.input.as_ref())?),
                    expressions,
                    advanced_extension: None,
                }))),
            }))
        }
        _ => Err(DataFusionError::NotImplemented(
            "Unsupported logical plan operator".to_string(),
        )),
    }
}

/// Convert Substrait Rex to DataFusion Expr
// pub fn from_substrait_rex(rex: &Expression) -> Result<Expr> {
// }

/// Convert Substrait Rel to DataFusion DataFrame
#[async_recursion]
pub async fn from_substrait_rel(
    ctx: &mut ExecutionContext,
    rel: &Rel,
) -> Result<Arc<dyn DataFrame>> {
    match &rel.rel_type {
        Some(RelType::Project(p)) => {
            let input = from_substrait_rel(ctx, p.input.as_ref().unwrap()).await?;
            let exprs: Vec<Expr> = p
                .expressions
                .iter()
                .map(|e| {
                    match &e.rex_type {
                        Some(RexType::Selection(field_ref)) => {
                            match &field_ref.reference_type {
                                Some(MaskedReference(mask)) => {
                                    //TODO remove unwrap
                                    let xx = &mask.select.as_ref().unwrap().struct_items;
                                    assert!(xx.len() == 1);
                                    Ok(Expr::Column(Column {
                                        relation: None,
                                        name: input
                                            .schema()
                                            .field(xx[0].field as usize)
                                            .name()
                                            .to_string(),
                                    }))
                                }
                                _ => Err(DataFusionError::NotImplemented(
                                    "unsupported field ref type".to_string(),
                                )),
                            }
                        }
                        _ => Err(DataFusionError::NotImplemented(
                            "unsupported rex_type in projection".to_string(),
                        )),
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            input.select(exprs)
        }
        Some(RelType::Read(read)) => {
            let schema = match &read.base_schema {
                Some(named_struct) => Schema::new(
                    named_struct
                        .names
                        .iter()
                        .map(|n| Field::new(n, DataType::Utf8, false))
                        .collect(),
                ),
                _ => unimplemented!(),
            };

            let table_name = match &read.as_ref().read_type {
                Some(ReadType::NamedTable(nt)) => nt.names[0].to_owned(),
                _ => unimplemented!(),
            };

            //TODO assumes csv for now
            let options = CsvReadOptions::new().has_header(true).schema(&schema);
            ctx.read_csv(table_name, options).await
        }
        _ => Err(DataFusionError::NotImplemented(format!(
            "{:?}",
            rel.rel_type
        ))),
    }
}

#[cfg(test)]
mod tests {

    use crate::{from_substrait_rel, to_substrait_rel};
    use datafusion::error::Result;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn simple_select() -> Result<()> {
        roundtrip("SELECT a, b FROM data").await
    }

    async fn roundtrip(sql: &str) -> Result<()> {
        let mut ctx = ExecutionContext::new();
        ctx.register_csv("data", "testdata/data.csv", CsvReadOptions::new())
            .await?;
        let df = ctx.sql(sql).await?;
        let plan = df.to_logical_plan();
        let proto = to_substrait_rel(&plan)?;
        let df = from_substrait_rel(&mut ctx, &proto).await?;
        let plan2 = df.to_logical_plan();
        let plan2 = ctx.optimize(&plan2)?;
        let plan1str = format!("{:?}", plan);
        let plan2str = format!("{:?}", plan2);
        assert_eq!(plan1str, plan2str);
        Ok(())
    }
}
