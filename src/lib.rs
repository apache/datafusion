use std::sync::Arc;

use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    datasource::empty::EmptyTable,
    error::{DataFusionError, Result},
    logical_plan::{plan::Projection, DFSchema, LogicalPlan, TableScan},
};

use substrait::protobuf::{
    read_rel::{NamedTable, ReadType},
    rel::RelType,
    ProjectRel, ReadRel, Rel,
};

//TODO refactor to use Into or From

pub fn to_substrait_rel(plan: &LogicalPlan) -> Result<Box<Rel>> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Read(Box::new(ReadRel {
                    common: None,             //<RelCommon>,
                    base_schema: None,        //<NamedStruct>,
                    filter: None,             //<Box<Expression>>,
                    projection: None,         //<MaskExpression>,
                    advanced_extension: None, //<AdvancedExtension>,
                    read_type: Some(ReadType::NamedTable(NamedTable {
                        names: vec![scan.table_name.clone()],
                        advanced_extension: None,
                    })),
                }))),
            }))
        }
        LogicalPlan::Projection(p) => {
            Ok(Box::new(Rel {
                rel_type: Some(RelType::Project(Box::new(ProjectRel {
                    common: None,                                     //<RelCommon>,
                    input: Some(to_substrait_rel(p.input.as_ref())?), //<Box<Rel>>,
                    expressions: vec![],                              // Vec<Expression>,
                    advanced_extension: None,                         //<AdvancedExtension>,
                }))),
            }))
        }
        _ => Err(DataFusionError::NotImplemented(
            "Unsupported logical plan operator".to_string(),
        )),
    }
}

pub fn from_substrait(proto: &Rel) -> Result<LogicalPlan> {
    match &proto.rel_type {
        Some(RelType::Project(p)) => Ok(LogicalPlan::Projection(Projection {
            expr: vec![],
            input: Arc::new(from_substrait(p.input.as_ref().unwrap())?),
            schema: Arc::new(DFSchema::empty()),
            alias: None,
        })),
        Some(RelType::Read(_read)) => Ok(LogicalPlan::TableScan(TableScan {
            table_name: "".to_string(),
            source: Arc::new(EmptyTable::new(SchemaRef::new(Schema::empty()))),
            projection: None,
            projected_schema: Arc::new(DFSchema::empty()),
            filters: vec![],
            limit: None,
        })),
        _ => Err(DataFusionError::NotImplemented(format!(
            "{:?}",
            proto.rel_type
        ))),
    }
}

#[cfg(test)]
mod tests {

    use crate::{from_substrait, to_substrait_rel};
    use datafusion::error::Result;
    use datafusion::prelude::*;

    #[tokio::test]
    async fn it_works() -> Result<()> {
        let mut ctx = ExecutionContext::new();
        ctx.register_csv("data", "testdata/data.csv", CsvReadOptions::new())
            .await?;
        let df = ctx.sql("SELECT a, b FROM data").await?;
        let plan = df.to_logical_plan();
        let proto = to_substrait_rel(&plan)?;
        let plan2 = from_substrait(&proto)?;
        let plan1str = format!("{:?}", plan);
        let plan2str = format!("{:?}", plan2);
        assert_eq!(plan1str, plan2str);

        Ok(())
    }
}
