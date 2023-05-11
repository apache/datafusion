use crate::datasource::file_format::FileFormat;
use crate::error::Result;
use crate::execution::context::SessionState;
use crate::physical_plan::file_format::{ArrowExec, FileScanConfig};
use crate::physical_plan::ExecutionPlan;
use arrow::ipc::reader::FileReader;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_common::Statistics;
use datafusion_physical_expr::PhysicalExpr;
use object_store::{GetResult, ObjectMeta, ObjectStore};
use std::any::Any;
use std::io::{Read, Seek};
use std::sync::Arc;

/// The default file extension of arrow files
pub const DEFAULT_ARROW_EXTENSION: &str = ".arrow";
/// Arrow `FileFormat` implementation.
#[derive(Default, Debug)]
pub struct ArrowFormat;

#[async_trait]
impl FileFormat for ArrowFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn infer_schema(
        &self,
        _state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        let mut schemas = vec![];
        for object in objects {
            let schema = match store.get(&object.location).await? {
                GetResult::File(mut file, _) => read_arrow_schema_from_reader(&mut file)?,
                r @ GetResult::Stream(_) => {
                    // TODO: Fetching entire file to get schema is potentially wasteful
                    let data = r.bytes().await?;
                    let mut cursor = std::io::Cursor::new(&data);
                    read_arrow_schema_from_reader(&mut cursor)?
                }
            };
            schemas.push(schema.as_ref().clone());
        }
        let merged_schema = Schema::try_merge(schemas)?;
        Ok(Arc::new(merged_schema))
    }

    async fn infer_stats(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> Result<Statistics> {
        Ok(Statistics::default())
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let exec = ArrowExec::new(conf);
        Ok(Arc::new(exec))
    }
}

fn read_arrow_schema_from_reader<R: Read + Seek>(reader: R) -> Result<SchemaRef> {
    let reader = FileReader::try_new(reader, None)?;
    Ok(reader.schema())
}

// #[cfg(test)]
// mod tests {
//     use arrow::datatypes::DataType;
//     use crate::datasource::file_format::FileFormat;
//
//     use super::ArrowFormat;
//
//     #[tokio::test]
//     async fn test_schema() {
//         let filename = "tests/example.arrow";
//         let format = ArrowFormat {};
//         let file_schema = format
//             .infer_schema(local_object_reader_stream(vec![filename.to_owned()]))
//             .await
//             .expect("Schema inference");
//         assert_eq!(
//             vec!["f0", "f1", "f2"],
//             file_schema
//                 .fields()
//                 .iter()
//                 .map(|x| x.name().clone())
//                 .collect::<Vec<String>>()
//         );
//
//         assert_eq!(
//             vec![DataType::Int64, DataType::Utf8, DataType::Boolean],
//             file_schema
//                 .fields()
//                 .iter()
//                 .map(|x| x.data_type().clone())
//                 .collect::<Vec<DataType>>()
//         );
//     }
// }
