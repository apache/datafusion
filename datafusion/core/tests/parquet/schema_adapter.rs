use std::sync::Arc;

use arrow::array::{record_batch, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::{BufMut, BytesMut};
use datafusion::assert_batches_eq;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
use datafusion::prelude::SessionContext;
use datafusion_datasource::ListingTableUrl;
use datafusion_datasource::{
    schema_adapter::DefaultSchemaAdapterFactory,
};
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_physical_expr::schema_rewriter::DefaultPhysicalExprAdapterFactory;
use object_store::{memory::InMemory, path::Path, ObjectStore};
use parquet::arrow::ArrowWriter;

async fn write_parquet(batch: RecordBatch, store: Arc<dyn ObjectStore>, path: &str) {
    let mut out = BytesMut::new().writer();
    {
        let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let data = out.into_inner().freeze();
    store.put(&Path::from(path), data.into()).await.unwrap();
}

#[tokio::test]
async fn single_file() {
    let batch =
        record_batch!(("extra", Int64, [1, 2, 3]), ("c1", Int32, [1, 2, 3])).unwrap();

    let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let store_url = ObjectStoreUrl::parse("memory://").unwrap();
    let path = "test.parquet";
    write_parquet(batch, store.clone(), path).await;

    let table_schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Int64, false),
        Field::new("c2", DataType::Utf8, true),
    ]));

    let ctx = SessionContext::new();
    ctx.register_object_store(store_url.as_ref(), Arc::clone(&store));

    let listing_table_config =
        ListingTableConfig::new(ListingTableUrl::parse("memory:///").unwrap())
            .infer_options(&ctx.state())
            .await
            .unwrap()
            .with_schema(table_schema.clone())
            .with_schema_adapter_factory(Arc::new(DefaultSchemaAdapterFactory))
            .with_physical_expr_adapter_factory(Arc::new(
                DefaultPhysicalExprAdapterFactory,
            ));

    let table = ListingTable::try_new(listing_table_config).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();

    let batches = ctx
        .sql("SELECT c2, c1 FROM t WHERE c1 = 2 AND c2 IS NULL")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let expected = [
        "+----+----+",
        "| c2 | c1 |",
        "+----+----+",
        "|    | 2  |",
        "+----+----+",
    ];
    assert_batches_eq!(expected, &batches);
}
