#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]

use std::{any::Any, sync::Arc, time::Duration};

use arrow::{
    array::{ArrayRef, AsArray, Float64Array},
    datatypes::Float64Type,
};
//use arrow::infer_arrow_schema_from_json_value;
use arrow_schema::{DataType, Field, Schema, SchemaRef, SortOptions, TimeUnit};
use datafusion::{
    config::ConfigOptions,
    dataframe::DataFrame,
    datasource::{provider_as_source, TableProvider},
    execution::{
        context::{SessionContext, SessionState},
        RecordBatchStream,
    },
    physical_plan::{
        display::DisplayableExecutionPlan,
        kafka_source::{KafkaStreamConfig, KafkaStreamRead, StreamEncoding},
        streaming::StreamingTableExec,
        time::TimestampUnit,
        ExecutionPlan,
    },
};
use datafusion_common::{
    franz_arrow::infer_arrow_schema_from_json_value, plan_err, ScalarValue,
};
use datafusion_expr::{
    col, create_udwf, ident, max, min, Expr, LogicalPlanBuilder, PartitionEvaluator,
    TableType, Volatility, WindowFrame,
};

use datafusion_common::Result;
use datafusion_physical_expr::{expressions, LexOrdering, PhysicalSortExpr};
use futures::StreamExt;
use tonic::async_trait;
use tracing_subscriber::{fmt::format::FmtSpan, FmtSubscriber};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::WARN)
        .with_span_events(FmtSpan::CLOSE | FmtSpan::ENTER)
        .finish();
    tracing::subscriber::set_global_default(subscriber)
        .expect("setting default subscriber failed");

    let sample_event = r#"
        {
            "driver_id": "690c119e-63c9-479b-b822-872ee7d89165",
            "occurred_at_ms": 1715201766763,
            "imu_measurement": {
                "timestamp": "2024-05-08T20:56:06.763260Z",
                "accelerometer": {
                    "x": 1.4187794,
                    "y": -0.13967037,
                    "z": 0.5483732
                },
                "gyroscope": {
                    "x": 0.005840948,
                    "y": 0.0035944171,
                    "z": 0.0041645765
                },
                "gps": {
                    "latitude": 72.3492587464122,
                    "longitude": 144.85596244550095,
                    "altitude": 2.9088259,
                    "speed": 57.96137
                }
            },
            "meta": {
                "nonsense": "MMMMMMMMMM"
            }
        }"#;

    // register the window function with DataFusion so we can call it
    let sample_value: serde_json::Value = serde_json::from_str(sample_event).unwrap();
    let inferred_schema = infer_arrow_schema_from_json_value(&sample_value).unwrap();

    // println!("{:?}", inferred_schema);
    let mut fields = inferred_schema.fields().to_vec();
    // println!("{:?}", fields);

    // Add a new column to the dataset that should mirror the occured_at_ms field
    fields.insert(
        fields.len(),
        Arc::new(Field::new(
            String::from("franz_canonical_timestamp"),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )),
    );
    let canonical_schema = Arc::new(Schema::new(fields));
    let _config = KafkaStreamConfig {
        bootstrap_servers: String::from(
            "localhost:19092,localhost:29092,localhost:39092",
        ),
        topic: String::from("driver-imu-data"),
        consumer_group_id: String::from("my_test_consumer"),
        original_schema: Arc::new(inferred_schema),
        schema: canonical_schema,
        batch_size: 10,
        encoding: StreamEncoding::Json,
        order: vec![],
        partitions: 1_i32,
        timestamp_column: String::from("occurred_at_ms"),
        timestamp_unit: TimestampUnit::Int64Millis,
        offset_reset: String::from("earliest"),
    };

    // Create a new streaming table
    let db = StreamTable(Arc::new(_config));
    let mut config = ConfigOptions::default();
    let _ = config.set("datafusion.execution.batch_size", "32");

    let ctx = SessionContext::new_with_config(config.into());

    // create logical plan composed of a single TableScan
    let logical_plan = LogicalPlanBuilder::scan_with_filters(
        "kafka_imu_data",
        provider_as_source(Arc::new(db)),
        None,
        vec![],
    )
    .unwrap()
    .build()
    .unwrap();

    let df = DataFrame::new(ctx.state(), logical_plan);
    let windowed_df = df
        .clone()
        .franz_window(
            vec![],
            vec![
                max(col("imu_measurement").field("gps").field("speed")),
                min(col("imu_measurement").field("gps").field("altitude")),
            ],
            Duration::from_millis(2000),
        )
        .unwrap();

    print_stream(&windowed_df).await;
}

async fn print_stream(windowed_df: &DataFrame) {
    let mut stream: std::pin::Pin<Box<dyn RecordBatchStream + Send>> =
        windowed_df.clone().execute_stream().await.unwrap();

    // for _ in 1..100 {
    loop {
        let rb = stream.next().await.transpose();
        // println!("{:?}", rb);
        if let Ok(Some(batch)) = rb {
            if batch.num_rows() > 0 {
                println!(
                    "{}",
                    arrow::util::pretty::pretty_format_batches(&[batch]).unwrap()
                );
            }
        }
    }
}

fn create_ordering(
    schema: &Schema,
    sort_order: &[Vec<Expr>],
) -> Result<Vec<LexOrdering>> {
    let mut all_sort_orders = vec![];

    for exprs in sort_order {
        // Construct PhysicalSortExpr objects from Expr objects:
        let mut sort_exprs = vec![];
        for expr in exprs {
            match expr {
                Expr::Sort(sort) => match sort.expr.as_ref() {
                    Expr::Column(col) => match expressions::col(&col.name, schema) {
                        Ok(expr) => {
                            sort_exprs.push(PhysicalSortExpr {
                                expr,
                                options: SortOptions {
                                    descending: !sort.asc,
                                    nulls_first: sort.nulls_first,
                                },
                            });
                        }
                        // Cannot find expression in the projected_schema, stop iterating
                        // since rest of the orderings are violated
                        Err(_) => break,
                    },
                    expr => {
                        return plan_err!(
                            "Expected single column references in output_ordering, got {expr}"
                        )
                    }
                },
                expr => return plan_err!("Expected Expr::Sort in output_ordering, but got {expr}"),
            }
        }
        if !sort_exprs.is_empty() {
            all_sort_orders.push(sort_exprs);
        }
    }
    Ok(all_sort_orders)
}

// Used to createa kafka source
pub struct StreamTable(pub Arc<KafkaStreamConfig>);

impl StreamTable {
    /// Create a new [`StreamTable`] for the given [`StreamConfig`]
    pub fn new(config: Arc<KafkaStreamConfig>) -> Self {
        Self(config)
    }

    pub async fn create_physical_plan(
        &self,
        projection: Option<&Vec<usize>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = match projection {
            Some(p) => {
                let projected = self.0.schema.project(p)?;
                create_ordering(&projected, &self.0.order)?
            }
            None => create_ordering(self.0.schema.as_ref(), &self.0.order)?,
        };
        let mut partition_streams = Vec::with_capacity(self.0.partitions as usize);

        for part in 0..self.0.partitions {
            let my_struct = Arc::new(KafkaStreamRead {
                config: self.0.clone(),
                assigned_partitions: vec![part],
            });
            partition_streams.push(my_struct as _);
        }

        Ok(Arc::new(StreamingTableExec::try_new(
            self.0.schema.clone(),
            partition_streams,
            projection,
            projected_schema,
            true,
        )?))
    }
}

#[async_trait]
impl TableProvider for StreamTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.0.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        return self.create_physical_plan(projection).await;
    }
}
