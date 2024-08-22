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

use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    new_null_array, Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int32Array,
    LargeBinaryArray, LargeStringArray, NullArray, StringArray, TimestampNanosecondArray,
    UnionArray,
};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{
    DataType, Field, Schema, SchemaRef, TimeUnit, UnionFields, UnionMode,
};
use arrow::record_batch::RecordBatch;
use datafusion::logical_expr::{create_udf, ColumnarValue, Expr, ScalarUDF, Volatility};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion::{
    catalog::CatalogProvider,
    catalog_common::{memory::MemoryCatalogProvider, memory::MemorySchemaProvider},
    datasource::{MemTable, TableProvider, TableType},
    prelude::{CsvReadOptions, SessionContext},
};
use datafusion_common::cast::as_float64_array;
use datafusion_common::DataFusionError;

use async_trait::async_trait;
use datafusion::catalog::Session;
use log::info;
use tempfile::TempDir;

/// Context for running tests
pub struct TestContext {
    /// Context for running queries
    ctx: SessionContext,
    /// Temporary directory created and cleared at the end of the test
    test_dir: Option<TempDir>,
}

impl TestContext {
    pub fn new(ctx: SessionContext) -> Self {
        Self {
            ctx,
            test_dir: None,
        }
    }

    /// Create a SessionContext, configured for the specific sqllogictest
    /// test(.slt file) , if possible.
    ///
    /// If `None` is returned (e.g. because some needed feature is not
    /// enabled), the file should be skipped
    pub async fn try_new_for_test_file(relative_path: &Path) -> Option<Self> {
        let config = SessionConfig::new()
            // hardcode target partitions so plans are deterministic
            .with_target_partitions(4);

        let mut test_ctx = TestContext::new(SessionContext::new_with_config(config));

        let file_name = relative_path.file_name().unwrap().to_str().unwrap();
        match file_name {
            "information_schema_table_types.slt" => {
                info!("Registering local temporary table");
                register_temp_table(test_ctx.session_ctx()).await;
            }
            "information_schema_columns.slt" => {
                info!("Registering table with many types");
                register_table_with_many_types(test_ctx.session_ctx()).await;
            }
            "map.slt" => {
                info!("Registering table with map");
                register_table_with_map(test_ctx.session_ctx()).await;
            }
            "avro.slt" => {
                #[cfg(feature = "avro")]
                {
                    info!("Registering avro tables");
                    register_avro_tables(&mut test_ctx).await;
                }
                #[cfg(not(feature = "avro"))]
                {
                    info!("Skipping {file_name} because avro feature is not enabled");
                    return None;
                }
            }
            "joins.slt" => {
                info!("Registering partition table tables");
                let example_udf = create_example_udf();
                test_ctx.ctx.register_udf(example_udf);
                register_partition_table(&mut test_ctx).await;
            }
            "metadata.slt" => {
                info!("Registering metadata table tables");
                register_metadata_tables(test_ctx.session_ctx()).await;
            }
            "union_datatype.slt" => {
                info!("Registering tables with union column");
                register_union_tables(test_ctx.session_ctx())
            }
            _ => {
                info!("Using default SessionContext");
            }
        };
        Some(test_ctx)
    }

    /// Enables the test directory feature. If not enabled,
    /// calling `testdir_path` will result in a panic.
    pub fn enable_testdir(&mut self) {
        if self.test_dir.is_none() {
            self.test_dir = Some(TempDir::new().expect("failed to create testdir"));
        }
    }

    /// Returns the path to the test directory. Panics if the test
    /// directory feature is not enabled via `enable_testdir`.
    pub fn testdir_path(&self) -> &Path {
        self.test_dir.as_ref().expect("testdir not enabled").path()
    }

    /// Returns a reference to the internal SessionContext
    pub fn session_ctx(&self) -> &SessionContext {
        &self.ctx
    }
}

#[cfg(feature = "avro")]
pub async fn register_avro_tables(ctx: &mut crate::TestContext) {
    use datafusion::prelude::AvroReadOptions;

    ctx.enable_testdir();

    let table_path = ctx.testdir_path().join("avro");
    std::fs::create_dir(&table_path).expect("failed to create avro table path");

    let testdata = datafusion::test_util::arrow_test_data();
    let alltypes_plain_file = format!("{testdata}/avro/alltypes_plain.avro");
    std::fs::copy(
        &alltypes_plain_file,
        format!("{}/alltypes_plain1.avro", table_path.display()),
    )
    .unwrap();
    std::fs::copy(
        &alltypes_plain_file,
        format!("{}/alltypes_plain2.avro", table_path.display()),
    )
    .unwrap();

    ctx.session_ctx()
        .register_avro(
            "alltypes_plain_multi_files",
            table_path.display().to_string().as_str(),
            AvroReadOptions::default(),
        )
        .await
        .unwrap();
}

/// Generate a partitioned CSV file and register it with an execution context
pub async fn register_partition_table(test_ctx: &mut TestContext) {
    test_ctx.enable_testdir();
    let partition_count = 1;
    let file_extension = "csv";
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::UInt32, false),
        Field::new("c2", DataType::UInt64, false),
        Field::new("c3", DataType::Boolean, false),
    ]));
    // generate a partitioned file
    for partition in 0..partition_count {
        let filename = format!("partition-{partition}.{file_extension}");
        let file_path = test_ctx.testdir_path().join(filename);
        let mut file = File::create(file_path).unwrap();

        // generate some data
        for i in 0..=10 {
            let data = format!("{},{},{}\n", partition, i, i % 2 == 0);
            file.write_all(data.as_bytes()).unwrap()
        }
    }

    // register csv file with the execution context
    test_ctx
        .ctx
        .register_csv(
            "test_partition_table",
            test_ctx.testdir_path().to_str().unwrap(),
            CsvReadOptions::new().schema(&schema),
        )
        .await
        .unwrap();
}

// registers a LOCAL TEMPORARY table.
pub async fn register_temp_table(ctx: &SessionContext) {
    struct TestTable(TableType);

    #[async_trait]
    impl TableProvider for TestTable {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn table_type(&self) -> TableType {
            self.0
        }

        fn schema(&self) -> SchemaRef {
            unimplemented!()
        }

        async fn scan(
            &self,
            _state: &dyn Session,
            _: Option<&Vec<usize>>,
            _: &[Expr],
            _: Option<usize>,
        ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
            unimplemented!()
        }
    }

    ctx.register_table(
        "datafusion.public.temp",
        Arc::new(TestTable(TableType::Temporary)),
    )
    .unwrap();
}

pub async fn register_table_with_many_types(ctx: &SessionContext) {
    let catalog = MemoryCatalogProvider::new();
    let schema = MemorySchemaProvider::new();

    catalog
        .register_schema("my_schema", Arc::new(schema))
        .unwrap();
    ctx.register_catalog("my_catalog", Arc::new(catalog));

    ctx.register_table("my_catalog.my_schema.t2", table_with_many_types())
        .unwrap();
}

pub async fn register_table_with_map(ctx: &SessionContext) {
    let key = Field::new("key", DataType::Int64, false);
    let value = Field::new("value", DataType::Int64, true);
    let map_field =
        Field::new("entries", DataType::Struct(vec![key, value].into()), false);
    let fields = vec![
        Field::new("int_field", DataType::Int64, true),
        Field::new("map_field", DataType::Map(map_field.into(), false), true),
    ];
    let schema = Schema::new(fields);

    let memory_table = MemTable::try_new(schema.into(), vec![vec![]]).unwrap();

    ctx.register_table("table_with_map", Arc::new(memory_table))
        .unwrap();
}

fn table_with_many_types() -> Arc<dyn TableProvider> {
    let schema = Schema::new(vec![
        Field::new("int32_col", DataType::Int32, false),
        Field::new("float64_col", DataType::Float64, true),
        Field::new("utf8_col", DataType::Utf8, true),
        Field::new("large_utf8_col", DataType::LargeUtf8, false),
        Field::new("binary_col", DataType::Binary, false),
        Field::new("large_binary_col", DataType::LargeBinary, false),
        Field::new(
            "timestamp_nanos",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(StringArray::from(vec![Some("foo")])),
            Arc::new(LargeStringArray::from(vec![Some("bar")])),
            Arc::new(BinaryArray::from(vec![b"foo" as &[u8]])),
            Arc::new(LargeBinaryArray::from(vec![b"foo" as &[u8]])),
            Arc::new(TimestampNanosecondArray::from(vec![Some(123)])),
        ],
    )
    .unwrap();
    let provider = MemTable::try_new(Arc::new(schema), vec![vec![batch]]).unwrap();
    Arc::new(provider)
}

/// Registers a table_with_metadata that contains both field level and Table level metadata
pub async fn register_metadata_tables(ctx: &SessionContext) {
    let id = Field::new("id", DataType::Int32, true).with_metadata(HashMap::from([(
        String::from("metadata_key"),
        String::from("the id field"),
    )]));
    let name = Field::new("name", DataType::Utf8, true).with_metadata(HashMap::from([(
        String::from("metadata_key"),
        String::from("the name field"),
    )]));

    let schema = Schema::new(vec![id, name]).with_metadata(HashMap::from([(
        String::from("metadata_key"),
        String::from("the entire schema"),
    )]));

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as _,
            Arc::new(StringArray::from(vec![None, Some("bar"), Some("baz")])) as _,
        ],
    )
    .unwrap();

    ctx.register_batch("table_with_metadata", batch).unwrap();
}

/// Create a UDF function named "example". See the `sample_udf.rs` example
/// file for an explanation of the API.
fn create_example_udf() -> ScalarUDF {
    let adder = Arc::new(|args: &[ColumnarValue]| {
        let ColumnarValue::Array(lhs) = &args[0] else {
            panic!("should be array")
        };
        let ColumnarValue::Array(rhs) = &args[1] else {
            panic!("should be array")
        };

        let lhs = as_float64_array(lhs).expect("cast failed");
        let rhs = as_float64_array(rhs).expect("cast failed");
        let array = lhs
            .iter()
            .zip(rhs.iter())
            .map(|(lhs, rhs)| match (lhs, rhs) {
                (Some(lhs), Some(rhs)) => Some(lhs + rhs),
                _ => None,
            })
            .collect::<Float64Array>();
        Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
    });
    create_udf(
        "example",
        // Expects two f64 values:
        vec![DataType::Float64, DataType::Float64],
        // Returns an f64 value:
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        adder,
    )
}

fn register_union_tables(ctx: &SessionContext) {
    sparse_1_1_single_field(ctx);
    sparse_1_2_empty(ctx);
    sparse_1_3a_null_target(ctx);
    sparse_1_3b_null_target(ctx);
    sparse_2_all_types_match(ctx);
    sparse_3_1_none_match_target_can_contain_null_mask(ctx);
    sparse_3_2_none_match_cant_contain_null_mask_union_target(ctx);
    sparse_4_1_1_target_with_nulls(ctx);
    sparse_4_1_2_target_without_nulls(ctx);
    sparse_4_2_some_match_target_cant_contain_null_mask(ctx);
    dense_1_1_both_empty(ctx);
    dense_1_2_empty_union_target_non_empty(ctx);
    dense_2_non_empty_union_target_empty(ctx);
    dense_3_1_null_target_smaller_len(ctx);
    dense_3_2_null_target_equal_len(ctx);
    dense_3_3_null_target_bigger_len(ctx);
    dense_4_1a_single_type_sequential_offsets_equal_len(ctx);
    dense_4_2a_single_type_sequential_offsets_bigger(ctx);
    dense_4_3a_single_type_non_sequential(ctx);
    dense_4_1b_empty_siblings_sequential_equal_len(ctx);
    dense_4_2b_empty_siblings_sequential_bigger_len(ctx);
    dense_4_3b_empty_sibling_non_sequential(ctx);
    dense_4_1c_all_types_match_sequential_equal_len(ctx);
    dense_4_2c_all_types_match_sequential_bigger_len(ctx);
    dense_4_3c_all_types_match_non_sequential(ctx);
    dense_5_1a_none_match_less_len(ctx);
    dense_5_1b_cant_contain_null_mask(ctx);
    dense_5_2_none_match_equal_len(ctx);
    dense_5_3_none_match_greater_len(ctx);
    dense_6_some_matches(ctx);
    empty_sparse_union(ctx);
    empty_dense_union(ctx);
}

fn register_union_table(
    ctx: &SessionContext,
    union: UnionArray,
    table_name: &str,
    expected: impl Array + 'static,
) {
    let schema = Schema::new(vec![
        Field::new("my_union", union.data_type().clone(), false),
        Field::new("expected", expected.data_type().clone(), true),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(union), Arc::new(expected)],
    )
    .unwrap();

    ctx.register_batch(table_name, batch).unwrap();
}

fn sparse_1_1_single_field(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        //single field
        UnionFields::new(vec![3], vec![Field::new("int", DataType::Int32, false)]),
        ScalarBuffer::from(vec![3, 3]), // non empty, every type id must match
        None,                           //sparse
        vec![
            Arc::new(Int32Array::from(vec![1, 2])), // not null
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "sparse_1_1_single_field",
        Int32Array::from(vec![1, 2]),
    );
}

fn sparse_1_2_empty(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        // multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false), //target type is not Null
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![]), //empty union
        None,                       // sparse
        vec![
            Arc::new(StringArray::new_null(0)),
            Arc::new(Int32Array::new_null(0)),
        ],
    )
    .unwrap();

    register_union_table(ctx, union, "sparse_1_2_empty", StringArray::new_null(0));
}

fn sparse_1_3a_null_target(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        // multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("null", DataType::Null, true),
            ],
        ),
        ScalarBuffer::from(vec![1]), //not empty
        None,                        // sparse
        vec![
            Arc::new(StringArray::new_null(1)),
            Arc::new(NullArray::new(1)), // null data type
        ],
    )
    .unwrap();

    register_union_table(ctx, union, "sparse_1_3a_null_target", NullArray::new(1));
}

fn sparse_1_3b_null_target(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        // multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false), //target type is not Null
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![1]), //not empty
        None,                        // sparse
        vec![
            Arc::new(StringArray::new_null(1)), //all null
            Arc::new(Int32Array::new_null(1)),
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "sparse_1_3b_null_target",
        StringArray::new_null(1),
    );
}

fn sparse_2_all_types_match(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        //multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![3, 3]), // all types match
        None,                           //sparse
        vec![
            Arc::new(StringArray::new_null(2)),
            Arc::new(Int32Array::from(vec![1, 4])), // not null
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "sparse_2_all_types_match",
        Int32Array::from(vec![1, 4]),
    );
}

fn sparse_3_1_none_match_target_can_contain_null_mask(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        //multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![1, 1, 1, 1]), // none match
        None,                                 // sparse
        vec![
            Arc::new(StringArray::new_null(4)),
            Arc::new(Int32Array::from(vec![None, Some(4), None, Some(8)])), // target is not null
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "sparse_3_1_none_match_target_can_contain_null_mask",
        Int32Array::new_null(4),
    );
}

fn sparse_3_2_none_match_cant_contain_null_mask_union_target(ctx: &SessionContext) {
    let target_fields =
        UnionFields::new([10], [Field::new("bool", DataType::Boolean, true)]);

    let target_data_type = DataType::Union(target_fields.clone(), UnionMode::Sparse);

    let union = UnionArray::try_new(
        //multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("union", target_data_type.clone(), false),
            ],
        ),
        ScalarBuffer::from(vec![1, 1]), // none match
        None,                           //sparse
        vec![
            Arc::new(StringArray::new_null(2)),
            //target is not null
            Arc::new(
                UnionArray::try_new(
                    target_fields.clone(),
                    ScalarBuffer::from(vec![10, 10]),
                    None,
                    vec![Arc::new(BooleanArray::from(vec![true, false]))],
                )
                .unwrap(),
            ),
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "sparse_3_2_none_match_cant_contain_null_mask_union_target",
        new_null_array(&target_data_type, 2),
    );
}

fn sparse_4_1_1_target_with_nulls(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        //multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![3, 3, 1, 1]), // multiple selected types
        None,                                 // sparse
        vec![
            Arc::new(StringArray::new_null(4)),
            Arc::new(Int32Array::from(vec![None, Some(4), None, Some(8)])), // target with nulls
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "sparse_4_1_1_target_with_nulls",
        Int32Array::from(vec![None, Some(4), None, None]),
    );
}

fn sparse_4_1_2_target_without_nulls(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        //multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![1, 3, 3]), // multiple selected types
        None,                              // sparse
        vec![
            Arc::new(StringArray::new_null(3)),
            Arc::new(Int32Array::from(vec![2, 4, 8])), // target without nulls
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "sparse_4_1_2_target_without_nulls",
        Int32Array::from(vec![None, Some(4), Some(8)]),
    );
}

fn sparse_4_2_some_match_target_cant_contain_null_mask(ctx: &SessionContext) {
    let target_fields =
        UnionFields::new([10], [Field::new("bool", DataType::Boolean, true)]);

    let union = UnionArray::try_new(
        //multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new(
                    "union",
                    DataType::Union(target_fields.clone(), UnionMode::Sparse),
                    false,
                ),
            ],
        ),
        ScalarBuffer::from(vec![3, 1]), // some types match, but not all
        None,                           //sparse
        vec![
            Arc::new(StringArray::new_null(2)),
            Arc::new(
                UnionArray::try_new(
                    target_fields.clone(),
                    ScalarBuffer::from(vec![10, 10]),
                    None,
                    vec![Arc::new(BooleanArray::from(vec![true, false]))],
                )
                .unwrap(),
            ),
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "sparse_4_2_some_match_target_cant_contain_null_mask",
        UnionArray::try_new(
            target_fields,
            ScalarBuffer::from(vec![10, 10]),
            None,
            vec![Arc::new(BooleanArray::from(vec![Some(true), None]))],
        )
        .unwrap(),
    );
}

fn dense_1_1_both_empty(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![]),       //empty union
        Some(ScalarBuffer::from(vec![])), // dense
        vec![
            Arc::new(StringArray::new_null(0)), //empty target
            Arc::new(Int32Array::new_null(0)),
        ],
    )
    .unwrap();

    register_union_table(ctx, union, "dense_1_1_both_empty", StringArray::new_null(0));
}

fn dense_1_2_empty_union_target_non_empty(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![]),       //empty union
        Some(ScalarBuffer::from(vec![])), // dense
        vec![
            Arc::new(StringArray::new_null(1)), //non empty target
            Arc::new(Int32Array::new_null(0)),
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_1_2_empty_union_target_non_empty",
        StringArray::new_null(0),
    );
}

fn dense_2_non_empty_union_target_empty(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![3, 3]), //non empty union
        Some(ScalarBuffer::from(vec![0, 1])), // dense
        vec![
            Arc::new(StringArray::new_null(0)), //empty target
            Arc::new(Int32Array::new_null(2)),
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_2_non_empty_union_target_empty",
        StringArray::new_null(2),
    );
}

fn dense_3_1_null_target_smaller_len(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![3, 3]), //non empty union
        Some(ScalarBuffer::from(vec![0, 0])), //dense
        vec![
            Arc::new(StringArray::new_null(1)), //smaller target
            Arc::new(Int32Array::new_null(2)),
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_3_1_null_target_smaller_len",
        StringArray::new_null(2),
    );
}

fn dense_3_2_null_target_equal_len(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![3, 3]), //non empty union
        Some(ScalarBuffer::from(vec![0, 0])), //dense
        vec![
            Arc::new(StringArray::new_null(2)), //equal len
            Arc::new(Int32Array::new_null(2)),
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_3_2_null_target_equal_len",
        StringArray::new_null(2),
    );
}

fn dense_3_3_null_target_bigger_len(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![3, 3]), //non empty union
        Some(ScalarBuffer::from(vec![0, 0])), //dense
        vec![
            Arc::new(StringArray::new_null(3)), //bigger len
            Arc::new(Int32Array::new_null(3)),
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_3_3_null_target_bigger_len",
        StringArray::new_null(2),
    );
}

fn dense_4_1a_single_type_sequential_offsets_equal_len(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        // single field
        UnionFields::new(vec![1], vec![Field::new("str", DataType::Utf8, false)]),
        ScalarBuffer::from(vec![1, 1]), //non empty union
        Some(ScalarBuffer::from(vec![0, 1])), //sequential
        vec![
            Arc::new(StringArray::from_iter_values(["a1", "b2"])), //equal len, non null
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_4_1a_single_type_sequential_offsets_equal_len",
        StringArray::from_iter_values(["a1", "b2"]),
    );
}

fn dense_4_2a_single_type_sequential_offsets_bigger(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        // single field
        UnionFields::new(vec![1], vec![Field::new("str", DataType::Utf8, false)]),
        ScalarBuffer::from(vec![1, 1]), //non empty union
        Some(ScalarBuffer::from(vec![0, 1])), //sequential
        vec![
            Arc::new(StringArray::from_iter_values(["a1", "b2", "c3"])), //equal len, non null
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_4_2a_single_type_sequential_offsets_bigger",
        StringArray::from_iter_values(["a1", "b2"]),
    );
}

fn dense_4_3a_single_type_non_sequential(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        // single field
        UnionFields::new(vec![1], vec![Field::new("str", DataType::Utf8, false)]),
        ScalarBuffer::from(vec![1, 1]), //non empty union
        Some(ScalarBuffer::from(vec![0, 2])), //non sequential
        vec![
            Arc::new(StringArray::from_iter_values(["a1", "b2", "c3"])), //equal len, non null
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_4_3a_single_type_non_sequential",
        StringArray::from_iter_values(["a1", "c3"]),
    );
}

fn dense_4_1b_empty_siblings_sequential_equal_len(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        // multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![1, 1]), //non empty union
        Some(ScalarBuffer::from(vec![0, 1])), //sequential
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])), //equal len, non null
            Arc::new(Int32Array::new_null(0)),           //empty sibling
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_4_1b_empty_siblings_sequential_equal_len",
        StringArray::from(vec!["a", "b"]),
    );
}

fn dense_4_2b_empty_siblings_sequential_bigger_len(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        // multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![1, 1]), //non empty union
        Some(ScalarBuffer::from(vec![0, 1])), //sequential
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])), //bigger len, non null
            Arc::new(Int32Array::new_null(0)),                //empty sibling
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_4_2b_empty_siblings_sequential_bigger_len",
        StringArray::from(vec!["a", "b"]),
    );
}

fn dense_4_3b_empty_sibling_non_sequential(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        // multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![1, 1]), //non empty union
        Some(ScalarBuffer::from(vec![0, 2])), //non sequential
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c"])), //non null
            Arc::new(Int32Array::new_null(0)),                //empty sibling
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_4_3b_empty_sibling_non_sequential",
        StringArray::from(vec!["a", "c"]),
    );
}

fn dense_4_1c_all_types_match_sequential_equal_len(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        // multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![1, 1]), //all types match
        Some(ScalarBuffer::from(vec![0, 1])), //sequential
        vec![
            Arc::new(StringArray::from(vec!["a1", "b2"])), //equal len
            Arc::new(Int32Array::new_null(2)),             //non empty sibling
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_4_1c_all_types_match_sequential_equal_len",
        StringArray::from(vec!["a1", "b2"]),
    );
}

fn dense_4_2c_all_types_match_sequential_bigger_len(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        // multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![1, 1]), //all types match
        Some(ScalarBuffer::from(vec![0, 1])), //sequential
        vec![
            Arc::new(StringArray::from(vec!["a1", "b2", "b3"])), //bigger len
            Arc::new(Int32Array::new_null(2)),                   //non empty sibling
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_4_2c_all_types_match_sequential_bigger_len",
        StringArray::from(vec!["a1", "b2"]),
    );
}

fn dense_4_3c_all_types_match_non_sequential(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        // multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![1, 1]), //all types match
        Some(ScalarBuffer::from(vec![0, 2])), //non sequential
        vec![
            Arc::new(StringArray::from(vec!["a1", "b2", "b3"])),
            Arc::new(Int32Array::new_null(2)), //non empty sibling
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_4_3c_all_types_match_non_sequential",
        StringArray::from(vec!["a1", "b3"]),
    );
}

fn dense_5_1a_none_match_less_len(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        // multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![3, 3, 3, 3, 3]), //none matches
        Some(ScalarBuffer::from(vec![0, 0, 0, 1, 1])), // dense
        vec![
            Arc::new(StringArray::from(vec!["a1", "b2", "c3"])), // less len
            Arc::new(Int32Array::from(vec![1, 2])),
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_5_1a_none_match_less_len",
        StringArray::new_null(5),
    );
}

fn dense_5_1b_cant_contain_null_mask(ctx: &SessionContext) {
    let target_fields =
        UnionFields::new([10], [Field::new("bool", DataType::Boolean, true)]);

    let union = UnionArray::try_new(
        // multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new(
                    "union",
                    DataType::Union(target_fields.clone(), UnionMode::Sparse),
                    false,
                ),
            ],
        ),
        ScalarBuffer::from(vec![1, 1, 1, 1, 1]), //none matches
        Some(ScalarBuffer::from(vec![0, 0, 0, 1, 1])), // dense
        vec![
            Arc::new(StringArray::from(vec!["a1", "b2", "c3"])), // less len
            Arc::new(
                UnionArray::try_new(
                    target_fields.clone(),
                    ScalarBuffer::from(vec![10]),
                    None,
                    vec![Arc::new(BooleanArray::from(vec![true]))],
                )
                .unwrap(),
            ), // non empty
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_5_1b_cant_contain_null_mask",
        new_null_array(&DataType::Union(target_fields, UnionMode::Sparse), 5),
    );
}

fn dense_5_2_none_match_equal_len(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        // multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![3, 3, 3, 3, 3]), //none matches
        Some(ScalarBuffer::from(vec![0, 0, 0, 1, 1])), // dense
        vec![
            Arc::new(StringArray::from(vec!["a1", "b2", "c3", "d4", "e5"])), // equal len
            Arc::new(Int32Array::from(vec![1, 2])),
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_5_2_none_match_equal_len",
        StringArray::new_null(5),
    );
}

fn dense_5_3_none_match_greater_len(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        // multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![3, 3, 3, 3, 3]), //none matches
        Some(ScalarBuffer::from(vec![0, 0, 0, 1, 1])), // dense
        vec![
            Arc::new(StringArray::from(vec!["a1", "b2", "c3", "d4", "e5", "f6"])), // greater len
            Arc::new(Int32Array::from(vec![1, 2])), //non null
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_5_3_none_match_greater_len",
        StringArray::new_null(5),
    );
}

fn dense_6_some_matches(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        // multiple fields
        UnionFields::new(
            vec![1, 3],
            vec![
                Field::new("str", DataType::Utf8, false),
                Field::new("int", DataType::Int32, false),
            ],
        ),
        ScalarBuffer::from(vec![3, 3, 1, 1, 1]), //some matches
        Some(ScalarBuffer::from(vec![0, 1, 0, 1, 2])), // dense
        vec![
            Arc::new(StringArray::from(vec!["a1", "b2", "c3"])), // non null
            Arc::new(Int32Array::from(vec![1, 2])),
        ],
    )
    .unwrap();

    register_union_table(
        ctx,
        union,
        "dense_6_some_matches",
        Int32Array::from(vec![Some(1), Some(2), None, None, None]),
    );
}

fn empty_sparse_union(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        UnionFields::empty(),
        ScalarBuffer::from(vec![]),
        None,
        vec![],
    )
    .unwrap();

    register_union_table(ctx, union, "empty_sparse_union", NullArray::new(0))
}

fn empty_dense_union(ctx: &SessionContext) {
    let union = UnionArray::try_new(
        UnionFields::empty(),
        ScalarBuffer::from(vec![]),
        Some(ScalarBuffer::from(vec![])),
        vec![],
    )
    .unwrap();

    register_union_table(ctx, union, "empty_dense_union", NullArray::new(0))
}
