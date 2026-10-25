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

//! Tests for parquet schema handling
use std::{collections::HashMap, fs, path::Path};

use arrow::array::{Array, ArrayRef, LargeListArray, ListArray, StructArray};
use arrow::buffer::OffsetBuffer;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use tempfile::TempDir;

use super::*;
use datafusion_common::test_util::batches_to_sort_string;
use insta::assert_snapshot;

#[tokio::test]
async fn schema_merge_ignores_metadata_by_default() {
    // Create several parquet files in same directory / table with
    // same schema but different metadata
    let tmp_dir = TempDir::new().unwrap();
    let table_dir = tmp_dir.path().join("parquet_test");

    let options = ParquetReadOptions::default();

    let f1 = Field::new("id", DataType::Int32, true);
    let f2 = Field::new("name", DataType::Utf8, true);

    let schemas = vec![
        // schema level metadata
        Schema::new(vec![f1.clone(), f2.clone()]).with_metadata(make_meta("foo", "bar")),
        // schema different (incompatible) metadata
        Schema::new(vec![f1.clone(), f2.clone()]).with_metadata(make_meta("foo", "baz")),
        // schema with no meta
        Schema::new(vec![f1.clone(), f2.clone()]),
        // field level metadata
        Schema::new(vec![
            f1.clone().with_metadata(make_meta("blarg", "bar")),
            f2.clone(),
        ]),
        // incompatible field level metadata
        Schema::new(vec![
            f1.clone().with_metadata(make_meta("blarg", "baz")),
            f2.clone(),
        ]),
        // schema with no meta
        Schema::new(vec![f1, f2]),
    ];
    write_files(table_dir.as_path(), schemas);

    // Read the parquet files into a dataframe to confirm results
    // (no errors)
    let table_path = table_dir.to_str().unwrap().to_string();

    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(&table_path, options.clone())
        .await
        .unwrap();
    let actual = df.collect().await.unwrap();

    assert_snapshot!(batches_to_sort_string(&actual), @r"
    +----+------+
    | id | name |
    +----+------+
    | 0  | test |
    | 1  | test |
    | 2  | test |
    | 3  | test |
    | 4  | test |
    | 5  | test |
    +----+------+
    ");
    assert_no_metadata(&actual);

    // also validate it works via SQL interface as well
    ctx.register_parquet("t", &table_path, options)
        .await
        .unwrap();

    let actual = ctx
        .sql("SELECT * from t")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_snapshot!(batches_to_sort_string(&actual), @r"
    +----+------+
    | id | name |
    +----+------+
    | 0  | test |
    | 1  | test |
    | 2  | test |
    | 3  | test |
    | 4  | test |
    | 5  | test |
    +----+------+
    ");
    assert_no_metadata(&actual);
}

#[tokio::test]
async fn schema_merge_can_preserve_metadata() {
    // Create several parquet files in same directory / table with
    // same schema but different metadata
    let tmp_dir = TempDir::new().unwrap();
    let table_dir = tmp_dir.path().join("parquet_test");

    // explicitly disable schema clearing
    let options = ParquetReadOptions::default().skip_metadata(false);

    let f1 = Field::new("id", DataType::Int32, true);
    let f2 = Field::new("name", DataType::Utf8, true);

    let schemas = vec![
        // schema level metadata
        Schema::new(vec![f1.clone(), f2.clone()]).with_metadata(make_meta("foo", "bar")),
        // schema different (compatible) metadata
        Schema::new(vec![f1.clone(), f2.clone()]).with_metadata(make_meta("foo2", "baz")),
        // schema with no meta
        Schema::new(vec![f1.clone(), f2.clone()]),
    ];
    write_files(table_dir.as_path(), schemas);

    let mut expected_metadata = make_meta("foo", "bar");
    expected_metadata.insert("foo2".into(), "baz".into());

    // Read the parquet files into a dataframe to confirm results
    // (no errors)
    let table_path = table_dir.to_str().unwrap().to_string();

    let ctx = SessionContext::new();
    let df = ctx
        .read_parquet(&table_path, options.clone())
        .await
        .unwrap();

    let actual = df.schema().metadata();
    assert_eq!(actual.clone(), expected_metadata,);

    let actual = df.collect().await.unwrap();

    assert_snapshot!(batches_to_sort_string(&actual), @r"
    +----+------+
    | id | name |
    +----+------+
    | 0  | test |
    | 1  | test |
    | 2  | test |
    +----+------+
    ");
    assert_metadata(&actual, &expected_metadata);

    // also validate it works via SQL interface as well
    ctx.register_parquet("t", &table_path, options)
        .await
        .unwrap();

    let df = ctx.sql("SELECT * from t").await.unwrap();

    let actual = df.schema().metadata();
    assert_eq!(actual.clone(), expected_metadata);

    let actual = df.collect().await.unwrap();
    assert_snapshot!(batches_to_sort_string(&actual), @r"
    +----+------+
    | id | name |
    +----+------+
    | 0  | test |
    | 1  | test |
    | 2  | test |
    +----+------+
    ");
    assert_metadata(&actual, &expected_metadata);
}

/// A column that only some files carry must be inferred as nullable, because
/// reading the files without it yields nulls, even when every file that has
/// the column declares it required.
#[tokio::test]
async fn schema_merge_marks_partially_present_columns_nullable() {
    let tmp_dir = TempDir::new().unwrap();
    let table_dir = tmp_dir.path().join("parquet_test");

    let id = Field::new("id", DataType::Int32, false);
    let name = Field::new("name", DataType::Utf8, false);
    let extra = Field::new("extra", DataType::Int32, false);
    let opt = Field::new("opt", DataType::Int32, true);

    let schemas = vec![
        // required `extra` and nullable `opt` in the first file only
        Schema::new(vec![id.clone(), name.clone(), extra.clone(), opt]),
        // neither column
        Schema::new(vec![id.clone(), name.clone()]),
        // required `extra` again, in a different position
        Schema::new(vec![extra, id, name]),
    ];
    write_files_with_columns(table_dir.as_path(), schemas);
    let table_path = table_dir.to_str().unwrap().to_string();

    for skip_metadata in [true, false] {
        let options = ParquetReadOptions::default().skip_metadata(skip_metadata);
        let ctx = SessionContext::new();
        let df = ctx.read_parquet(&table_path, options).await.unwrap();

        let nullability: Vec<(&str, bool)> = df
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().as_str(), f.is_nullable()))
            .collect();
        assert_eq!(
            nullability,
            vec![
                ("id", false),
                ("name", false),
                ("extra", true),
                ("opt", true)
            ],
            "skip_metadata={skip_metadata}"
        );

        let actual = df.collect().await.unwrap();
        let expected = [
            "+----+------+-------+-----+",
            "| id | name | extra | opt |",
            "+----+------+-------+-----+",
            "| 0  | test | 0     |     |",
            "| 1  | test |       |     |",
            "| 2  | test | 2     |     |",
            "+----+------+-------+-----+",
        ]
        .join("\n");
        assert_eq!(
            batches_to_sort_string(&actual).trim(),
            expected,
            "skip_metadata={skip_metadata}"
        );
    }
}

fn make_meta(k: impl Into<String>, v: impl Into<String>) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    meta.insert(k.into(), v.into());
    meta
}

/// Writes individual files with the specified schemas to temp_path)
///
/// Assumes each schema has an int32 and a string column
fn write_files(table_path: &Path, schemas: Vec<Schema>) {
    fs::create_dir(table_path).expect("Error creating temp dir");

    for (i, schema) in schemas.into_iter().enumerate() {
        let schema = Arc::new(schema);
        let filename = format!("part-{i}.parquet");
        let path = table_path.join(filename);
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

        // create mock record batch
        let ids = Arc::new(Int32Array::from(vec![i as i32]));
        let names = Arc::new(StringArray::from(vec!["test"]));
        let rec_batch = RecordBatch::try_new(schema.clone(), vec![ids, names]).unwrap();

        writer.write(&rec_batch).unwrap();
        writer.close().unwrap();
    }
}

/// Writes one file per schema, filling each column from its name: `id` and
/// `extra` get the file index, `name` gets "test", anything else is null.
fn write_files_with_columns(table_path: &Path, schemas: Vec<Schema>) {
    fs::create_dir(table_path).expect("Error creating temp dir");

    for (i, schema) in schemas.into_iter().enumerate() {
        let schema = Arc::new(schema);
        let path = table_path.join(format!("part-{i}.parquet"));
        let file = fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();

        let columns: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "id" | "extra" => Arc::new(Int32Array::from(vec![i as i32])) as ArrayRef,
                "name" => Arc::new(StringArray::from(vec!["test"])),
                _ => Arc::new(Int32Array::from(vec![None::<i32>])),
            })
            .collect();
        let rec_batch = RecordBatch::try_new(schema, columns).unwrap();

        writer.write(&rec_batch).unwrap();
        writer.close().unwrap();
    }
}

fn assert_no_metadata(batches: &[RecordBatch]) {
    // all batches should have no metadata
    for batch in batches {
        assert!(
            batch.schema().metadata().is_empty(),
            "schema had metadata: {:?}",
            batch.schema()
        );
    }
}

fn assert_metadata(batches: &[RecordBatch], expected_metadata: &HashMap<String, String>) {
    // all batches should have no metadata
    for batch in batches {
        assert_eq!(batch.schema().metadata(), expected_metadata,);
    }
}

#[tokio::test]
async fn schema_merge_marks_missing_nested_children_nullable() {
    for kind in ["struct", "list", "large_list"] {
        let dir = TempDir::new().unwrap();
        for (name, id, with_y) in [("a.parquet", 1, true), ("b.parquet", 2, false)] {
            let mut fields = vec![Arc::new(Field::new("x", DataType::Int32, false))];
            let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int32Array::from(vec![id]))];
            if with_y {
                fields.push(Arc::new(Field::new("y", DataType::Int32, false)));
                arrays.push(Arc::new(Int32Array::from(vec![10])));
            }
            let s = StructArray::new(fields.into(), arrays, None);
            let s: ArrayRef = match kind {
                "struct" => Arc::new(s),
                "list" => Arc::new(ListArray::new(
                    Arc::new(Field::new("item", s.data_type().clone(), false)),
                    OffsetBuffer::new(vec![0_i32, 1].into()),
                    Arc::new(s),
                    None,
                )),
                "large_list" => Arc::new(LargeListArray::new(
                    Arc::new(Field::new("item", s.data_type().clone(), false)),
                    OffsetBuffer::new(vec![0_i64, 1].into()),
                    Arc::new(s),
                    None,
                )),
                _ => unreachable!(),
            };
            write_nested_file(
                &dir,
                name,
                vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("s", s.data_type().clone(), false),
                ],
                vec![Arc::new(Int32Array::from(vec![id])), s],
                EnabledStatistics::Page,
            );
        }
        for skip_metadata in [true, false] {
            for pushdown in [true, false] {
                let config = SessionConfig::new()
                    .set_bool("datafusion.execution.parquet.pushdown_filters", pushdown);
                let ctx = SessionContext::new_with_config(config);
                ctx.register_parquet(
                    "t",
                    dir.path().to_str().unwrap(),
                    ParquetReadOptions::default().skip_metadata(skip_metadata),
                )
                .await
                .unwrap();
                let df = ctx.table("t").await.unwrap();
                let mut data_type = df
                    .schema()
                    .field_with_unqualified_name("s")
                    .unwrap()
                    .data_type();
                if let DataType::List(item) | DataType::LargeList(item) = data_type {
                    assert!(!item.is_nullable());
                    data_type = item.data_type();
                }
                let DataType::Struct(fields) = data_type else {
                    panic!("expected struct")
                };
                let child = if kind == "struct" {
                    "s.y"
                } else {
                    "get_field(s[1], 'y')"
                };
                assert!(!fields[0].is_nullable());
                assert!(fields[1].is_nullable());
                let batches = ctx
                    .sql(&format!("SELECT id, {child} AS y FROM t ORDER BY id"))
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                assert_eq!(
                    batches_to_sort_string(&batches).trim(),
                    "+----+----+\n| id | y  |\n+----+----+\n| 1  | 10 |\n| 2  |    |\n+----+----+"
                );
                let batches = ctx
                    .sql(&format!("SELECT id FROM t WHERE {child} IS NULL"))
                    .await
                    .unwrap()
                    .collect()
                    .await
                    .unwrap();
                assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
                assert_eq!(
                    batches[0]
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(0),
                    2
                );
            }
        }
    }
}

fn write_nested_file(
    dir: &TempDir,
    name: &str,
    fields: Vec<Field>,
    arrays: Vec<ArrayRef>,
    statistics: EnabledStatistics,
) {
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .set_statistics_enabled(statistics)
        .build();
    let mut writer = ArrowWriter::try_new(
        fs::File::create(dir.path().join(name)).unwrap(),
        schema,
        Some(props),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}
