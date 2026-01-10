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

//! Functions that are query-able and searchable via the `\h` command

use datafusion_common::instant::Instant;
use std::fmt;
use std::fs::File;
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::{
    DurationMillisecondArray, GenericListArray, Int64Array, StringArray, StructArray,
    TimestampMillisecondArray, UInt64Array,
};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::{Session, TableFunctionImpl};
use datafusion::common::{Column, plan_err};
use datafusion::datasource::TableProvider;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::Result;
use datafusion::execution::cache::cache_manager::CacheManager;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;

use async_trait::async_trait;
use parquet::basic::ConvertedType;
use parquet::data_type::{ByteArray, FixedLenByteArray};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::statistics::Statistics;

#[derive(Debug)]
pub enum Function {
    Select,
    Explain,
    Show,
    CreateTable,
    CreateTableAs,
    Insert,
    DropTable,
}

const ALL_FUNCTIONS: [Function; 7] = [
    Function::CreateTable,
    Function::CreateTableAs,
    Function::DropTable,
    Function::Explain,
    Function::Insert,
    Function::Select,
    Function::Show,
];

impl Function {
    pub fn function_details(&self) -> Result<&str> {
        let details = match self {
            Function::Select => {
                r#"
Command:     SELECT
Description: retrieve rows from a table or view
Syntax:
SELECT [ ALL | DISTINCT [ ON ( expression [, ...] ) ] ]
    [ * | expression [ [ AS ] output_name ] [, ...] ]
    [ FROM from_item [, ...] ]
    [ WHERE condition ]
    [ GROUP BY [ ALL | DISTINCT ] grouping_element [, ...] ]
    [ HAVING condition ]
    [ WINDOW window_name AS ( window_definition ) [, ...] ]
    [ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] select ]
    [ ORDER BY expression [ ASC | DESC | USING operator ] [ NULLS { FIRST | LAST } ] [, ...] ]
    [ LIMIT { count | ALL } ]
    [ OFFSET start [ ROW | ROWS ] ]

where from_item can be one of:

    [ ONLY ] table_name [ * ] [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
                [ TABLESAMPLE sampling_method ( argument [, ...] ) [ REPEATABLE ( seed ) ] ]
    [ LATERAL ] ( select ) [ AS ] alias [ ( column_alias [, ...] ) ]
    with_query_name [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
    [ LATERAL ] function_name ( [ argument [, ...] ] )
                [ WITH ORDINALITY ] [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
    [ LATERAL ] function_name ( [ argument [, ...] ] ) [ AS ] alias ( column_definition [, ...] )
    [ LATERAL ] function_name ( [ argument [, ...] ] ) AS ( column_definition [, ...] )
    [ LATERAL ] ROWS FROM( function_name ( [ argument [, ...] ] ) [ AS ( column_definition [, ...] ) ] [, ...] )
                [ WITH ORDINALITY ] [ [ AS ] alias [ ( column_alias [, ...] ) ] ]
    from_item [ NATURAL ] join_type from_item [ ON join_condition | USING ( join_column [, ...] ) [ AS join_using_alias ] ]

and grouping_element can be one of:

    ( )
    expression
    ( expression [, ...] )

and with_query is:

    with_query_name [ ( column_name [, ...] ) ] AS [ [ NOT ] MATERIALIZED ] ( select | values | insert | update | delete )

TABLE [ ONLY ] table_name [ * ]"#
            }
            Function::Explain => {
                r#"
Command:     EXPLAIN
Description: show the execution plan of a statement
Syntax:
EXPLAIN [ ANALYZE ] statement
"#
            }
            Function::Show => {
                r#"
Command:     SHOW
Description: show the value of a run-time parameter
Syntax:
SHOW name
"#
            }
            Function::CreateTable => {
                r#"
Command:     CREATE TABLE
Description: define a new table
Syntax:
CREATE [ EXTERNAL ]  TABLE table_name ( [
  { column_name data_type }
    [, ... ]
] )
"#
            }
            Function::CreateTableAs => {
                r#"
Command:     CREATE TABLE AS
Description: define a new table from the results of a query
Syntax:
CREATE TABLE table_name
    [ (column_name [, ...] ) ]
    AS query
    [ WITH [ NO ] DATA ]
"#
            }
            Function::Insert => {
                r#"
Command:     INSERT
Description: create new rows in a table
Syntax:
INSERT INTO table_name [ ( column_name [, ...] ) ]
    { VALUES ( { expression } [, ...] ) [, ...] }
"#
            }
            Function::DropTable => {
                r#"
Command:     DROP TABLE
Description: remove a table
Syntax:
DROP TABLE [ IF EXISTS ] name [, ...]
"#
            }
        };
        Ok(details)
    }
}

impl FromStr for Function {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s.trim().to_uppercase().as_str() {
            "SELECT" => Self::Select,
            "EXPLAIN" => Self::Explain,
            "SHOW" => Self::Show,
            "CREATE TABLE" => Self::CreateTable,
            "CREATE TABLE AS" => Self::CreateTableAs,
            "INSERT" => Self::Insert,
            "DROP TABLE" => Self::DropTable,
            _ => return Err(()),
        })
    }
}

impl fmt::Display for Function {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Function::Select => write!(f, "SELECT"),
            Function::Explain => write!(f, "EXPLAIN"),
            Function::Show => write!(f, "SHOW"),
            Function::CreateTable => write!(f, "CREATE TABLE"),
            Function::CreateTableAs => write!(f, "CREATE TABLE AS"),
            Function::Insert => write!(f, "INSERT"),
            Function::DropTable => write!(f, "DROP TABLE"),
        }
    }
}

pub fn display_all_functions() -> Result<()> {
    println!("Available help:");
    let array = StringArray::from(
        ALL_FUNCTIONS
            .iter()
            .map(|f| format!("{f}"))
            .collect::<Vec<String>>(),
    );
    let schema = Schema::new(vec![Field::new("Function", DataType::Utf8, false)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)])?;
    println!("{}", pretty_format_batches(&[batch]).unwrap());
    Ok(())
}

/// PARQUET_META table function
#[derive(Debug)]
struct ParquetMetadataTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for ParquetMetadataTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![self.batch.clone()]],
            TableProvider::schema(self),
            projection.cloned(),
        )?)
    }
}

fn convert_parquet_statistics(
    value: &Statistics,
    converted_type: ConvertedType,
) -> (Option<String>, Option<String>) {
    match (value, converted_type) {
        (Statistics::Boolean(val), _) => (
            val.min_opt().map(|v| v.to_string()),
            val.max_opt().map(|v| v.to_string()),
        ),
        (Statistics::Int32(val), _) => (
            val.min_opt().map(|v| v.to_string()),
            val.max_opt().map(|v| v.to_string()),
        ),
        (Statistics::Int64(val), _) => (
            val.min_opt().map(|v| v.to_string()),
            val.max_opt().map(|v| v.to_string()),
        ),
        (Statistics::Int96(val), _) => (
            val.min_opt().map(|v| v.to_string()),
            val.max_opt().map(|v| v.to_string()),
        ),
        (Statistics::Float(val), _) => (
            val.min_opt().map(|v| v.to_string()),
            val.max_opt().map(|v| v.to_string()),
        ),
        (Statistics::Double(val), _) => (
            val.min_opt().map(|v| v.to_string()),
            val.max_opt().map(|v| v.to_string()),
        ),
        (Statistics::ByteArray(val), ConvertedType::UTF8) => (
            byte_array_to_string(val.min_opt()),
            byte_array_to_string(val.max_opt()),
        ),
        (Statistics::ByteArray(val), _) => (
            val.min_opt().map(|v| v.to_string()),
            val.max_opt().map(|v| v.to_string()),
        ),
        (Statistics::FixedLenByteArray(val), ConvertedType::UTF8) => (
            fixed_len_byte_array_to_string(val.min_opt()),
            fixed_len_byte_array_to_string(val.max_opt()),
        ),
        (Statistics::FixedLenByteArray(val), _) => (
            val.min_opt().map(|v| v.to_string()),
            val.max_opt().map(|v| v.to_string()),
        ),
    }
}

/// Convert to a string if it has utf8 encoding, otherwise print bytes directly
fn byte_array_to_string(val: Option<&ByteArray>) -> Option<String> {
    val.map(|v| {
        v.as_utf8()
            .map(|s| s.to_string())
            .unwrap_or_else(|_e| v.to_string())
    })
}

/// Convert to a string if it has utf8 encoding, otherwise print bytes directly
fn fixed_len_byte_array_to_string(val: Option<&FixedLenByteArray>) -> Option<String> {
    val.map(|v| {
        v.as_utf8()
            .map(|s| s.to_string())
            .unwrap_or_else(|_e| v.to_string())
    })
}

#[derive(Debug)]
pub struct ParquetMetadataFunc {}

impl TableFunctionImpl for ParquetMetadataFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let filename = match exprs.first() {
            Some(Expr::Literal(ScalarValue::Utf8(Some(s)), _)) => s, // single quote: parquet_metadata('x.parquet')
            Some(Expr::Column(Column { name, .. })) => name, // double quote: parquet_metadata("x.parquet")
            _ => {
                return plan_err!(
                    "parquet_metadata requires string argument as its input"
                );
            }
        };

        let file = File::open(filename.clone())?;
        let reader = SerializedFileReader::new(file)?;
        let metadata = reader.metadata();

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("row_group_id", DataType::Int64, true),
            Field::new("row_group_num_rows", DataType::Int64, true),
            Field::new("row_group_num_columns", DataType::Int64, true),
            Field::new("row_group_bytes", DataType::Int64, true),
            Field::new("column_id", DataType::Int64, true),
            Field::new("file_offset", DataType::Int64, true),
            Field::new("num_values", DataType::Int64, true),
            Field::new("path_in_schema", DataType::Utf8, true),
            Field::new("type", DataType::Utf8, true),
            Field::new("stats_min", DataType::Utf8, true),
            Field::new("stats_max", DataType::Utf8, true),
            Field::new("stats_null_count", DataType::Int64, true),
            Field::new("stats_distinct_count", DataType::Int64, true),
            Field::new("stats_min_value", DataType::Utf8, true),
            Field::new("stats_max_value", DataType::Utf8, true),
            Field::new("compression", DataType::Utf8, true),
            Field::new("encodings", DataType::Utf8, true),
            Field::new("index_page_offset", DataType::Int64, true),
            Field::new("dictionary_page_offset", DataType::Int64, true),
            Field::new("data_page_offset", DataType::Int64, true),
            Field::new("total_compressed_size", DataType::Int64, true),
            Field::new("total_uncompressed_size", DataType::Int64, true),
        ]));

        // construct record batch from metadata
        let mut filename_arr = vec![];
        let mut row_group_id_arr = vec![];
        let mut row_group_num_rows_arr = vec![];
        let mut row_group_num_columns_arr = vec![];
        let mut row_group_bytes_arr = vec![];
        let mut column_id_arr = vec![];
        let mut file_offset_arr = vec![];
        let mut num_values_arr = vec![];
        let mut path_in_schema_arr = vec![];
        let mut type_arr = vec![];
        let mut stats_min_arr = vec![];
        let mut stats_max_arr = vec![];
        let mut stats_null_count_arr = vec![];
        let mut stats_distinct_count_arr = vec![];
        let mut stats_min_value_arr = vec![];
        let mut stats_max_value_arr = vec![];
        let mut compression_arr = vec![];
        let mut encodings_arr = vec![];
        let mut index_page_offset_arr = vec![];
        let mut dictionary_page_offset_arr = vec![];
        let mut data_page_offset_arr = vec![];
        let mut total_compressed_size_arr = vec![];
        let mut total_uncompressed_size_arr = vec![];
        for (rg_idx, row_group) in metadata.row_groups().iter().enumerate() {
            for (col_idx, column) in row_group.columns().iter().enumerate() {
                filename_arr.push(filename.clone());
                row_group_id_arr.push(rg_idx as i64);
                row_group_num_rows_arr.push(row_group.num_rows());
                row_group_num_columns_arr.push(row_group.num_columns() as i64);
                row_group_bytes_arr.push(row_group.total_byte_size());
                column_id_arr.push(col_idx as i64);
                file_offset_arr.push(column.file_offset());
                num_values_arr.push(column.num_values());
                path_in_schema_arr.push(column.column_path().to_string());
                type_arr.push(column.column_type().to_string());
                let converted_type = column.column_descr().converted_type();

                if let Some(s) = column.statistics() {
                    let (min_val, max_val) =
                        convert_parquet_statistics(s, converted_type);
                    stats_min_arr.push(min_val.clone());
                    stats_max_arr.push(max_val.clone());
                    stats_null_count_arr.push(s.null_count_opt().map(|c| c as i64));
                    stats_distinct_count_arr
                        .push(s.distinct_count_opt().map(|c| c as i64));
                    stats_min_value_arr.push(min_val);
                    stats_max_value_arr.push(max_val);
                } else {
                    stats_min_arr.push(None);
                    stats_max_arr.push(None);
                    stats_null_count_arr.push(None);
                    stats_distinct_count_arr.push(None);
                    stats_min_value_arr.push(None);
                    stats_max_value_arr.push(None);
                };
                compression_arr.push(format!("{:?}", column.compression()));
                // need to collect into Vec to format
                let encodings: Vec<_> = column.encodings().collect();
                encodings_arr.push(format!("{:?}", encodings));
                index_page_offset_arr.push(column.index_page_offset());
                dictionary_page_offset_arr.push(column.dictionary_page_offset());
                data_page_offset_arr.push(column.data_page_offset());
                total_compressed_size_arr.push(column.compressed_size());
                total_uncompressed_size_arr.push(column.uncompressed_size());
            }
        }

        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(filename_arr)),
                Arc::new(Int64Array::from(row_group_id_arr)),
                Arc::new(Int64Array::from(row_group_num_rows_arr)),
                Arc::new(Int64Array::from(row_group_num_columns_arr)),
                Arc::new(Int64Array::from(row_group_bytes_arr)),
                Arc::new(Int64Array::from(column_id_arr)),
                Arc::new(Int64Array::from(file_offset_arr)),
                Arc::new(Int64Array::from(num_values_arr)),
                Arc::new(StringArray::from(path_in_schema_arr)),
                Arc::new(StringArray::from(type_arr)),
                Arc::new(StringArray::from(stats_min_arr)),
                Arc::new(StringArray::from(stats_max_arr)),
                Arc::new(Int64Array::from(stats_null_count_arr)),
                Arc::new(Int64Array::from(stats_distinct_count_arr)),
                Arc::new(StringArray::from(stats_min_value_arr)),
                Arc::new(StringArray::from(stats_max_value_arr)),
                Arc::new(StringArray::from(compression_arr)),
                Arc::new(StringArray::from(encodings_arr)),
                Arc::new(Int64Array::from(index_page_offset_arr)),
                Arc::new(Int64Array::from(dictionary_page_offset_arr)),
                Arc::new(Int64Array::from(data_page_offset_arr)),
                Arc::new(Int64Array::from(total_compressed_size_arr)),
                Arc::new(Int64Array::from(total_uncompressed_size_arr)),
            ],
        )?;

        let parquet_metadata = ParquetMetadataTable { schema, batch: rb };
        Ok(Arc::new(parquet_metadata))
    }
}

/// METADATA_CACHE table function
#[derive(Debug)]
struct MetadataCacheTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for MetadataCacheTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![self.batch.clone()]],
            TableProvider::schema(self),
            projection.cloned(),
        )?)
    }
}

#[derive(Debug)]
pub struct MetadataCacheFunc {
    cache_manager: Arc<CacheManager>,
}

impl MetadataCacheFunc {
    pub fn new(cache_manager: Arc<CacheManager>) -> Self {
        Self { cache_manager }
    }
}

impl TableFunctionImpl for MetadataCacheFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("metadata_cache should have no arguments");
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new(
                "file_modified",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("file_size_bytes", DataType::UInt64, false),
            Field::new("e_tag", DataType::Utf8, true),
            Field::new("version", DataType::Utf8, true),
            Field::new("metadata_size_bytes", DataType::UInt64, false),
            Field::new("hits", DataType::UInt64, false),
            Field::new("extra", DataType::Utf8, true),
        ]));

        // construct record batch from metadata
        let mut path_arr = vec![];
        let mut file_modified_arr = vec![];
        let mut file_size_bytes_arr = vec![];
        let mut e_tag_arr = vec![];
        let mut version_arr = vec![];
        let mut metadata_size_bytes = vec![];
        let mut hits_arr = vec![];
        let mut extra_arr = vec![];

        let cached_entries = self.cache_manager.get_file_metadata_cache().list_entries();

        for (path, entry) in cached_entries {
            path_arr.push(path.to_string());
            file_modified_arr
                .push(Some(entry.object_meta.last_modified.timestamp_millis()));
            file_size_bytes_arr.push(entry.object_meta.size);
            e_tag_arr.push(entry.object_meta.e_tag);
            version_arr.push(entry.object_meta.version);
            metadata_size_bytes.push(entry.size_bytes as u64);
            hits_arr.push(entry.hits as u64);

            let mut extra = entry
                .extra
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>();
            extra.sort();
            extra_arr.push(extra.join(" "));
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(path_arr)),
                Arc::new(TimestampMillisecondArray::from(file_modified_arr)),
                Arc::new(UInt64Array::from(file_size_bytes_arr)),
                Arc::new(StringArray::from(e_tag_arr)),
                Arc::new(StringArray::from(version_arr)),
                Arc::new(UInt64Array::from(metadata_size_bytes)),
                Arc::new(UInt64Array::from(hits_arr)),
                Arc::new(StringArray::from(extra_arr)),
            ],
        )?;

        let metadata_cache = MetadataCacheTable { schema, batch };
        Ok(Arc::new(metadata_cache))
    }
}

/// STATISTICS_CACHE table function
#[derive(Debug)]
struct StatisticsCacheTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for StatisticsCacheTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![self.batch.clone()]],
            TableProvider::schema(self),
            projection.cloned(),
        )?)
    }
}

#[derive(Debug)]
pub struct StatisticsCacheFunc {
    cache_manager: Arc<CacheManager>,
}

impl StatisticsCacheFunc {
    pub fn new(cache_manager: Arc<CacheManager>) -> Self {
        Self { cache_manager }
    }
}

impl TableFunctionImpl for StatisticsCacheFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("statistics_cache should have no arguments");
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new(
                "file_modified",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("file_size_bytes", DataType::UInt64, false),
            Field::new("e_tag", DataType::Utf8, true),
            Field::new("version", DataType::Utf8, true),
            Field::new("num_rows", DataType::Utf8, false),
            Field::new("num_columns", DataType::UInt64, false),
            Field::new("table_size_bytes", DataType::Utf8, false),
            Field::new("statistics_size_bytes", DataType::UInt64, false),
        ]));

        // construct record batch from metadata
        let mut path_arr = vec![];
        let mut file_modified_arr = vec![];
        let mut file_size_bytes_arr = vec![];
        let mut e_tag_arr = vec![];
        let mut version_arr = vec![];
        let mut num_rows_arr = vec![];
        let mut num_columns_arr = vec![];
        let mut table_size_bytes_arr = vec![];
        let mut statistics_size_bytes_arr = vec![];

        if let Some(file_statistics_cache) = self.cache_manager.get_file_statistic_cache()
        {
            for (path, entry) in file_statistics_cache.list_entries() {
                path_arr.push(path.to_string());
                file_modified_arr
                    .push(Some(entry.object_meta.last_modified.timestamp_millis()));
                file_size_bytes_arr.push(entry.object_meta.size);
                e_tag_arr.push(entry.object_meta.e_tag);
                version_arr.push(entry.object_meta.version);
                num_rows_arr.push(entry.num_rows.to_string());
                num_columns_arr.push(entry.num_columns as u64);
                table_size_bytes_arr.push(entry.table_size_bytes.to_string());
                statistics_size_bytes_arr.push(entry.statistics_size_bytes as u64);
            }
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(path_arr)),
                Arc::new(TimestampMillisecondArray::from(file_modified_arr)),
                Arc::new(UInt64Array::from(file_size_bytes_arr)),
                Arc::new(StringArray::from(e_tag_arr)),
                Arc::new(StringArray::from(version_arr)),
                Arc::new(StringArray::from(num_rows_arr)),
                Arc::new(UInt64Array::from(num_columns_arr)),
                Arc::new(StringArray::from(table_size_bytes_arr)),
                Arc::new(UInt64Array::from(statistics_size_bytes_arr)),
            ],
        )?;

        let statistics_cache = StatisticsCacheTable { schema, batch };
        Ok(Arc::new(statistics_cache))
    }
}

// Implementation of the `list_files_cache` table function in datafusion-cli.
///
/// This function returns the cached results of running a LIST command on a particular object store path for a table. The object metadata is returned as a List of Structs, with one Struct for each object.
/// DataFusion uses these cached results to plan queries against external tables.
/// # Schema
/// ```sql
/// > describe select * from list_files_cache();
/// +---------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+
/// | column_name         | data_type                                                                                                                                                                | is_nullable |
/// +---------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+
/// | table               | Utf8                                                                                                                                                                     | NO          |
/// | path                | Utf8                                                                                                                                                                     | NO          |
/// | metadata_size_bytes | UInt64                                                                                                                                                                   | NO          |
/// | expires_in          | Duration(ms)                                                                                                                                                             | YES         |
/// | metadata_list       | List(Struct("file_path": non-null Utf8, "file_modified": non-null Timestamp(ms), "file_size_bytes": non-null UInt64, "e_tag": Utf8, "version": Utf8), field: 'metadata') | YES         |
/// +---------------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------+-------------+
/// ```
#[derive(Debug)]
struct ListFilesCacheTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for ListFilesCacheTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(MemorySourceConfig::try_new_exec(
            &[vec![self.batch.clone()]],
            TableProvider::schema(self),
            projection.cloned(),
        )?)
    }
}

#[derive(Debug)]
pub struct ListFilesCacheFunc {
    cache_manager: Arc<CacheManager>,
}

impl ListFilesCacheFunc {
    pub fn new(cache_manager: Arc<CacheManager>) -> Self {
        Self { cache_manager }
    }
}

impl TableFunctionImpl for ListFilesCacheFunc {
    fn call(&self, exprs: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if !exprs.is_empty() {
            return plan_err!("list_files_cache should have no arguments");
        }

        let nested_fields = Fields::from(vec![
            Field::new("file_path", DataType::Utf8, false),
            Field::new(
                "file_modified",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("file_size_bytes", DataType::UInt64, false),
            Field::new("e_tag", DataType::Utf8, true),
            Field::new("version", DataType::Utf8, true),
        ]);

        let metadata_field =
            Field::new("metadata", DataType::Struct(nested_fields.clone()), true);

        let schema = Arc::new(Schema::new(vec![
            Field::new("table", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("metadata_size_bytes", DataType::UInt64, false),
            // expires field in ListFilesEntry has type Instant when set, from which we cannot get "the number of seconds", hence using Duration instead of Timestamp as data type.
            Field::new(
                "expires_in",
                DataType::Duration(TimeUnit::Millisecond),
                true,
            ),
            Field::new(
                "metadata_list",
                DataType::List(Arc::new(metadata_field.clone())),
                true,
            ),
        ]));

        let mut table_arr = vec![];
        let mut path_arr = vec![];
        let mut metadata_size_bytes_arr = vec![];
        let mut expires_arr = vec![];

        let mut file_path_arr = vec![];
        let mut file_modified_arr = vec![];
        let mut file_size_bytes_arr = vec![];
        let mut etag_arr = vec![];
        let mut version_arr = vec![];
        let mut offsets: Vec<i32> = vec![0];

        if let Some(list_files_cache) = self.cache_manager.get_list_files_cache() {
            let now = Instant::now();
            let mut current_offset: i32 = 0;

            for (path, entry) in list_files_cache.list_entries() {
                table_arr.push(path.table.map_or("NULL".to_string(), |t| t.to_string()));
                path_arr.push(path.path.to_string());
                metadata_size_bytes_arr.push(entry.size_bytes as u64);
                // calculates time left before entry expires
                expires_arr.push(
                    entry
                        .expires
                        .map(|t| t.duration_since(now).as_millis() as i64),
                );

                for meta in entry.metas.files.iter() {
                    file_path_arr.push(meta.location.to_string());
                    file_modified_arr.push(meta.last_modified.timestamp_millis());
                    file_size_bytes_arr.push(meta.size);
                    etag_arr.push(meta.e_tag.clone());
                    version_arr.push(meta.version.clone());
                }
                current_offset += entry.metas.files.len() as i32;
                offsets.push(current_offset);
            }
        }

        let struct_arr = StructArray::new(
            nested_fields,
            vec![
                Arc::new(StringArray::from(file_path_arr)),
                Arc::new(TimestampMillisecondArray::from(file_modified_arr)),
                Arc::new(UInt64Array::from(file_size_bytes_arr)),
                Arc::new(StringArray::from(etag_arr)),
                Arc::new(StringArray::from(version_arr)),
            ],
            None,
        );

        let offsets_buffer: OffsetBuffer<i32> =
            OffsetBuffer::new(ScalarBuffer::from(Buffer::from_vec(offsets)));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(table_arr)),
                Arc::new(StringArray::from(path_arr)),
                Arc::new(UInt64Array::from(metadata_size_bytes_arr)),
                Arc::new(DurationMillisecondArray::from(expires_arr)),
                Arc::new(GenericListArray::new(
                    Arc::new(metadata_field),
                    offsets_buffer,
                    Arc::new(struct_arr),
                    None,
                )),
            ],
        )?;

        let list_files_cache = ListFilesCacheTable { schema, batch };
        Ok(Arc::new(list_files_cache))
    }
}
