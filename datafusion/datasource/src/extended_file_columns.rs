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

use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::metadata_columns::INPUT_FILE_NAME_COL;
use futures::{FutureExt, StreamExt};

use crate::PartitionedFile;
use crate::file_stream::{FileOpenFuture, FileOpener};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtendedFileColumn {
    InputFileName,
}

pub fn extended_file_column_fields(columns: &[ExtendedFileColumn]) -> Vec<Field> {
    columns
        .iter()
        .map(|column| match column {
            ExtendedFileColumn::InputFileName => {
                Field::new(INPUT_FILE_NAME_COL, DataType::Utf8, false)
            }
        })
        .collect()
}

pub fn append_extended_file_columns(
    schema: &SchemaRef,
    columns: &[ExtendedFileColumn],
) -> SchemaRef {
    if columns.is_empty() {
        return Arc::clone(schema);
    }

    let mut fields: Vec<Field> = schema
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    fields.extend(extended_file_column_fields(columns));

    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

pub(crate) struct ExtendedFileColumnsOpener {
    inner: Arc<dyn FileOpener>,
    columns: Vec<ExtendedFileColumn>,
    schema: SchemaRef,
}

impl ExtendedFileColumnsOpener {
    pub fn wrap(
        inner: Arc<dyn FileOpener>,
        columns: Vec<ExtendedFileColumn>,
        schema: SchemaRef,
    ) -> Arc<dyn FileOpener> {
        Arc::new(Self {
            inner,
            columns,
            schema,
        })
    }
}

impl FileOpener for ExtendedFileColumnsOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let file_name = partitioned_file.object_meta.location.to_string();
        let columns = self.columns.clone();
        let schema = Arc::clone(&self.schema);
        let inner = self.inner.open(partitioned_file)?;

        Ok(async move {
            let stream = inner.await?;
            let stream = stream.map(move |batch| {
                let batch = batch?;
                let num_rows = batch.num_rows();
                let mut arrays: Vec<ArrayRef> = batch.columns().to_vec();

                for column in &columns {
                    let array: ArrayRef = match column {
                        ExtendedFileColumn::InputFileName => {
                            Arc::new(StringArray::from(vec![file_name.clone(); num_rows]))
                        }
                    };
                    arrays.push(array);
                }

                Ok(RecordBatch::try_new(Arc::clone(&schema), arrays)?)
            });

            Ok(stream.boxed())
        }
        .boxed())
    }
}
