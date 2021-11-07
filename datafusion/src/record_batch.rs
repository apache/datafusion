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

//! RecordBatch API for wrapping around arrow RecordBatch in order
//! to provide richer functionality with more flexibility

use crate::arrow::error::Result as ArrowResult;
use crate::arrow::record_batch::RecordBatch as ArrowRecordBatch;
use crate::error::Result;
use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct RecordBatch {
  arrow_record_batch: ArrowRecordBatch,
}

impl RecordBatch {
  pub fn try_new(schema: SchemaRef, columns: Vec<ArrayRef>) -> ArrowResult<Self> {
    unimplemented!()
  }

  pub fn new_empty(schema: SchemaRef) -> Self {
    unimplemented!()
  }

  pub fn schema(&self) -> SchemaRef {
    self.arrow_record_batch.schema()
  }

  pub fn num_columns(&self) -> usize {
    unimplemented!()
  }

  pub fn num_rows(&self) -> usize {
    unimplemented!()
  }

  pub fn column(&self, index: usize) -> &ArrayRef {
    unimplemented!()
  }

  pub fn columns(&self) -> &[ArrayRef] {
    unimplemented!()
  }
}

impl From<&ArrowRecordBatch> for RecordBatch {
  fn from(batch: &ArrowRecordBatch) -> Self {
    unimplemented!()
  }
}

impl Into<ArrowRecordBatch> for RecordBatch {
  fn into(self) -> ArrowRecordBatch {
    unimplemented!()
  }
}
