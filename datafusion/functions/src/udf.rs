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

use arrow::{
    array::{Array, ArrayRef, RecordBatch},
    datatypes::{Field, Schema, SchemaRef},
};
use arrow_udf::{function, sig::REGISTRY};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_expr::ColumnarValue;
// use arrow_string::predicate::Predicate;

#[function("eq(boolean, boolean) -> boolean")]
#[function("eq(int8, int8) -> boolean")]
#[function("eq(int16, int16) -> boolean")]
#[function("eq(int32, int32) -> boolean")]
#[function("eq(int64, int64) -> boolean")]
#[function("eq(uint8, uint8) -> boolean")]
#[function("eq(uint16, uint16) -> boolean")]
#[function("eq(uint32, uint32) -> boolean")]
#[function("eq(uint64, uint64) -> boolean")]
#[function("eq(string, string) -> boolean")]
#[function("eq(binary, binary) -> boolean")]
#[function("eq(largestring, largestring) -> boolean")]
#[function("eq(largebinary, largebinary) -> boolean")]
#[function("eq(date32, date32) -> boolean")]
// #[function("eq(struct Dictionary, struct Dictionary) -> boolean")]
fn eq<T: Eq>(lhs: T, rhs: T) -> bool {
    lhs == rhs
}

// Bad, we could not use the non-public API
// fn like(lhs: &str, rhs: &str) -> bool {
//     Predicate::like(rhs).unwrap().matches(lhs);
// }

#[function("concat(string, string) -> string")]
#[function("concat(largestring, largestring) -> largestring")]
fn concat(lhs: &str, rhs: &str) -> String {
    format!("{}{}", lhs, rhs)
}

pub fn apply_udf(
    lhs: &ColumnarValue,
    rhs: &ColumnarValue,
    return_field: &Field,
    udf_name: &str,
) -> Result<ArrayRef> {
    let (record_batch, schema) = match (lhs, rhs) {
        (ColumnarValue::Array(left), ColumnarValue::Array(right)) => {
            let schema = Arc::new(Schema::new(vec![
                Field::new("", left.data_type().clone(), left.is_nullable()),
                Field::new("", right.data_type().clone(), right.is_nullable()),
            ]));
            let record_batch =
                RecordBatch::try_new(schema.clone(), vec![left.clone(), right.clone()])?;
            Ok::<(RecordBatch, SchemaRef), DataFusionError>((record_batch, schema))
        }
        (ColumnarValue::Scalar(left), ColumnarValue::Array(right)) => {
            let schema = Arc::new(Schema::new(vec![
                Field::new("", left.data_type().clone(), false),
                Field::new("", right.data_type().clone(), right.is_nullable()),
            ]));
            let record_batch = RecordBatch::try_new(
                schema.clone(),
                vec![left.to_array_of_size(right.len())?, right.clone()],
            )?;
            Ok((record_batch, schema))
        }
        (ColumnarValue::Array(left), ColumnarValue::Scalar(right)) => {
            let schema = Arc::new(Schema::new(vec![
                Field::new("", left.data_type().clone(), left.is_nullable()),
                Field::new("", right.data_type().clone(), false),
            ]));
            let record_batch = RecordBatch::try_new(
                schema.clone(),
                vec![left.clone(), right.to_array_of_size(left.len())?],
            )?;
            Ok((record_batch, schema))
        }
        (ColumnarValue::Scalar(left), ColumnarValue::Scalar(right)) => {
            let schema = Arc::new(Schema::new(vec![
                Field::new("", left.data_type().clone(), false),
                Field::new("", right.data_type().clone(), false),
            ]));
            let record_batch = RecordBatch::try_new(
                schema.clone(),
                vec![left.to_array()?, right.to_array()?],
            )?;
            Ok((record_batch, schema))
        }
    }?;

    apply_udf_inner(schema, &record_batch, return_field, udf_name)
}

fn apply_udf_inner(
    schema: SchemaRef,
    record_batch: &RecordBatch,
    return_field: &Field,
    udf_name: &str,
) -> Result<ArrayRef> {
    println!("schema: {:?}", schema);

    let Some(eval) = REGISTRY
        .get(
            udf_name,
            schema
                .all_fields()
                .into_iter()
                .map(|f| f.to_owned())
                .collect::<Vec<_>>()
                .as_slice(),
            return_field,
        )
        .and_then(|f| f.function.as_scalar())
    else {
        return internal_err!("UDF {} not found for schema {}", udf_name, schema);
    };

    let result = eval(record_batch)?;

    let result_array = result.column_by_name(udf_name).unwrap();

    Ok(result_array.to_owned())
}
