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

use arrow::datatypes::{DataType, Field, Fields, Schema};

/// Verifies whether the original planned schema can be satisfied with data
/// adhering to the candidate schema. In practice, this is equality check on the
/// schemas except that original schema can have nullable fields where candidate
/// is constrained to not provide null data.
pub(crate) fn schema_satisfied_by(original: &Schema, candidate: &Schema) -> bool {
    original.metadata() == candidate.metadata()
        && fields_satisfied_by(original.fields(), candidate.fields())
}

/// See [`schema_satisfied_by`] for the contract.
fn fields_satisfied_by(original: &Fields, candidate: &Fields) -> bool {
    original.len() == candidate.len()
        && original
            .iter()
            .zip(candidate)
            .all(|(original, candidate)| field_satisfied_by(original, candidate))
}

/// See [`schema_satisfied_by`] for the contract.
fn field_satisfied_by(original: &Field, candidate: &Field) -> bool {
    original.name() == candidate.name()
        && (original.is_nullable() || !candidate.is_nullable())
        && original.metadata() == candidate.metadata()
        && data_type_satisfied_by(original.data_type(), candidate.data_type())
}

/// See [`schema_satisfied_by`] for the contract.
fn data_type_satisfied_by(original: &DataType, candidate: &DataType) -> bool {
    match (original, candidate) {
        (DataType::List(original_field), DataType::List(candidate_field)) => {
            field_satisfied_by(original_field, candidate_field)
        }

        (DataType::ListView(original_field), DataType::ListView(candidate_field)) => {
            field_satisfied_by(original_field, candidate_field)
        }

        (
            DataType::FixedSizeList(original_field, original_size),
            DataType::FixedSizeList(candidate_field, candidate_size),
        ) => {
            original_size == candidate_size
                && field_satisfied_by(original_field, candidate_field)
        }

        (DataType::LargeList(original_field), DataType::LargeList(candidate_field)) => {
            field_satisfied_by(original_field, candidate_field)
        }

        (
            DataType::LargeListView(original_field),
            DataType::LargeListView(candidate_field),
        ) => field_satisfied_by(original_field, candidate_field),

        (DataType::Struct(original_fields), DataType::Struct(candidate_fields)) => {
            fields_satisfied_by(original_fields, candidate_fields)
        }

        // TODO (DataType::Union(, _), DataType::Union(_, _)) => {}
        // TODO (DataType::Dictionary(_, _), DataType::Dictionary(_, _)) => {}
        // TODO (DataType::Map(_, _), DataType::Map(_, _)) => {}
        // TODO (DataType::RunEndEncoded(_, _), DataType::RunEndEncoded(_, _)) => {}
        _ => original == candidate,
    }
}
