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

use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use arrow::datatypes::{Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion_common::{DFSchema, DFSchemaRef, TableReference};

use crate::match_recognize::columns::MrMetadataColumn;

/// Projection/pruning specification for MATCH_RECOGNIZE output columns.
///
/// Controls which input columns are passed through (and in what order), which
/// MR metadata columns are materialized, and which classifier bitset columns
/// are appended. This structure is constructed as "full output" by default
/// and can be pruned by the optimizer.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MatchRecognizeOutputSpec {
    pub passthrough_input_indices: Vec<usize>,
    pub metadata_columns: Vec<MrMetadataColumn>,
    pub classifier_bitset_symbols: Vec<String>,
}

impl MatchRecognizeOutputSpec {
    /// Construct a spec that passes through all input columns, selects all
    /// MR metadata columns, and appends classifier bitsets for all declared symbols.
    pub fn full_from(input_schema: &DFSchemaRef, symbols: &[String]) -> Self {
        let passthrough_input_indices = (0..input_schema.fields().len()).collect();
        let metadata_columns = MrMetadataColumn::all().to_vec();
        let classifier_bitset_symbols = symbols.to_vec();
        Self {
            passthrough_input_indices,
            metadata_columns,
            classifier_bitset_symbols,
        }
    }

    /// Construct a spec from explicit components (used by optimizer).
    pub fn new(
        passthrough_input_indices: Vec<usize>,
        metadata_columns: Vec<MrMetadataColumn>,
        classifier_bitset_symbols: Vec<String>,
    ) -> Self {
        Self {
            passthrough_input_indices,
            metadata_columns,
            classifier_bitset_symbols,
        }
    }

    /// Build logical (DF) schema: passthrough input columns + MR metadata + classifier bitsets.
    pub fn build_df_schema(
        &self,
        input: &DFSchema,
    ) -> datafusion_common::Result<DFSchemaRef> {
        // Validate passthrough indices against input
        let input_len = input.fields().len();
        if self
            .passthrough_input_indices
            .iter()
            .any(|&i| i >= input_len)
        {
            return datafusion_common::plan_err!(
                "passthrough_input_indices contains out-of-bounds index"
            );
        }

        let mut fields: Vec<(Option<TableReference>, Arc<Field>)> = self
            .passthrough_input_indices
            .iter()
            .map(|&i| {
                let (qualifier, field) = input.qualified_field(i);
                (qualifier.cloned(), Arc::new(field.clone()))
            })
            .collect();

        fields.extend(
            crate::match_recognize::columns::mr_metadata_df_fields_subset_enum(
                &self.metadata_columns,
            ),
        );

        fields.extend(crate::match_recognize::columns::classifier_bits_df_fields(
            &self.classifier_bitset_symbols,
        ));

        Ok(Arc::new(DFSchema::new_with_metadata(
            fields,
            input.metadata().clone(),
        )?))
    }

    /// Build physical (Arrow) schema: passthrough input columns + MR metadata + classifier bitsets.
    pub fn build_arrow_schema(&self, input: &ArrowSchema) -> ArrowSchemaRef {
        let mut fields: Vec<Arc<Field>> = self
            .passthrough_input_indices
            .iter()
            .map(|&i| Arc::clone(&input.fields()[i]))
            .collect();

        fields.extend(
            crate::match_recognize::columns::mr_metadata_arrow_fields_subset_enum(
                &self.metadata_columns,
            ),
        );

        for sym in &self.classifier_bitset_symbols {
            let name = crate::match_recognize::columns::classifier_bits_col_name(sym);
            fields.push(Arc::new(Field::new(
                &name,
                arrow::datatypes::DataType::Boolean,
                false,
            )));
        }

        Arc::new(ArrowSchema::new_with_metadata(
            fields,
            input.metadata().clone(),
        ))
    }

    /// Returns true if there is nothing to display/materialize in the spec.
    pub fn is_empty(&self) -> bool {
        self.metadata_columns.is_empty()
            && self.classifier_bitset_symbols.is_empty()
            && self.passthrough_input_indices.is_empty()
    }

    pub fn display_with_df_schema<'a>(
        &'a self,
        schema: &'a DFSchemaRef,
    ) -> OutputSpecWithDFSchema<'a> {
        OutputSpecWithDFSchema::new(self, schema)
    }

    pub fn display_with_arrow_schema<'a>(
        &'a self,
        schema: &'a ArrowSchemaRef,
    ) -> OutputSpecWithArrowSchema<'a> {
        OutputSpecWithArrowSchema::new(self, schema)
    }
}

/// Helper wrapper to format `MatchRecognizeOutputSpec` with a DF schema
pub struct OutputSpecWithDFSchema<'a> {
    spec: &'a MatchRecognizeOutputSpec,
    schema: &'a DFSchemaRef,
}

impl<'a> OutputSpecWithDFSchema<'a> {
    pub fn new(spec: &'a MatchRecognizeOutputSpec, schema: &'a DFSchemaRef) -> Self {
        Self { spec, schema }
    }
}

// Small shared formatter to avoid duplicating meta/bits/passthrough rendering
fn fmt_output_spec_with<F>(
    spec: &MatchRecognizeOutputSpec,
    f: &mut Formatter<'_>,
    mut name_of: F,
) -> fmt::Result
where
    F: FnMut(usize) -> String,
{
    let mut parts: Vec<String> = Vec::new();

    if !spec.passthrough_input_indices.is_empty() {
        let passthrough = spec
            .passthrough_input_indices
            .iter()
            .map(|&i| name_of(i))
            .collect::<Vec<_>>()
            .join(",");
        parts.push(format!("passthrough_columns=[{passthrough}]"));
    }

    if !spec.metadata_columns.is_empty() {
        let meta = spec
            .metadata_columns
            .iter()
            .map(|c| c.as_ref())
            .collect::<Vec<_>>()
            .join(",");
        parts.push(format!("metadata=[{meta}]"));
    }

    if !spec.classifier_bitset_symbols.is_empty() {
        let bits = spec
            .classifier_bitset_symbols
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>()
            .join(",");
        parts.push(format!("classifier_flags=[{bits}]"));
    }

    if parts.is_empty() {
        return Ok(());
    }

    write!(f, "output={{")?;
    write!(f, "{}", parts.join(", "))?;
    write!(f, "}}")
}

impl<'a> Display for OutputSpecWithDFSchema<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fmt_output_spec_with(self.spec, f, |i| {
            let (q, field) = self.schema.qualified_field(i);
            match q {
                Some(q) => format!("{}.{}", q, field.name()),
                None => field.name().to_string(),
            }
        })
    }
}

/// Helper wrapper to format `MatchRecognizeOutputSpec` with an Arrow schema
pub struct OutputSpecWithArrowSchema<'a> {
    spec: &'a MatchRecognizeOutputSpec,
    schema: &'a ArrowSchemaRef,
}

impl<'a> OutputSpecWithArrowSchema<'a> {
    pub fn new(spec: &'a MatchRecognizeOutputSpec, schema: &'a ArrowSchemaRef) -> Self {
        Self { spec, schema }
    }
}

impl<'a> Display for OutputSpecWithArrowSchema<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fmt_output_spec_with(self.spec, f, |i| self.schema.fields()[i].name().to_string())
    }
}
