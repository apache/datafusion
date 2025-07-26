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

//! MATCH_RECOGNIZE specific window function implementations
//!
//! These functions are used specifically within MATCH_RECOGNIZE clauses
//! and provide specialized functionality for pattern matching operations.

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::arrow::array::ArrayRef;
use datafusion_common::{
    arrow_datafusion_err, exec_datafusion_err, exec_err, DataFusionError, Result,
    ScalarValue,
};
use datafusion_expr::window_doc_sections::DOC_SECTION_ANALYTICAL;

use crate::utils::{get_scalar_value_from_args, get_signed_integer};
use arrow::array::{Array, StringArray};
use arrow::array::{BooleanArray, UInt32Array};
use arrow::compute::take as arrow_take;
use datafusion_expr::col;
use datafusion_expr::window_state::WindowAggState;
use datafusion_expr::{
    Documentation, Literal, PartitionEvaluator, Signature, TypeSignature, Volatility,
    WindowUDFImpl,
};
use datafusion_functions_window_common::field::WindowUDFFieldArgs;
use datafusion_functions_window_common::partition::PartitionEvaluatorArgs;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::any::Any;
use std::fmt::Debug;
use std::ops::Range;
use std::sync::{Arc, LazyLock};

// Define all MATCH_RECOGNIZE functions using macros
get_or_init_udwf!(
    MatchRecognizeFirst,
    mr_first,
    "Returns the first value in a MATCH_RECOGNIZE pattern",
    MatchRecognizeFirst::new
);

get_or_init_udwf!(
    MatchRecognizeLast,
    mr_last,
    "Returns the last value in a MATCH_RECOGNIZE pattern",
    MatchRecognizeLast::new
);

get_or_init_udwf!(
    MatchRecognizePrev,
    mr_prev,
    "Returns the previous value in a MATCH_RECOGNIZE pattern",
    MatchRecognizePrev::new
);

get_or_init_udwf!(
    MatchRecognizeNext,
    mr_next,
    "Returns the next value in a MATCH_RECOGNIZE pattern",
    MatchRecognizeNext::new
);

get_or_init_udwf!(
    MatchNumber,
    match_number,
    "Returns the sequential number of the current match",
    MatchNumber::new
);

get_or_init_udwf!(
    MatchSequenceNumber,
    match_sequence_number,
    "Returns the row number within the current match",
    MatchSequenceNumber::new
);

get_or_init_udwf!(
    Classifier,
    classifier,
    "Returns the symbol that the current row matched in the pattern",
    Classifier::new
);

// Expression API functions for creating MATCH_RECOGNIZE function calls

fn symbols_literal_from_expr(expr: &datafusion_expr::Expr) -> datafusion_expr::Expr {
    let symbols = datafusion_expr::utils::find_symbol_predicates(expr);
    if symbols.is_empty() {
        return ScalarValue::Null.lit();
    }

    // Join all symbols with commas for compact transmission
    let joined = symbols.join(",");
    let symbols_scalar = ScalarValue::Utf8(Some(joined));

    symbols_scalar.lit()
}

fn symbol_from_args(
    args: &[Arc<dyn PhysicalExpr>],
    idx: usize,
    fn_name: &str,
) -> Result<Option<String>> {
    if let Some(ScalarValue::Utf8(Some(list))) = get_scalar_value_from_args(args, idx)? {
        let num_symbols = list.split(',').count();
        if num_symbols >= 2 {
            return exec_err!("{fn_name} function can reference at most one symbol, but found {num_symbols}: {list}");
        }
        Ok(Some(list.to_uppercase()))
    } else {
        Ok(None)
    }
}

pub fn mr_first(arg: datafusion_expr::Expr) -> datafusion_expr::Expr {
    let symbols_lit = symbols_literal_from_expr(&arg);
    mr_first_udwf().call(vec![arg, col("__mr_classifier"), symbols_lit])
}

pub fn mr_last(arg: datafusion_expr::Expr) -> datafusion_expr::Expr {
    let symbols_lit = symbols_literal_from_expr(&arg);
    mr_last_udwf().call(vec![arg, col("__mr_classifier"), symbols_lit])
}

pub fn mr_prev(
    arg: datafusion_expr::Expr,
    offset: Option<i64>,
    default: Option<ScalarValue>,
) -> datafusion_expr::Expr {
    let offset_lit = offset.map(|v| v.lit()).unwrap_or(ScalarValue::Null.lit());
    let default_lit = default.unwrap_or(ScalarValue::Null).lit();
    let symbols_lit = symbols_literal_from_expr(&arg);

    mr_prev_udwf().call(vec![
        arg,
        col("__mr_classifier"),
        symbols_lit,
        offset_lit,
        default_lit,
    ])
}

pub fn mr_next(
    arg: datafusion_expr::Expr,
    offset: Option<i64>,
    default: Option<ScalarValue>,
) -> datafusion_expr::Expr {
    let offset_lit = offset.map(|v| v.lit()).unwrap_or(ScalarValue::Null.lit());
    let default_lit = default.unwrap_or(ScalarValue::Null).lit();
    let symbols_lit = symbols_literal_from_expr(&arg);
    mr_next_udwf().call(vec![
        arg,
        col("__mr_classifier"),
        symbols_lit,
        offset_lit,
        default_lit,
    ])
}

pub fn match_number() -> datafusion_expr::Expr {
    match_number_udwf().call(vec![col("__mr_match_number")])
}

pub fn match_sequence_number() -> datafusion_expr::Expr {
    match_sequence_number_udwf().call(vec![col("__mr_match_sequence_number")])
}

pub fn classifier() -> datafusion_expr::Expr {
    classifier_udwf().call(vec![col("__mr_classifier")])
}

// Common trait for MATCH_RECOGNIZE specific function kinds
#[derive(Debug, Copy, Clone)]
pub enum MatchRecognizeFunctionKind {
    First,
    Last,
    Prev,
    Next,
    MatchNumber,
    MatchSequenceNumber,
    Classifier,
}

/// Shift evaluator kind for PREV / NEXT
#[derive(Copy, Clone, Debug)]
enum MRShiftKind {
    Prev,
    Next,
}

impl MRShiftKind {
    /// For fast-path LEAD/LAG shift_array offset sign
    fn offset_sign(&self) -> i64 {
        match self {
            MRShiftKind::Prev => 1,  // positive offset for LAG semantics
            MRShiftKind::Next => -1, // negative offset for LEAD semantics
        }
    }

    fn is_causal(&self) -> bool {
        matches!(self, MRShiftKind::Prev)
    }
}

// -----------------------------------------------------------------------------
// Edge evaluator for FIRST / LAST
// -----------------------------------------------------------------------------

/// Evaluator kind for FIRST / LAST
#[derive(Copy, Clone, Debug)]
enum MREdgeKind {
    First,
    Last,
}

#[derive(Debug)]
struct MatchRecognizeEdgeEvaluator {
    cache: SymbolIndexCache,
    kind: MREdgeKind,
    finalized_result: Option<ScalarValue>,
}

impl MatchRecognizeEdgeEvaluator {
    fn new(symbol_opt: Option<String>, kind: MREdgeKind) -> Self {
        Self {
            cache: SymbolIndexCache::new(symbol_opt),
            kind,
            finalized_result: None,
        }
    }
}

impl PartitionEvaluator for MatchRecognizeEdgeEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let data_arr = &values[0];

        // Fast path when no symbol predicate is present
        if !self.cache.has_symbol() {
            return match self.kind {
                MREdgeKind::First => {
                    let first_val = if data_arr.len() > 0 {
                        ScalarValue::try_from_array(data_arr.as_ref(), 0)?
                    } else {
                        create_default_value(data_arr.data_type())?
                    };
                    first_val.to_array_of_size(num_rows)
                }
                MREdgeKind::Last => Ok(Arc::clone(data_arr)),
            };
        }

        // Build indices if needed
        let classifier_arr = get_classifier_array(values)?;
        self.cache.ensure(classifier_arr);

        // Construct the row index mapping once and use Arrow's `take` kernel for fast gathering
        let gather_indices: Vec<Option<usize>> = match self.kind {
            MREdgeKind::First => {
                let first_seen_idx = self.cache.first_seen();
                (0..num_rows)
                    .map(|idx| {
                        if let Some(first_idx) = first_seen_idx {
                            if idx >= first_idx {
                                Some(first_idx)
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect()
            }
            MREdgeKind::Last => (0..num_rows)
                .map(|idx| self.cache.nearest_before(idx))
                .collect(),
        };

        gather_array_by_indices(data_arr, gather_indices)
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue> {
        let data_arr = &values[0];
        let idx = range.start;

        // Fast path when no symbol predicate is present
        if !self.cache.has_symbol() {
            return match self.kind {
                MREdgeKind::First => {
                    if data_arr.len() > 0 {
                        ScalarValue::try_from_array(data_arr.as_ref(), 0)
                    } else {
                        create_default_value(data_arr.data_type())
                    }
                }
                MREdgeKind::Last => ScalarValue::try_from_array(data_arr.as_ref(), idx),
            };
        }

        // Build indices if needed
        let classifier_arr = get_classifier_array(values)?;
        self.cache.ensure(classifier_arr);

        match self.kind {
            MREdgeKind::First => match self.cache.first_seen() {
                Some(first_idx) if idx >= first_idx => {
                    ScalarValue::try_from_array(data_arr.as_ref(), first_idx)
                }
                _ => create_default_value(data_arr.data_type()),
            },
            MREdgeKind::Last => {
                if let Some(r) = self.cache.nearest_before(idx) {
                    ScalarValue::try_from_array(data_arr.as_ref(), r)
                } else {
                    create_default_value(data_arr.data_type())
                }
            }
        }
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    fn is_causal(&self) -> bool {
        true
    }

    fn memoize(&mut self, state: &mut WindowAggState) -> Result<()> {
        let size = state.out_col.len();
        if size == 0 {
            return Ok(());
        }

        // Number of rows we need to keep after pruning.
        let buffer_size = 1;

        // Helper to move window start backwards by at most `buffer_size`.
        let prune = |state: &mut WindowAggState| {
            state.window_frame_range.start =
                state.window_frame_range.end.saturating_sub(buffer_size);
        };

        match self.kind {
            MREdgeKind::First => {
                // Only finalise when the first value is non-NULL (symbol seen).
                if self.finalized_result.is_none() {
                    let result =
                        ScalarValue::try_from_array(state.out_col.as_ref(), size - 1)?;
                    if !result.is_null() {
                        self.finalized_result = Some(result);
                    }
                }

                if self.finalized_result.is_some() {
                    // We can safely prune everything before the current end minus buffer_size.
                    prune(state);
                    // Invalidate any cached indices so they are rebuilt relative to new buffer.
                    self.cache.indices = None;
                    self.cache.first_seen = None;
                }
            }
            MREdgeKind::Last => {
                // For LAST we always only need to keep the last row (buffer_size = 1)
                prune(state);
                self.cache.indices = None;
                self.cache.first_seen = None;
            }
        }
        Ok(())
    }
}

// FIRST function implementation
#[derive(Debug)]
pub struct MatchRecognizeFirst {
    signature: Signature,
}

impl MatchRecognizeFirst {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Any(1),
                    TypeSignature::Any(2),
                    TypeSignature::Any(3),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

static MR_FIRST_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_ANALYTICAL,
        "Returns the first value in a MATCH_RECOGNIZE pattern match. \
        This function can only be used within MATCH_RECOGNIZE clauses.",
        "first(expression)",
    )
    .with_argument("expression", "Expression to operate on")
    .build()
});

impl WindowUDFImpl for MatchRecognizeFirst {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "first"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let symbol_opt =
            symbol_from_args(partition_evaluator_args.input_exprs(), 2, "FIRST")?;
        Ok(Box::new(MatchRecognizeEdgeEvaluator::new(
            symbol_opt,
            MREdgeKind::First,
        )))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        let return_type = field_args
            .input_fields()
            .first()
            .map(|f| f.data_type())
            .cloned()
            .unwrap_or(DataType::Null);
        Ok(Field::new(field_args.name(), return_type, true).into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&MR_FIRST_DOCUMENTATION)
    }
}

// LAST function implementation
#[derive(Debug)]
pub struct MatchRecognizeLast {
    signature: Signature,
}

impl MatchRecognizeLast {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Any(1),
                    TypeSignature::Any(2),
                    TypeSignature::Any(3),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

static MR_LAST_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_ANALYTICAL,
        "Returns the last value in a MATCH_RECOGNIZE pattern match. \
        This function can only be used within MATCH_RECOGNIZE clauses.",
        "last(expression)",
    )
    .with_argument("expression", "Expression to operate on")
    .build()
});

impl WindowUDFImpl for MatchRecognizeLast {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "last"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let symbol_opt =
            symbol_from_args(partition_evaluator_args.input_exprs(), 2, "LAST")?;
        Ok(Box::new(MatchRecognizeEdgeEvaluator::new(
            symbol_opt,
            MREdgeKind::Last,
        )))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        let return_type = field_args
            .input_fields()
            .first()
            .map(|f| f.data_type())
            .cloned()
            .unwrap_or(DataType::Null);
        Ok(Field::new(field_args.name(), return_type, true).into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&MR_LAST_DOCUMENTATION)
    }
}

// PREV function implementation
#[derive(Debug)]
pub struct MatchRecognizePrev {
    signature: Signature,
}

impl MatchRecognizePrev {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Any(1),
                    TypeSignature::Any(2),
                    TypeSignature::Any(3),
                    TypeSignature::Any(4),
                    TypeSignature::Any(5),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

static MR_PREV_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_ANALYTICAL,
        "Returns a value from a previous row in a MATCH_RECOGNIZE pattern match. \
        This function can only be used within MATCH_RECOGNIZE clauses.",
        "prev(expression [, offset [, default]])",
    )
    .with_argument("expression", "Expression to operate on")
    .with_argument(
        "offset",
        "Integer. Number of rows back to look. Defaults to 1.",
    )
    .with_argument("default", "Default value if offset is out of bounds")
    .build()
});

impl WindowUDFImpl for MatchRecognizePrev {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "prev"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let symbol_opt =
            symbol_from_args(partition_evaluator_args.input_exprs(), 2, "PREV")?;
        let offset_val =
            get_scalar_value_from_args(partition_evaluator_args.input_exprs(), 3)?
                .map(get_signed_integer)
                .map_or(Ok(1i64), |res| res)?;
        if offset_val < 0 {
            return exec_err!("Offset for PREV must be non-negative");
        }
        let offset = offset_val as usize;

        let default_value =
            get_scalar_value_from_args(partition_evaluator_args.input_exprs(), 4)?
                .unwrap_or(ScalarValue::Null);

        Ok(Box::new(MatchRecognizeShiftEvaluator::new(
            symbol_opt,
            offset,
            default_value,
            MRShiftKind::Prev,
        )))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        let return_type = field_args
            .input_fields()
            .first()
            .map(|f| f.data_type())
            .cloned()
            .unwrap_or(DataType::Null);
        Ok(Field::new(field_args.name(), return_type, true).into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&MR_PREV_DOCUMENTATION)
    }
}

// NEXT function implementation
#[derive(Debug)]
pub struct MatchRecognizeNext {
    signature: Signature,
}

impl MatchRecognizeNext {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Any(1),
                    TypeSignature::Any(2),
                    TypeSignature::Any(3),
                    TypeSignature::Any(4),
                    TypeSignature::Any(5),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

static MR_NEXT_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_ANALYTICAL,
        "Returns a value from a following row in a MATCH_RECOGNIZE pattern match. \
        This function can only be used within MATCH_RECOGNIZE clauses.",
        "next(expression [, offset [, default]])",
    )
    .with_argument("expression", "Expression to operate on")
    .with_argument(
        "offset",
        "Integer. Number of rows ahead to look. Defaults to 1.",
    )
    .with_argument("default", "Default value if offset is out of bounds")
    .build()
});

impl WindowUDFImpl for MatchRecognizeNext {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "next"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let symbol_opt =
            symbol_from_args(partition_evaluator_args.input_exprs(), 2, "NEXT")?;
        let offset_val =
            get_scalar_value_from_args(partition_evaluator_args.input_exprs(), 3)?
                .map(get_signed_integer)
                .map_or(Ok(1i64), |res| res)?;
        if offset_val < 0 {
            return exec_err!("Offset for NEXT must be non-negative");
        }
        let offset = offset_val as usize;
        let default_value =
            get_scalar_value_from_args(partition_evaluator_args.input_exprs(), 4)?
                .unwrap_or(ScalarValue::Null);
        Ok(Box::new(MatchRecognizeShiftEvaluator::new(
            symbol_opt,
            offset,
            default_value,
            MRShiftKind::Next,
        )))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        let return_type = field_args
            .input_fields()
            .first()
            .map(|f| f.data_type())
            .cloned()
            .unwrap_or(DataType::Null);
        Ok(Field::new(field_args.name(), return_type, true).into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&MR_NEXT_DOCUMENTATION)
    }
}

// MATCH_NUMBER function implementation
#[derive(Debug)]
pub struct MatchNumber {
    signature: Signature,
}

impl MatchNumber {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::UInt64],
                Volatility::Immutable,
            ),
        }
    }
}

static MATCH_NUMBER_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_ANALYTICAL,
        "Returns the sequential number of the current match in a MATCH_RECOGNIZE clause. \
        The match number starts from 1 and increments for each match found.",
        "match_number()",
    )
    .build()
});

impl WindowUDFImpl for MatchNumber {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "match_number"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(PassThroughEvaluator::new(
            "MATCH_NUMBER",
            "__mr_match_number",
        )))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Field::new(field_args.name(), DataType::UInt64, false).into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&MATCH_NUMBER_DOCUMENTATION)
    }
}

// MATCH_SEQUENCE_NUMBER function implementation
#[derive(Debug)]
pub struct MatchSequenceNumber {
    signature: Signature,
}

impl MatchSequenceNumber {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![DataType::UInt64],
                Volatility::Immutable,
            ),
        }
    }
}

static MATCH_SEQUENCE_NUMBER_DOCUMENTATION: LazyLock<Documentation> =
    LazyLock::new(|| {
        Documentation::builder(
        DOC_SECTION_ANALYTICAL,
        "Returns the row number within the current match in a MATCH_RECOGNIZE clause. \
        The sequence number starts from 1 for each match.",
        "match_sequence_number()",
    )
    .build()
    });

impl WindowUDFImpl for MatchSequenceNumber {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "match_sequence_number"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(PassThroughEvaluator::new(
            "MATCH_SEQUENCE_NUMBER",
            "__mr_match_sequence_number",
        )))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Field::new(field_args.name(), DataType::UInt64, false).into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&MATCH_SEQUENCE_NUMBER_DOCUMENTATION)
    }
}

// CLASSIFIER function implementation
#[derive(Debug)]
pub struct Classifier {
    signature: Signature,
}

impl Classifier {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![DataType::Utf8], Volatility::Immutable),
        }
    }
}

static CLASSIFIER_DOCUMENTATION: LazyLock<Documentation> = LazyLock::new(|| {
    Documentation::builder(
        DOC_SECTION_ANALYTICAL,
        "Returns the symbol name that the current row matched in a MATCH_RECOGNIZE pattern. \
        Returns the symbol name as a string.",
        "classifier()",
    )
    .build()
});

impl WindowUDFImpl for Classifier {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "classifier"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn partition_evaluator(
        &self,
        _partition_evaluator_args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(PassThroughEvaluator::new(
            "CLASSIFIER",
            "__mr_classifier",
        )))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> Result<FieldRef> {
        Ok(Field::new(field_args.name(), DataType::Utf8, true).into())
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(&CLASSIFIER_DOCUMENTATION)
    }
}

/// Pre-compute, in a single pass, for every row `i` the nearest row ≤ `i`
/// whose classifier matches `symbol`. Returns the resulting vector **and**
/// the index of the first matching row (if any).
fn build_nearest_before(
    symbol: &str,
    classifier_arr: &StringArray,
) -> (Vec<Option<usize>>, Option<usize>) {
    let mut nearest = Vec::with_capacity(classifier_arr.len());
    let mut last_match: Option<usize> = None;
    let mut first_seen: Option<usize> = None;

    for i in 0..classifier_arr.len() {
        if classifier_arr.is_valid(i) && classifier_arr.value(i) == symbol {
            last_match = Some(i);
            if first_seen.is_none() {
                first_seen = last_match;
            }
        }
        nearest.push(last_match);
    }
    (nearest, first_seen)
}

/// Simple cache that stores the nearest-before vector for a single symbol
#[derive(Debug)]
struct SymbolIndexCache {
    symbol: Option<String>,
    indices: Option<Vec<Option<usize>>>,
    first_seen: Option<usize>,
}

impl SymbolIndexCache {
    fn new(symbol: Option<String>) -> Self {
        Self {
            symbol,
            indices: None,
            first_seen: None,
        }
    }

    #[inline]
    fn has_symbol(&self) -> bool {
        self.symbol.is_some()
    }

    fn ensure(&mut self, classifier_arr: &StringArray) {
        if self.indices.is_none() {
            if let Some(ref sym) = self.symbol {
                let (vec, first) = build_nearest_before(sym, classifier_arr);
                self.indices = Some(vec);
                self.first_seen = first;
            }
        }
    }

    #[inline]
    fn nearest_before(&self, idx: usize) -> Option<usize> {
        self.indices.as_ref().and_then(|v| v[idx])
    }

    #[inline]
    fn first_seen(&self) -> Option<usize> {
        self.first_seen
    }
}

#[derive(Debug)]
struct MatchRecognizeShiftEvaluator {
    cache: SymbolIndexCache,
    offset: usize,
    default_value: ScalarValue,
    kind: MRShiftKind,
}

impl MatchRecognizeShiftEvaluator {
    fn new(
        symbol_opt: Option<String>,
        offset: usize,
        default_value: ScalarValue,
        kind: MRShiftKind,
    ) -> Self {
        Self {
            cache: SymbolIndexCache::new(symbol_opt),
            offset,
            default_value,
            kind,
        }
    }
}

impl PartitionEvaluator for MatchRecognizeShiftEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let data_arr = &values[0];
        let default_val = if self.default_value.is_null() {
            create_default_value(data_arr.data_type())?
        } else {
            self.default_value.clone()
        };

        // Fast path when no symbol predicate – behaves like LAG/LEAD
        if !self.cache.has_symbol() {
            let offset_signed = self.kind.offset_sign() * self.offset as i64;
            return shift_array_with_default(data_arr, offset_signed, &default_val);
        }

        // Build indices if needed
        let classifier_arr = get_classifier_array(values)?;
        self.cache.ensure(classifier_arr);

        let mut gather_indices: Vec<Option<usize>> = Vec::with_capacity(num_rows);
        let mut default_mask: Vec<bool> = Vec::with_capacity(num_rows);

        for idx in 0..num_rows {
            match self.cache.nearest_before(idx) {
                Some(r) => {
                    let target = match self.kind {
                        MRShiftKind::Prev => {
                            if r >= self.offset {
                                Some(r - self.offset)
                            } else {
                                None
                            }
                        }
                        MRShiftKind::Next => {
                            let t = r + self.offset;
                            if t < num_rows {
                                Some(t)
                            } else {
                                None
                            }
                        }
                    };
                    match target {
                        Some(t_idx) => {
                            gather_indices.push(Some(t_idx));
                            default_mask.push(false);
                        }
                        None => {
                            // symbol matched but offset out-of-bounds -> use default
                            gather_indices.push(None);
                            default_mask.push(true);
                        }
                    }
                }
                None => {
                    // No symbol -> NULL (no default)
                    gather_indices.push(None);
                    default_mask.push(false);
                }
            }
        }

        gather_with_default(data_arr, gather_indices, default_mask, &default_val)
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue> {
        let data_arr = &values[0];
        let idx = range.start;
        let default_val = if self.default_value.is_null() {
            create_default_value(data_arr.data_type())?
        } else {
            self.default_value.clone()
        };

        if !self.cache.has_symbol() {
            let target = match self.kind {
                MRShiftKind::Prev => idx.checked_sub(self.offset),
                MRShiftKind::Next => idx.checked_add(self.offset),
            };
            return if let Some(t) = target.filter(|&t| t < data_arr.len()) {
                ScalarValue::try_from_array(data_arr.as_ref(), t)
            } else {
                Ok(default_val)
            };
        }

        let classifier_arr = get_classifier_array(values)?;
        self.cache.ensure(classifier_arr);
        if let Some(r) = self.cache.nearest_before(idx) {
            let target = match self.kind {
                MRShiftKind::Prev => {
                    if r >= self.offset {
                        Some(r - self.offset)
                    } else {
                        None
                    }
                }
                MRShiftKind::Next => {
                    let t = r + self.offset;
                    if t < data_arr.len() {
                        Some(t)
                    } else {
                        None
                    }
                }
            };
            if let Some(t_idx) = target {
                ScalarValue::try_from_array(data_arr.as_ref(), t_idx)
            } else {
                Ok(default_val)
            }
        } else {
            create_default_value(data_arr.data_type())
        }
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    fn is_causal(&self) -> bool {
        self.kind.is_causal()
    }
}

// Generic pass-through evaluator for simple virtual column functions
#[derive(Debug)]
struct PassThroughEvaluator {
    function_name: &'static str,
    column_name: &'static str,
}

impl PassThroughEvaluator {
    fn new(function_name: &'static str, column_name: &'static str) -> Self {
        Self {
            function_name,
            column_name,
        }
    }
}

impl PartitionEvaluator for PassThroughEvaluator {
    fn evaluate_all(&mut self, values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        if values.is_empty() {
            return exec_err!(
                "{} evaluator expects one input column (virtual column {}) but got 0",
                self.function_name,
                self.column_name
            );
        }

        let arr = &values[0];
        if arr.len() != num_rows {
            return exec_err!(
                "Input column length {} does not match expected num_rows {}",
                arr.len(),
                num_rows
            );
        }

        Ok(Arc::clone(arr))
    }

    fn evaluate(
        &mut self,
        values: &[ArrayRef],
        range: &Range<usize>,
    ) -> Result<ScalarValue> {
        if values.is_empty() {
            return exec_err!(
                "{} evaluator expects one input column but got 0",
                self.function_name
            );
        }

        let arr = &values[0];
        let idx = range.start;
        if idx >= arr.len() {
            return exec_err!(
                "Row index {} out of bounds for input column of length {}",
                idx,
                arr.len()
            );
        }

        ScalarValue::try_from_array(arr.as_ref(), idx)
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }
}

/// Extract the classifier array from the input values
fn get_classifier_array(values: &[ArrayRef]) -> Result<&StringArray> {
    values[1]
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| exec_datafusion_err!("classifier column must be a StringArray"))
}

/// Create a default scalar value for the given data type
fn create_default_value(data_type: &DataType) -> Result<ScalarValue> {
    ScalarValue::try_from(data_type)
}

/// Shift an Array by `offset` positions filling vacated slots with `default_value`.
/// Positive `offset` behaves like LAG (values move towards higher indices).
/// Negative `offset` behaves like LEAD (values move towards lower indices).
fn shift_array_with_default(
    array: &ArrayRef,
    offset: i64,
    default_value: &ScalarValue,
) -> Result<ArrayRef> {
    use datafusion_common::arrow::compute::concat;

    let value_len = array.len() as i64;
    if offset == 0 {
        return Ok(Arc::clone(array));
    }
    if offset == i64::MIN || offset.abs() >= value_len {
        return default_value.to_array_of_size(value_len as usize);
    }

    // Determine slice of the original data to keep
    let slice_offset = (-offset).clamp(0, value_len) as usize;
    let length = array.len() - offset.unsigned_abs() as usize;
    let slice = array.slice(slice_offset, length);

    // Generate default fill array for the shifted portion
    let fill_len = offset.unsigned_abs() as usize;
    let default_fill = default_value.to_array_of_size(fill_len)?;

    // Concatenate in the proper order
    if offset > 0 {
        concat(&[default_fill.as_ref(), slice.as_ref()])
            .map_err(|e| arrow_datafusion_err!(e))
    } else {
        concat(&[slice.as_ref(), default_fill.as_ref()])
            .map_err(|e| arrow_datafusion_err!(e))
    }
}

// -----------------------------------------------------------------------------
// New helpers to efficiently gather rows using Arrow's `take` kernel
// -----------------------------------------------------------------------------

/// Gather values from `data_arr` at the provided `indices` (row indices). An entry of
/// `None` in `indices` results in a NULL at that position in the output.
fn gather_array_by_indices(
    data_arr: &ArrayRef,
    indices: Vec<Option<usize>>,
) -> Result<ArrayRef> {
    // Convert to `Option<u32>` expected by `UInt32Array::from`.
    let idx_u32: Vec<Option<u32>> = indices
        .into_iter()
        .map(|opt| opt.map(|v| v as u32))
        .collect();

    let idx_array = UInt32Array::from(idx_u32);
    arrow_take(data_arr.as_ref(), &idx_array, None).map_err(|e| arrow_datafusion_err!(e))
}

/// Similar to [`gather_array_by_indices`] but fills NULL positions with `default_value`
/// (if it is **not** NULL). When `default_value` **is** NULL this is equivalent to
/// `gather_array_by_indices`.
fn gather_with_default(
    data_arr: &ArrayRef,
    indices: Vec<Option<usize>>,
    default_mask: Vec<bool>,
    default_value: &ScalarValue,
) -> Result<ArrayRef> {
    let gathered = gather_array_by_indices(data_arr, indices)?;

    if default_value.is_null() {
        return Ok(gathered);
    }

    // Early-exit if mask has no `true`
    if !default_mask.iter().any(|b| *b) {
        return Ok(gathered);
    }

    let default_arr = default_value.to_array_of_size(gathered.len())?;

    // convert mask vec<bool> into BooleanArray
    let mask_array = BooleanArray::from(default_mask);

    arrow::compute::kernels::zip::zip(&mask_array, &default_arr, &gathered)
        .map_err(|e| arrow_datafusion_err!(e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, UInt64Array};
    use arrow::datatypes::DataType;
    use datafusion_expr::window_state::WindowAggState;

    /// Helper to create a StringArray from a slice of &str
    fn string_array(data: &[&str]) -> StringArray {
        StringArray::from(data.to_vec())
    }

    #[test]
    fn test_first_no_symbol() {
        // data column
        let data = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        // classifier column – symbols per row
        let classifier = Arc::new(string_array(&["A", "B", "A", "B", "A"])) as ArrayRef;

        let mut evaluator = MatchRecognizeEdgeEvaluator::new(None, MREdgeKind::First);
        let result = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 5)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![10, 10, 10, 10, 10]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_last_with_symbol() {
        let data = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        let classifier = Arc::new(string_array(&["A", "B", "A", "B", "A"])) as ArrayRef;

        let mut evaluator =
            MatchRecognizeEdgeEvaluator::new(Some("A".to_string()), MREdgeKind::Last);
        let result = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 5)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![10, 10, 30, 30, 50]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_prev_no_symbol() {
        let data = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        let classifier = Arc::new(string_array(&["A", "B", "A", "B", "A"])) as ArrayRef;

        let mut evaluator = MatchRecognizeShiftEvaluator::new(
            None,
            1,
            ScalarValue::Null,
            MRShiftKind::Prev,
        );
        let result = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 5)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected =
            Int32Array::from(vec![None, Some(10), Some(20), Some(30), Some(40)]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_next_with_default() {
        let data = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        let classifier = Arc::new(string_array(&["A", "B", "A", "B", "A"])) as ArrayRef;

        let default_val = ScalarValue::Int32(Some(999));
        let mut evaluator = MatchRecognizeShiftEvaluator::new(
            None,
            2,
            default_val.clone(),
            MRShiftKind::Next,
        );
        let result = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 5)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected =
            Int32Array::from(vec![Some(30), Some(40), Some(50), Some(999), Some(999)]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_match_number_pass_through() {
        let input = Arc::new(UInt64Array::from(vec![1u64, 1, 2, 2, 3])) as ArrayRef;
        let mut evaluator =
            PassThroughEvaluator::new("MATCH_NUMBER", "__mr_match_number");
        let out = evaluator.evaluate_all(&[input.clone()], 5).unwrap();
        assert_eq!(out.as_ref(), input.as_ref());

        // single row evaluation
        let scalar = evaluator.evaluate(&[input.clone()], &(2..3)).unwrap();
        assert_eq!(scalar, ScalarValue::UInt64(Some(2)));
    }

    #[test]
    fn test_classifier_pass_through() {
        let classifier = Arc::new(string_array(&["A", "B", "A", "B", "A"])) as ArrayRef;
        let mut evaluator = PassThroughEvaluator::new("CLASSIFIER", "__mr_classifier");

        let out = evaluator.evaluate_all(&[classifier.clone()], 5).unwrap();
        assert_eq!(out.as_ref(), classifier.as_ref());

        let scalar = evaluator.evaluate(&[classifier.clone()], &(3..4)).unwrap();
        assert_eq!(scalar, ScalarValue::Utf8(Some("B".to_string())));
    }

    #[test]
    fn test_first_with_symbol_missing() {
        let data = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        let classifier = Arc::new(string_array(&["A", "B", "A", "B", "A"])) as ArrayRef;

        // Use symbol "B" so row 0 has no matching symbol in its history
        let mut evaluator =
            MatchRecognizeEdgeEvaluator::new(Some("B".to_string()), MREdgeKind::First);
        let result = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 5)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected =
            Int32Array::from(vec![None, Some(20), Some(20), Some(20), Some(20)]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_last_no_symbol() {
        let data = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        let classifier = Arc::new(string_array(&["A", "B", "A", "B", "A"])) as ArrayRef;

        let mut evaluator = MatchRecognizeEdgeEvaluator::new(None, MREdgeKind::Last);
        let result = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 5)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        // With no symbol filter, LAST should just return the data value itself
        let expected = Int32Array::from(vec![10, 20, 30, 40, 50]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_prev_with_symbol_and_default() {
        let data = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        let classifier = Arc::new(string_array(&["A", "B", "A", "B", "A"])) as ArrayRef;

        let mut evaluator = MatchRecognizeShiftEvaluator::new(
            Some("B".to_string()),
            1,
            ScalarValue::Int32(Some(999)),
            MRShiftKind::Prev,
        );
        let result = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 5)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected =
            Int32Array::from(vec![None, Some(10), Some(10), Some(30), Some(30)]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_next_with_symbol_and_default() {
        let data = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        let classifier = Arc::new(string_array(&["A", "B", "A", "B", "A"])) as ArrayRef;

        let mut evaluator = MatchRecognizeShiftEvaluator::new(
            Some("B".to_string()),
            1,
            ScalarValue::Int32(Some(999)),
            MRShiftKind::Next,
        );
        let result = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 5)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected =
            Int32Array::from(vec![None, Some(30), Some(30), Some(50), Some(50)]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_match_sequence_number_pass_through() {
        let input = Arc::new(UInt64Array::from(vec![1u64, 1, 2, 2, 3])) as ArrayRef;
        let mut evaluator = PassThroughEvaluator::new(
            "MATCH_SEQUENCE_NUMBER",
            "__mr_match_sequence_number",
        );
        let out = evaluator.evaluate_all(&[input.clone()], 5).unwrap();
        assert_eq!(out.as_ref(), input.as_ref());

        let scalar = evaluator.evaluate(&[input.clone()], &(4..5)).unwrap();
        assert_eq!(scalar, ScalarValue::UInt64(Some(3)));
    }

    #[test]
    fn test_prev_offset_zero() {
        let data = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        let classifier = Arc::new(string_array(&["A", "B", "A", "B", "A"])) as ArrayRef;

        let mut evaluator = MatchRecognizeShiftEvaluator::new(
            None,
            0,
            ScalarValue::Null,
            MRShiftKind::Prev,
        );
        let result = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 5)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![10, 20, 30, 40, 50]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_next_offset_zero() {
        let data = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        let classifier = Arc::new(string_array(&["A", "B", "A", "B", "A"])) as ArrayRef;

        let mut evaluator = MatchRecognizeShiftEvaluator::new(
            None,
            0,
            ScalarValue::Null,
            MRShiftKind::Next,
        );
        let result = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 5)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![10, 20, 30, 40, 50]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_prev_with_symbol_offset_two() {
        let data = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        let classifier = Arc::new(string_array(&["A", "B", "A", "B", "A"])) as ArrayRef;

        let mut evaluator = MatchRecognizeShiftEvaluator::new(
            Some("A".to_string()),
            2,
            ScalarValue::Null,
            MRShiftKind::Prev,
        );
        let result = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 5)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![None, None, Some(10), Some(10), Some(30)]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_next_with_symbol_offset_two() {
        let data = Arc::new(Int32Array::from(vec![10, 20, 30, 40, 50])) as ArrayRef;
        let classifier = Arc::new(string_array(&["A", "B", "A", "B", "A"])) as ArrayRef;

        let default_val = ScalarValue::Int32(Some(999));
        let mut evaluator = MatchRecognizeShiftEvaluator::new(
            Some("B".to_string()),
            2,
            default_val.clone(),
            MRShiftKind::Next,
        );
        let result = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 5)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected =
            Int32Array::from(vec![None, Some(40), Some(40), Some(999), Some(999)]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_first_symbol_not_found() {
        let data = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let classifier = Arc::new(string_array(&["A", "A", "A"])) as ArrayRef;

        let mut evaluator =
            MatchRecognizeEdgeEvaluator::new(Some("B".to_string()), MREdgeKind::First);
        let result = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 3)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![None, None, None]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_prev_symbol_not_found_default() {
        let data = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let classifier = Arc::new(string_array(&["A", "A", "A"])) as ArrayRef;

        let default_val = ScalarValue::Int32(Some(888));
        let mut evaluator = MatchRecognizeShiftEvaluator::new(
            Some("B".to_string()),
            1,
            default_val.clone(),
            MRShiftKind::Prev,
        );
        let result = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 3)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![None, None, None]);
        assert_eq!(result, &expected);
    }

    #[test]
    fn test_next_symbol_not_found_default() {
        let data = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
        let classifier = Arc::new(string_array(&["A", "A", "A"])) as ArrayRef;

        let default_val = ScalarValue::Int32(Some(888));
        let mut evaluator = MatchRecognizeShiftEvaluator::new(
            Some("B".to_string()),
            1,
            default_val.clone(),
            MRShiftKind::Next,
        );
        let result = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 3)
            .unwrap();
        let result = result.as_any().downcast_ref::<Int32Array>().unwrap();

        let expected = Int32Array::from(vec![None, None, None]);
        assert_eq!(result, &expected);
    }

    // ---------------------- New memoize tests ----------------------

    #[test]
    fn test_memoize_first_prunes() {
        let data = Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef;
        let classifier = Arc::new(string_array(&["X", "X", "X"])) as ArrayRef;

        let mut evaluator = MatchRecognizeEdgeEvaluator::new(None, MREdgeKind::First);
        let out = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 3)
            .unwrap();

        let mut win_state = WindowAggState::new(&DataType::Int32).unwrap();
        win_state.window_frame_range = 0..3;
        win_state.out_col = out;

        evaluator.memoize(&mut win_state).unwrap();

        assert_eq!(
            win_state.window_frame_range.end - win_state.window_frame_range.start,
            1
        );
    }

    #[test]
    fn test_memoize_first_no_prune_when_unresolved() {
        let data = Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef;
        // symbol filter "B" which never appears
        let classifier = Arc::new(string_array(&["A", "A", "A"])) as ArrayRef;

        let mut evaluator =
            MatchRecognizeEdgeEvaluator::new(Some("B".to_string()), MREdgeKind::First);
        let out = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 3)
            .unwrap();

        let mut win_state = WindowAggState::new(&DataType::Int32).unwrap();
        win_state.window_frame_range = 0..3;
        win_state.out_col = out;

        evaluator.memoize(&mut win_state).unwrap();

        assert_eq!(win_state.window_frame_range.start, 0);
    }

    #[test]
    fn test_memoize_last_prunes() {
        let data = Arc::new(Int32Array::from(vec![10, 20, 30, 40])) as ArrayRef;
        let classifier = Arc::new(string_array(&["A", "A", "A", "A"])) as ArrayRef;

        let mut evaluator = MatchRecognizeEdgeEvaluator::new(None, MREdgeKind::Last);
        let out = evaluator
            .evaluate_all(&[data.clone(), classifier.clone()], 4)
            .unwrap();

        let mut win_state = WindowAggState::new(&DataType::Int32).unwrap();
        win_state.window_frame_range = 0..4;
        win_state.out_col = out;

        evaluator.memoize(&mut win_state).unwrap();

        assert_eq!(
            win_state.window_frame_range.end - win_state.window_frame_range.start,
            1
        );
    }
}
