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

//! Declaration of built-in (scalar) functions.
//! This module contains built-in functions' enumeration and metadata.
//!
//! Generally, a function has:
//! * a signature
//! * a return type, that is a function of the incoming argument's types
//! * the computation, that must accept each valid signature
//!
//! * Signature: see `Signature`
//! * Return type: a function `(arg_types) -> return_type`. E.g. for sqrt, ([f32]) -> f32, ([f64]) -> f64.
//!
//! This module also has a set of coercion rules to improve user experience: if an argument i32 is passed
//! to a function that supports f64, it is coerced to f64.

use std::fmt::{self, Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock, RwLock};

use crate::PhysicalExpr;
use crate::expressions::Literal;

use arrow::array::{AnyDictionaryArray, Array, ArrayRef, AsArray, RecordBatch};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion_common::config::{ConfigEntry, ConfigOptions};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::ExprProperties;
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_expr::{
    ColumnarValue, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Volatility, expr_vec_fmt,
};

mod dictionary;
use dictionary::{
    Lookup, Memo, PeeledFields, Recollection, ValuesIdentity, compact_dictionary,
    scalar_arguments, unwrap_scalar_dictionaries, with_key_nulls,
};

/// Physical expression of a scalar function
pub struct ScalarFunctionExpr {
    fun: Arc<ScalarUDF>,
    name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
    return_field: FieldRef,
    config_options: Arc<ConfigOptions>,
    /// Fields for the peeled call, built once and reused across batches.
    peeled: OnceLock<PeeledFields>,
    /// Results computed for a dictionary's values: a batch carries its own
    /// keys but its column chunk's whole dictionary, so the same values recur.
    memoized: RwLock<Memo>,
}

impl Debug for ScalarFunctionExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ScalarFunctionExpr")
            .field("fun", &"<FUNC>")
            .field("name", &self.name)
            .field("args", &self.args)
            .field("return_field", &self.return_field)
            .finish()
    }
}

impl ScalarFunctionExpr {
    /// Create a new Scalar function
    pub fn new(
        name: &str,
        fun: Arc<ScalarUDF>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        return_field: FieldRef,
        config_options: Arc<ConfigOptions>,
    ) -> Self {
        Self {
            fun,
            name: name.to_owned(),
            args,
            return_field,
            config_options,
            peeled: OnceLock::new(),
            memoized: RwLock::new(Memo::default()),
        }
    }

    /// Create a new Scalar function
    pub fn try_new(
        fun: Arc<ScalarUDF>,
        args: Vec<Arc<dyn PhysicalExpr>>,
        schema: &Schema,
        config_options: Arc<ConfigOptions>,
    ) -> Result<Self> {
        let name = fun.name().to_string();
        let arg_fields = args
            .iter()
            .map(|e| e.return_field(schema))
            .collect::<Result<Vec<_>>>()?;

        // verify that input data types is consistent with function's `TypeSignature`
        fields_with_udf(&arg_fields, fun.as_ref())?;

        let arguments = args
            .iter()
            .map(|e| e.downcast_ref::<Literal>().map(|literal| literal.value()))
            .collect::<Vec<_>>();
        let ret_args = ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &arguments,
        };
        let return_field = fun.return_field_from_args(ret_args)?;
        Ok(Self {
            fun,
            name,
            args,
            return_field,
            config_options,
            peeled: OnceLock::new(),
            memoized: RwLock::new(Memo::default()),
        })
    }

    /// Get the scalar function implementation
    pub fn fun(&self) -> &ScalarUDF {
        &self.fun
    }

    /// The name for this expression
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Input arguments
    pub fn args(&self) -> &[Arc<dyn PhysicalExpr>] {
        &self.args
    }

    /// Data type produced by this expression
    pub fn return_type(&self) -> &DataType {
        self.return_field.data_type()
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.return_field = self
            .return_field
            .as_ref()
            .clone()
            .with_nullable(nullable)
            .into();
        // Derived from the field just replaced.
        self.peeled = OnceLock::new();
        self
    }

    pub fn nullable(&self) -> bool {
        self.return_field.is_nullable()
    }

    pub fn config_options(&self) -> &ConfigOptions {
        &self.config_options
    }

    /// Given an arbitrary PhysicalExpr attempt to downcast it to a ScalarFunctionExpr
    /// and verify that its inner function is of type T.
    /// If the downcast fails, or the function is not of type T, returns `None`.
    /// Otherwise returns `Some(ScalarFunctionExpr)`.
    pub fn try_downcast_func<T>(expr: &dyn PhysicalExpr) -> Option<&ScalarFunctionExpr>
    where
        T: ScalarUDFImpl,
    {
        match expr.downcast_ref::<ScalarFunctionExpr>() {
            Some(scalar_expr) if scalar_expr.fun().inner().is::<T>() => Some(scalar_expr),
            _ => None,
        }
    }

    /// Evaluates an elementwise function over the distinct values of a
    /// dictionary-encoded argument and re-maps the result through its keys, so
    /// the function runs once per distinct value instead of once per row.
    ///
    /// Returns `None` when peeling does not apply and the caller must evaluate
    /// the function over the batch as-is.
    fn try_invoke_peeled(
        &self,
        args: &[ColumnarValue],
        arg_fields: &[FieldRef],
        num_rows: usize,
    ) -> Result<Option<ColumnarValue>> {
        if num_rows == 0
            || !self.fun.evaluates_elementwise()
            || self.fun.signature().volatility == Volatility::Volatile
        {
            return Ok(None);
        }

        let returns_dictionary =
            matches!(self.return_field.data_type(), DataType::Dictionary(_, _));

        // Exactly one dictionary-encoded array; peeling several at once is only
        // sound when their keys line up. Dictionary-encoded scalars carry a
        // single value and are unwrapped rather than peeled.
        let mut dictionary_index = None;
        let mut scalar_dictionaries = false;
        for (index, arg) in args.iter().enumerate() {
            match arg {
                ColumnarValue::Array(array)
                    if matches!(array.data_type(), DataType::Dictionary(_, _)) =>
                {
                    if dictionary_index.replace(index).is_some() {
                        return Ok(None);
                    }
                }
                ColumnarValue::Scalar(ScalarValue::Dictionary(_, _)) => {
                    scalar_dictionaries = true
                }
                ColumnarValue::Scalar(_) => {}
                _ => return Ok(None),
            }
        }
        let Some(dictionary_index) = dictionary_index else {
            if !scalar_dictionaries || returns_dictionary {
                return Ok(None);
            }
            let (args, arg_fields) = unwrap_scalar_dictionaries(args, arg_fields);
            return self
                .fun
                .invoke_with_args(ScalarFunctionArgs {
                    args,
                    arg_fields,
                    number_rows: num_rows,
                    return_field: Arc::clone(&self.return_field),
                    config_options: Arc::clone(&self.config_options),
                })
                .map(Some);
        };
        let ColumnarValue::Array(array) = &args[dictionary_index] else {
            return internal_err!("dictionary argument is not an array");
        };
        // An extension type is bound to its storage type, so fields carrying
        // metadata keep the dictionary rather than have their type rewritten.
        if !arg_fields[dictionary_index].metadata().is_empty()
            || !self.return_field.metadata().is_empty()
        {
            return Ok(None);
        }
        let raw = array.as_any_dictionary();
        // A dictionary return keyed differently cannot be rebuilt from these keys.
        if let DataType::Dictionary(key_type, _) = self.return_field.data_type()
            && key_type.as_ref() != raw.keys().data_type()
        {
            return Ok(None);
        }
        let fields = self.peeled_fields(dictionary_index, arg_fields, raw.values());

        // Both tiers below re-use the keys as they are, which only a strict
        // `f` (NULL in -> NULL out) can answer for when some are null.
        let keys_are_answerable = raw.keys().null_count() == 0 || self.fun.is_strict();

        // Memoized tier: already evaluated for this expression — checked
        // before profitability, since work already done is worth re-using
        // however large the dictionary is.
        if keys_are_answerable {
            let scalars = scalar_arguments(args, fields.index);
            let recollection = self.memoized(raw.values(), &scalars);
            if let Recollection::Evaluated(output) = recollection {
                return self.remap(raw, output, &fields).map(Some);
            }

            // Fast tier: invoke over the values as-is — the cost profile of
            // the hand-rolled dictionary arms. Values no key references are
            // worth evaluating once a second sighting has made a repeat
            // likely; an error is not returned, because it may come from such
            // a value, and the tiers below settle that.
            let values_len = raw.values().len();
            let repeats = matches!(recollection, Recollection::SeenBefore);
            let profitable = if repeats {
                true
            } else if returns_dictionary {
                values_len <= num_rows
            } else {
                values_len * 2 <= num_rows
            };
            if profitable
                && let Ok(output) = self.invoke_on_array(
                    args,
                    arg_fields,
                    &fields,
                    Arc::clone(raw.values()),
                    &fields.output,
                )
            {
                // Kept only for a dictionary that has proved it comes back;
                // otherwise the entry would never be read.
                if repeats {
                    self.memoize(raw.values(), &scalars, &output);
                }
                return self.remap(raw, output, &fields).map(Some);
            }
        }

        // Guarded tier: `f` sees exactly the values this batch references,
        // null keys redirected to one appended NULL slot it evaluates like
        // any other value — correct even where `f(NULL)` is not NULL. A
        // dictionary return re-wraps in O(1); only flat returns carry a
        // row budget.
        let row_budget = (!returns_dictionary).then_some(num_rows);
        if let Some(dictionary) = compact_dictionary(array, row_budget)? {
            let peeled =
                self.invoke_on_values(args, arg_fields, &fields, &dictionary, raw);
            // A dictionary return cannot be rebuilt from a flat call, so its
            // errors surface here; a flat one retries over the whole column.
            if returns_dictionary || peeled.is_ok() {
                return peeled.map(Some);
            }
        }

        // A dictionary return cannot be rebuilt from a call over expanded rows,
        // so the column is left as it arrived and the function sees the
        // dictionary — exactly as it does wherever peeling declines.
        if returns_dictionary {
            return Ok(None);
        }

        // Peeling does not pay off for this batch, but `f` declared that it
        // evaluates elementwise, so it gets the expanded array rather than the
        // dictionary: exactly the input type coercion produces for functions
        // that do not preserve the encoding.
        let flattened = take(raw.values().as_ref(), raw.keys(), None)?;
        self.invoke_on_array(args, arg_fields, &fields, flattened, &self.return_field)
            .map(ColumnarValue::Array)
            .map(Some)
    }

    /// The fields for a peeled call, built once and reused across batches: they
    /// depend on the plan, not on the data. Callers have already established
    /// that neither field carries metadata.
    fn peeled_fields(
        &self,
        index: usize,
        arg_fields: &[FieldRef],
        values: &ArrayRef,
    ) -> PeeledFields {
        let source = &arg_fields[index];
        if let Some(cached) = self.peeled.get()
            && cached.index == index
            && cached.argument.data_type() == values.data_type()
            && cached.source.as_ref() == source.as_ref()
        {
            return cached.clone();
        }
        // Nullable: a peeled call can be handed a NULL value slot even where
        // the planned output was known not to be null.
        let output_type = match self.return_field.data_type() {
            DataType::Dictionary(_, value_type) => value_type.as_ref().clone(),
            flat => flat.clone(),
        };
        let built = PeeledFields {
            index,
            source: Arc::clone(source),
            argument: Arc::new(Field::new(
                source.name(),
                values.data_type().clone(),
                true,
            )),
            output: Arc::new(Field::new(self.return_field.name(), output_type, true)),
        };
        let _ = self.peeled.set(built.clone());
        built
    }

    /// Invokes the function over the values of `compacted` — `source` narrowed
    /// to what the batch references — and re-maps the result to batch length
    /// through the keys (`with_values` when the planned type is a dictionary,
    /// `take` otherwise).
    fn invoke_on_values(
        &self,
        args: &[ColumnarValue],
        arg_fields: &[FieldRef],
        fields: &PeeledFields,
        compacted: &ArrayRef,
        source: &dyn AnyDictionaryArray,
    ) -> Result<ColumnarValue> {
        let dictionary = compacted.as_any_dictionary();
        let output = self.invoke_on_array(
            args,
            arg_fields,
            fields,
            Arc::clone(dictionary.values()),
            &fields.output,
        )?;
        // Null keys were redirected to the appended NULL slot, the last value.
        // A dictionary result re-uses these keys, so where `f(NULL)` is NULL
        // the source's null keys go back: consumers read a dictionary's nulls
        // off its keys, and a valid key to a NULL value is invisible to them.
        if matches!(self.return_field.data_type(), DataType::Dictionary(_, _))
            && source.keys().null_count() > 0
        {
            let null_slot = dictionary.values().len() - 1;
            if output.is_null(null_slot) {
                let restored = with_key_nulls(compacted, source.keys().nulls())?;
                return self.remap(restored.as_any_dictionary(), output, fields);
            }
        }
        self.remap(dictionary, output, fields)
    }

    /// Spreads a per-value result back over the rows the keys address.
    fn remap(
        &self,
        dictionary: &dyn AnyDictionaryArray,
        output: ArrayRef,
        fields: &PeeledFields,
    ) -> Result<ColumnarValue> {
        // Checked before the re-map, which is not defined for every type a
        // misbehaving UDF could return.
        if output.data_type() != fields.output.data_type() {
            return internal_err!(
                "UDF {} returned type {} under dictionary peeling, expected {}",
                self.name,
                output.data_type(),
                fields.output.data_type()
            );
        }
        let remapped =
            if matches!(self.return_field.data_type(), DataType::Dictionary(_, _)) {
                dictionary.with_values(output)
            } else {
                take(output.as_ref(), dictionary.keys(), None)?
            };
        Ok(ColumnarValue::Array(remapped))
    }

    /// What this expression already knows about `values`: its result if it
    /// has one, or whether it has seen these values before. Hits take only
    /// the read lock, so partitions sharing this expression do not serialize
    /// on each other's warm batches; between the two locks another partition
    /// may record the same dictionary, which costs at most one extra full
    /// evaluation — what an unlucky first sighting costs anyway.
    fn memoized(&self, values: &ArrayRef, scalars: &[ScalarValue]) -> Recollection {
        let identity = {
            let Ok(memo) = self.memoized.read() else {
                return Recollection::Unknown;
            };
            match memo.find(values, scalars) {
                Lookup::Evaluated(output) => return Recollection::Evaluated(output),
                Lookup::Absent(identity) => identity,
            }
        };
        let hash = match identity {
            Some(identity) => identity.hash(),
            None => ValuesIdentity::hash_of(values),
        };
        let Ok(mut memo) = self.memoized.write() else {
            return Recollection::Unknown;
        };
        memo.note(hash)
    }

    /// Keeps `output` for the next batch that arrives with the same values.
    fn memoize(&self, values: &ArrayRef, scalars: &[ScalarValue], output: &ArrayRef) {
        if let Ok(mut memo) = self.memoized.write() {
            memo.keep(values, scalars, output);
        }
    }

    /// Invokes the function with `array` in place of the dictionary argument,
    /// whose field is rewritten to match. Returns `array.len()` rows.
    fn invoke_on_array(
        &self,
        args: &[ColumnarValue],
        arg_fields: &[FieldRef],
        fields: &PeeledFields,
        array: ArrayRef,
        return_field: &FieldRef,
    ) -> Result<ArrayRef> {
        let rows = array.len();
        let (mut peeled_args, mut peeled_fields) =
            unwrap_scalar_dictionaries(args, arg_fields);
        peeled_args[fields.index] = ColumnarValue::Array(array);
        peeled_fields[fields.index] = Arc::clone(&fields.argument);
        let output = self.fun.invoke_with_args(ScalarFunctionArgs {
            args: peeled_args,
            arg_fields: peeled_fields,
            number_rows: rows,
            return_field: Arc::clone(return_field),
            config_options: Arc::clone(&self.config_options),
        })?;

        let output = output.into_array(rows)?;
        if output.len() != rows {
            return internal_err!(
                "UDF {} returned {} rows for {} input rows",
                self.name,
                output.len(),
                rows
            );
        }
        Ok(output)
    }
}

impl fmt::Display for ScalarFunctionExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}({})", self.name, expr_vec_fmt!(self.args))
    }
}

impl PartialEq for ScalarFunctionExpr {
    fn eq(&self, o: &Self) -> bool {
        if std::ptr::eq(self, o) {
            // The equality implementation is somewhat expensive, so let's short-circuit when possible.
            return true;
        }
        let Self {
            fun,
            name,
            args,
            return_field,
            config_options,
            peeled: _, // derived from the fields above
            memoized: _,
        } = self;
        fun.eq(&o.fun)
            && name.eq(&o.name)
            && args.eq(&o.args)
            && return_field.eq(&o.return_field)
            && (Arc::ptr_eq(config_options, &o.config_options)
                || sorted_config_entries(config_options)
                    == sorted_config_entries(&o.config_options))
    }
}
impl Eq for ScalarFunctionExpr {}
impl Hash for ScalarFunctionExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let Self {
            fun,
            name,
            args,
            return_field,
            config_options: _, // expensive to hash, and often equal
            peeled: _,
            memoized: _,
        } = self;
        fun.hash(state);
        name.hash(state);
        args.hash(state);
        return_field.hash(state);
    }
}

fn sorted_config_entries(config_options: &ConfigOptions) -> Vec<ConfigEntry> {
    let mut entries = config_options.entries();
    entries.sort_by(|l, r| l.key.cmp(&r.key));
    entries
}

impl PhysicalExpr for ScalarFunctionExpr {
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.return_field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.return_field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let args = self
            .args
            .iter()
            .map(|e| e.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;

        let arg_fields = self
            .args
            .iter()
            .map(|e| e.return_field(batch.schema_ref()))
            .collect::<Result<Vec<_>>>()?;

        let input_empty = args.is_empty();
        let input_all_scalar = args
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));

        // evaluate the function, over the distinct dictionary values when possible
        let output = match self.try_invoke_peeled(&args, &arg_fields, batch.num_rows())? {
            Some(output) => output,
            None => self.fun.invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows: batch.num_rows(),
                return_field: Arc::clone(&self.return_field),
                config_options: Arc::clone(&self.config_options),
            })?,
        };

        if let ColumnarValue::Array(array) = &output
            && array.len() != batch.num_rows()
        {
            // If the arguments are a non-empty slice of scalar values, we can assume that
            // returning a one-element array is equivalent to returning a scalar.
            let preserve_scalar = array.len() == 1 && !input_empty && input_all_scalar;
            return if preserve_scalar {
                ScalarValue::try_from_array(array, 0).map(ColumnarValue::Scalar)
            } else {
                internal_err!(
                    "UDF {} returned a different number of rows than expected. Expected: {}, Got: {}",
                    self.name,
                    batch.num_rows(),
                    array.len()
                )
            };
        }
        Ok(output)
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&self.return_field))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.args.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(ScalarFunctionExpr::new(
            &self.name,
            Arc::clone(&self.fun),
            children,
            Arc::clone(&self.return_field),
            Arc::clone(&self.config_options),
        )))
    }

    fn evaluate_bounds(&self, children: &[&Interval]) -> Result<Interval> {
        self.fun.evaluate_bounds(children)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        children: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        self.fun.propagate_constraints(interval, children)
    }

    fn get_properties(&self, children: &[ExprProperties]) -> Result<ExprProperties> {
        let sort_properties = self.fun.output_ordering(children)?;
        let preserves_lex_ordering = self.fun.preserves_lex_ordering(children)?;
        let strictly_order_preserving = self.fun.strictly_order_preserving(children)?;
        let children_range = children
            .iter()
            .map(|props| &props.range)
            .collect::<Vec<_>>();
        let range = self.fun().evaluate_bounds(&children_range)?;

        Ok(ExprProperties {
            sort_properties,
            range,
            preserves_lex_ordering,
            strictly_order_preserving,
        })
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        for (i, expr) in self.args.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            expr.fmt_sql(f)?;
        }
        write!(f, ")")
    }

    fn is_volatile_node(&self) -> bool {
        self.fun.signature().volatility == Volatility::Volatile
    }

    fn placement(&self) -> ExpressionPlacement {
        let arg_placements: Vec<_> =
            self.args.iter().map(|arg| arg.placement()).collect();
        self.fun.placement(&arg_placements)
    }
}

#[cfg(test)]
mod tests {
    use super::dictionary::MEMOIZED_BYTES;
    use super::*;
    use crate::expressions::Column;
    use arrow::array::DictionaryArray;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Int32Type};
    use datafusion_expr::{ScalarUDFImpl, Signature};
    use datafusion_physical_expr_common::physical_expr::is_volatile;
    use std::hash::{Hash, Hasher};
    use std::sync::Mutex;

    /// Test helper to create a mock UDF with a specific volatility
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MockScalarUDF {
        signature: Signature,
    }

    impl ScalarUDFImpl for MockScalarUDF {
        fn name(&self) -> &str {
            "mock_function"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Int32)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(42))))
        }
    }

    /// Records the length of `args[0]` on every invocation, so tests can assert
    /// how many values the function actually saw.
    #[derive(Debug)]
    struct ObservingUdf {
        signature: Signature,
        seen: Arc<Mutex<Vec<usize>>>,
        saw_dictionary: Arc<std::sync::atomic::AtomicBool>,
        elementwise: bool,
        return_type: DataType,
        strict: bool,
        fail_if_contains: Option<&'static str>,
    }

    impl PartialEq for ObservingUdf {
        fn eq(&self, other: &Self) -> bool {
            self.signature == other.signature
        }
    }
    impl Eq for ObservingUdf {}
    impl Hash for ObservingUdf {
        fn hash<H: Hasher>(&self, state: &mut H) {
            self.signature.hash(state);
        }
    }

    impl ScalarUDFImpl for ObservingUdf {
        fn name(&self) -> &str {
            "observing_udf"
        }
        fn signature(&self) -> &Signature {
            &self.signature
        }
        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(self.return_type.clone())
        }
        fn evaluates_elementwise(&self) -> bool {
            self.elementwise
        }
        fn is_strict(&self) -> bool {
            self.strict
        }
        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            let len = match &args.args[0] {
                ColumnarValue::Array(array) => {
                    if matches!(array.data_type(), DataType::Dictionary(_, _)) {
                        self.saw_dictionary
                            .store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                    array.len()
                }
                ColumnarValue::Scalar(scalar) => {
                    if matches!(scalar, ScalarValue::Dictionary(_, _)) {
                        self.saw_dictionary
                            .store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                    1
                }
            };
            self.seen.lock().unwrap().push(len);
            if let (Some(needle), ColumnarValue::Array(array)) =
                (self.fail_if_contains, &args.args[0])
                && let Some(strings) = array.as_any().downcast_ref::<StringArray>()
                && strings.iter().flatten().any(|s| s == needle)
            {
                return datafusion_common::exec_err!("poisoned value {needle:?}");
            }
            // The hand-rolled arm a dictionary-returning function keeps: when a
            // declined peel hands over the dictionary itself, evaluate its
            // values and re-wrap, so the planned type still comes back.
            if let (ColumnarValue::Array(array), DataType::Dictionary(_, _)) =
                (&args.args[0], &self.return_type)
                && matches!(array.data_type(), DataType::Dictionary(_, _))
            {
                let dictionary = array.as_any_dictionary();
                let converted: Int32Array = (0..dictionary.values().len())
                    .map(|i| Some(i as i32))
                    .collect();
                return Ok(ColumnarValue::Array(
                    dictionary.with_values(Arc::new(converted)),
                ));
            }
            let values: Int32Array = (0..len).map(|i| Some(i as i32)).collect();
            Ok(ColumnarValue::Array(Arc::new(values)))
        }
    }

    struct PeelFixture {
        expr: ScalarFunctionExpr,
        batch: RecordBatch,
        values: ArrayRef,
        seen: Arc<Mutex<Vec<usize>>>,
        saw_dictionary: Arc<std::sync::atomic::AtomicBool>,
    }

    impl PeelFixture {}

    /// What a peeling test varies. Everything not named takes the default:
    /// an immutable elementwise function returning `Int32` over the three
    /// string values `ab`, `cd`, `ef`.
    struct PeelSetup {
        keys: Int32Array,
        values: ArrayRef,
        volatility: Volatility,
        elementwise: bool,
        return_type: DataType,
        strict: bool,
        fail_if_contains: Option<&'static str>,
    }

    impl Default for PeelSetup {
        fn default() -> Self {
            Self {
                keys: Int32Array::from(vec![0, 1, 0, 2, 1, 0, 2, 0]),
                values: Arc::new(StringArray::from(vec!["ab", "cd", "ef"])),
                volatility: Volatility::Immutable,
                elementwise: true,
                return_type: DataType::Int32,
                strict: false,
                fail_if_contains: None,
            }
        }
    }

    impl PeelSetup {
        /// The keys wrapped in a `ScalarFunctionExpr` over the values.
        fn build(self) -> PeelFixture {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let saw_dictionary = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let udf = Arc::new(ScalarUDF::from(ObservingUdf {
                signature: Signature::any(1, self.volatility),
                seen: Arc::clone(&seen),
                saw_dictionary: Arc::clone(&saw_dictionary),
                elementwise: self.elementwise,
                return_type: self.return_type.clone(),
                strict: self.strict,
                fail_if_contains: self.fail_if_contains,
            }));

            let values = self.values;
            let batch = dictionary_batch(self.keys, &values);
            let expr = ScalarFunctionExpr::new(
                "observing_udf",
                udf,
                vec![Arc::new(Column::new("d", 0)) as Arc<dyn PhysicalExpr>],
                Arc::new(Field::new("f", self.return_type, true)),
                Arc::new(ConfigOptions::new()),
            );
            PeelFixture {
                expr,
                batch,
                values,
                seen,
                saw_dictionary,
            }
        }
    }

    /// A one-column batch holding `keys` over `values`.
    fn dictionary_batch(keys: Int32Array, values: &ArrayRef) -> RecordBatch {
        let dict =
            DictionaryArray::<Int32Type>::try_new(keys, Arc::clone(values)).unwrap();
        let schema = Schema::new(vec![Field::new("d", dict.data_type().clone(), true)]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)]).unwrap()
    }

    fn keys_8_over_3() -> Int32Array {
        Int32Array::from(vec![0, 1, 0, 2, 1, 0, 2, 0])
    }

    /// `keys` over the default values, wrapped in a `ScalarFunctionExpr`.
    fn peel_fixture(
        keys: Int32Array,
        volatility: Volatility,
        elementwise: bool,
        return_type: DataType,
    ) -> PeelFixture {
        PeelSetup {
            keys,
            volatility,
            elementwise,
            return_type,
            ..Default::default()
        }
        .build()
    }

    #[test]
    fn peel_evaluates_once_per_distinct_value() {
        let f = peel_fixture(
            keys_8_over_3(),
            Volatility::Immutable,
            true,
            DataType::Int32,
        );
        let out = f.expr.evaluate(&f.batch).unwrap();

        // Three distinct values seen instead of eight rows...
        assert_eq!(*f.seen.lock().unwrap(), vec![3]);
        // ...and the result is still one value per row, re-mapped through the keys.
        let ColumnarValue::Array(array) = out else {
            panic!("expected an array");
        };
        assert_eq!(
            array.as_primitive::<Int32Type>().values(),
            &[0, 1, 0, 2, 1, 0, 2, 0]
        );
    }

    #[test]
    fn peel_skipped_when_flag_is_off() {
        let f = peel_fixture(
            keys_8_over_3(),
            Volatility::Immutable,
            false,
            DataType::Int32,
        );
        f.expr.evaluate(&f.batch).unwrap();
        assert_eq!(*f.seen.lock().unwrap(), vec![8]);
    }

    #[test]
    fn peel_skipped_for_volatile_functions() {
        let f =
            peel_fixture(keys_8_over_3(), Volatility::Volatile, true, DataType::Int32);
        f.expr.evaluate(&f.batch).unwrap();
        assert_eq!(*f.seen.lock().unwrap(), vec![8]);
    }

    #[test]
    fn peel_normalizes_null_keys_to_a_null_slot() {
        // Null keys are redirected to one appended NULL value: `f` sees the
        // three referenced values plus that slot, and rows with null keys get
        // `f(NULL)` (here: the slot's positional result), not an assumed NULL.
        let keys = Int32Array::from(vec![
            Some(0),
            None,
            Some(1),
            Some(2),
            Some(0),
            Some(1),
            Some(2),
            Some(0),
        ]);
        let f = peel_fixture(keys, Volatility::Immutable, true, DataType::Int32);
        let out = f.expr.evaluate(&f.batch).unwrap();
        assert_eq!(*f.seen.lock().unwrap(), vec![4]);
        let ColumnarValue::Array(array) = out else {
            panic!("expected an array");
        };
        assert_eq!(
            array.as_primitive::<Int32Type>().values(),
            &[0, 3, 1, 2, 0, 1, 2, 0]
        );
    }

    #[test]
    fn peel_ignores_garbage_under_null_key_slots() {
        // Arrow permits arbitrary key values under null slots; they must not
        // count as references (a fallible `f` would fail on data no row holds)
        // nor be read as indices.
        use arrow::buffer::NullBuffer;
        let keys = Int32Array::new(
            vec![0, 1, 0, 0, 0, 0, 0, 0].into(), // garbage `1` under the null slot
            Some(NullBuffer::from(vec![
                true, false, true, true, true, true, true, true,
            ])),
        );
        let f = peel_fixture(keys, Volatility::Immutable, true, DataType::Int32);
        f.expr.evaluate(&f.batch).unwrap();
        // Only value slot 0 is live; +1 appended NULL slot. Slot 1 ("cd") is
        // never evaluated despite the garbage key pointing at it.
        assert_eq!(*f.seen.lock().unwrap(), vec![2]);
    }

    #[test]
    fn peel_reads_no_out_of_bounds_key_under_a_null_slot() {
        // Same as above on the strict fast path, where the keys are re-mapped
        // as they are: the garbage index points past the values array.
        use arrow::buffer::NullBuffer;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let udf = Arc::new(ScalarUDF::from(ObservingUdf {
            signature: Signature::any(1, Volatility::Immutable),
            seen: Arc::clone(&seen),
            saw_dictionary: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            elementwise: true,
            return_type: DataType::Int32,
            strict: true,
            fail_if_contains: None,
        }));
        let keys = Int32Array::new(
            vec![0, 9999, 1, 2, 0, 1, 2, 0].into(),
            Some(NullBuffer::from(vec![
                true, false, true, true, true, true, true, true,
            ])),
        );
        let values = Arc::new(StringArray::from(vec!["ab", "cd", "ef"]));
        let dict = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
        let schema = Schema::new(vec![Field::new("d", dict.data_type().clone(), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)]).unwrap();
        let expr = ScalarFunctionExpr::new(
            "observing_udf",
            udf,
            vec![Arc::new(Column::new("d", 0)) as Arc<dyn PhysicalExpr>],
            Arc::new(Field::new("f", DataType::Int32, true)),
            Arc::new(ConfigOptions::new()),
        );

        let ColumnarValue::Array(array) = expr.evaluate(&batch).unwrap() else {
            panic!("expected an array");
        };
        assert_eq!(*seen.lock().unwrap(), vec![3]);
        assert!(array.is_null(1));
    }

    #[test]
    fn peel_declines_when_the_null_slot_overflows_the_key_type() {
        // Int8 keys address 128 values; a batch referencing all of them plus a
        // null key has nowhere to put the appended NULL slot.
        use arrow::datatypes::Int8Type;
        let seen = Arc::new(Mutex::new(Vec::new()));
        let udf = Arc::new(ScalarUDF::from(ObservingUdf {
            signature: Signature::any(1, Volatility::Immutable),
            seen: Arc::clone(&seen),
            saw_dictionary: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            elementwise: true,
            return_type: DataType::Int32,
            strict: false,
            fail_if_contains: None,
        }));
        let values = Arc::new(StringArray::from(
            (0..128).map(|i| format!("v{i}")).collect::<Vec<_>>(),
        ));
        let mut keys: Vec<Option<i8>> = (0..128).map(|i| Some(i as i8)).collect();
        keys.push(None);
        keys.extend((0..128).map(|i| Some(i as i8)));
        let rows = keys.len();
        let dict =
            DictionaryArray::<Int8Type>::try_new(keys.into_iter().collect(), values)
                .unwrap();
        let schema = Schema::new(vec![Field::new("d", dict.data_type().clone(), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)]).unwrap();
        let expr = ScalarFunctionExpr::new(
            "observing_udf",
            udf,
            vec![Arc::new(Column::new("d", 0)) as Arc<dyn PhysicalExpr>],
            Arc::new(Field::new("f", DataType::Int32, true)),
            Arc::new(ConfigOptions::new()),
        );

        // The batch is expanded rather than failed.
        expr.evaluate(&batch).unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![rows]);
    }

    #[test]
    fn a_dictionary_return_declines_rather_than_expands_on_overflow() {
        // The same overflow, but the planned type is a dictionary: a call over
        // expanded rows would come back flat, so the column must be handed over
        // as it arrived — exactly as if the function had not opted in.
        use arrow::datatypes::Int8Type;
        let return_type =
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int32));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let saw_dictionary = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let udf = Arc::new(ScalarUDF::from(ObservingUdf {
            signature: Signature::any(1, Volatility::Immutable),
            seen: Arc::clone(&seen),
            saw_dictionary: Arc::clone(&saw_dictionary),
            elementwise: true,
            return_type: return_type.clone(),
            strict: false,
            fail_if_contains: None,
        }));
        let values = Arc::new(StringArray::from(
            (0..128).map(|i| format!("v{i}")).collect::<Vec<_>>(),
        ));
        let mut keys: Vec<Option<i8>> = (0..128).map(|i| Some(i as i8)).collect();
        keys.push(None);
        let rows = keys.len();
        let dict =
            DictionaryArray::<Int8Type>::try_new(keys.into_iter().collect(), values)
                .unwrap();
        let schema = Schema::new(vec![Field::new("d", dict.data_type().clone(), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)]).unwrap();
        let expr = ScalarFunctionExpr::new(
            "observing_udf",
            udf,
            vec![Arc::new(Column::new("d", 0)) as Arc<dyn PhysicalExpr>],
            Arc::new(Field::new("f", return_type.clone(), true)),
            Arc::new(ConfigOptions::new()),
        );

        let out = expr.evaluate(&batch).unwrap();
        let ColumnarValue::Array(array) = out else {
            panic!("expected an array");
        };
        assert_eq!(array.data_type(), &return_type);
        assert_eq!(array.len(), rows);
        assert!(array.is_null(rows - 1));
        assert!(saw_dictionary.load(std::sync::atomic::Ordering::Relaxed));
        assert_eq!(*seen.lock().unwrap(), vec![rows]);
    }

    #[test]
    fn peel_skipped_for_empty_batches() {
        let f = peel_fixture(
            Int32Array::from(Vec::<i32>::new()),
            Volatility::Immutable,
            true,
            DataType::Int32,
        );
        f.expr.evaluate(&f.batch).unwrap();
        assert_eq!(*f.seen.lock().unwrap(), vec![0]);
    }

    #[test]
    fn peel_skipped_for_fields_with_metadata() {
        // Metadata (e.g. extension types) binds to the field's storage type,
        // which peeling would rewrite: such fields take the unpeeled path.
        let mut f = peel_fixture(
            keys_8_over_3(),
            Volatility::Immutable,
            true,
            DataType::Int32,
        );
        let metadata = std::collections::HashMap::from([(
            "ARROW:extension:name".to_string(),
            "myorg.uuid".to_string(),
        )]);
        f.expr = ScalarFunctionExpr::new(
            "observing_udf",
            Arc::new(f.expr.fun().clone()),
            f.expr.args().to_vec(),
            Arc::new(Field::new("f", DataType::Int32, true).with_metadata(metadata)),
            Arc::new(ConfigOptions::new()),
        );
        f.expr.evaluate(&f.batch).unwrap();
        assert_eq!(*f.seen.lock().unwrap(), vec![8]);
    }

    #[test]
    fn peel_fast_path_evaluates_raw_values() {
        // No null keys: the fast tier invokes over the values as-is (three,
        // including one unreferenced) with no discovery scan — the hand-rolled
        // cost profile. Harmless for an infallible `f`.
        let keys = Int32Array::from(vec![0, 1, 0, 1, 0, 1, 0, 0]);
        let f = peel_fixture(keys, Volatility::Immutable, true, DataType::Int32);
        f.expr.evaluate(&f.batch).unwrap();
        assert_eq!(*f.seen.lock().unwrap(), vec![3]);
    }

    #[test]
    fn peel_falls_back_to_compaction_when_an_unreferenced_value_errors() {
        // The fast attempt sees the poisoned unreferenced value and errors; the
        // guarded fallback compacts to the two referenced values and succeeds.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let udf = Arc::new(ScalarUDF::from(ObservingUdf {
            signature: Signature::any(1, Volatility::Immutable),
            seen: Arc::clone(&seen),
            saw_dictionary: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            elementwise: true,
            return_type: DataType::Int32,
            strict: false,
            fail_if_contains: Some("ef"),
        }));
        let keys = Int32Array::from(vec![0, 1, 0, 1, 0, 1, 0, 0]);
        let values = Arc::new(StringArray::from(vec!["ab", "cd", "ef"]));
        let dict = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
        let schema = Schema::new(vec![Field::new("d", dict.data_type().clone(), true)]);
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(dict)]).unwrap();
        let expr = ScalarFunctionExpr::new(
            "observing_udf",
            udf,
            vec![Arc::new(Column::new("d", 0)) as Arc<dyn PhysicalExpr>],
            Arc::new(Field::new("f", DataType::Int32, true)),
            Arc::new(ConfigOptions::new()),
        );
        let out = expr.evaluate(&batch).unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![3, 2]);
        let ColumnarValue::Array(array) = out else {
            panic!("expected an array");
        };
        assert_eq!(array.len(), 8);
    }

    #[test]
    fn peel_surfaces_an_error_from_a_referenced_value() {
        // The counterpart of the test above: an error the query can actually
        // observe is not swallowed by the retry.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let udf = Arc::new(ScalarUDF::from(ObservingUdf {
            signature: Signature::any(1, Volatility::Immutable),
            seen: Arc::clone(&seen),
            saw_dictionary: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            elementwise: true,
            return_type: DataType::Int32,
            strict: false,
            fail_if_contains: Some("cd"),
        }));
        let keys = Int32Array::from(vec![0, 1, 0, 1, 0, 1, 0, 0]);
        let values = Arc::new(StringArray::from(vec!["ab", "cd", "ef"]));
        let dict = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
        let schema = Schema::new(vec![Field::new("d", dict.data_type().clone(), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)]).unwrap();
        let expr = ScalarFunctionExpr::new(
            "observing_udf",
            udf,
            vec![Arc::new(Column::new("d", 0)) as Arc<dyn PhysicalExpr>],
            Arc::new(Field::new("f", DataType::Int32, true)),
            Arc::new(ConfigOptions::new()),
        );

        let error = expr.evaluate(&batch).unwrap_err().to_string();
        assert!(error.contains("poisoned value"), "{error}");
    }

    #[test]
    fn peel_fast_path_keeps_null_keys_for_strict_functions() {
        // `is_strict` promises NULL in -> NULL out, so null keys pass through
        // untouched: the fast tier runs (three raw values, no null slot) and
        // the null-key row stays NULL in the output.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let udf = Arc::new(ScalarUDF::from(ObservingUdf {
            signature: Signature::any(1, Volatility::Immutable),
            seen: Arc::clone(&seen),
            saw_dictionary: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            elementwise: true,
            return_type: DataType::Int32,
            strict: true,
            fail_if_contains: None,
        }));
        use arrow::buffer::NullBuffer;
        let keys = Int32Array::new(
            vec![0, 0, 1, 2, 0, 1, 2, 0].into(),
            Some(NullBuffer::from(vec![
                true, false, true, true, true, true, true, true,
            ])),
        );
        let values = Arc::new(StringArray::from(vec!["ab", "cd", "ef"]));
        let dict = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
        let schema = Schema::new(vec![Field::new("d", dict.data_type().clone(), true)]);
        let batch =
            RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(dict)]).unwrap();
        let expr = ScalarFunctionExpr::new(
            "observing_udf",
            udf,
            vec![Arc::new(Column::new("d", 0)) as Arc<dyn PhysicalExpr>],
            Arc::new(Field::new("f", DataType::Int32, true)),
            Arc::new(ConfigOptions::new()),
        );
        let out = expr.evaluate(&batch).unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![3]);
        let ColumnarValue::Array(array) = out else {
            panic!("expected an array");
        };
        assert!(array.is_null(1), "null-key row must stay NULL");
    }

    #[test]
    fn unprofitable_batch_is_expanded_instead_of_peeled() {
        // Two rows referencing two values: the gather would cost more than the
        // two saved invocations, so the guard declines — and `f`, which opted
        // in to elementwise evaluation, gets the expanded array rather than the
        // dictionary it cannot be assumed to handle.
        let f = peel_fixture(
            Int32Array::from(vec![0, 1]),
            Volatility::Immutable,
            true,
            DataType::Int32,
        );
        let out = f.expr.evaluate(&f.batch).unwrap();

        assert_eq!(*f.seen.lock().unwrap(), vec![2]);
        assert!(!f.saw_dictionary.load(std::sync::atomic::Ordering::Relaxed));
        let ColumnarValue::Array(array) = out else {
            panic!("expected an array");
        };
        assert_eq!(array.as_primitive::<Int32Type>().values(), &[0, 1]);
    }

    #[test]
    fn values_are_evaluated_once_across_batches() {
        // A batch carries its own keys but the dictionary of a whole column
        // chunk: the second batch over the same values evaluates nothing.
        let values = Arc::new(StringArray::from(vec!["ab", "cd", "ef"]));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let udf = Arc::new(ScalarUDF::from(ObservingUdf {
            signature: Signature::any(1, Volatility::Immutable),
            seen: Arc::clone(&seen),
            saw_dictionary: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            elementwise: true,
            return_type: DataType::Int32,
            strict: false,
            fail_if_contains: None,
        }));
        let expr = ScalarFunctionExpr::new(
            "observing_udf",
            udf,
            vec![Arc::new(Column::new("d", 0)) as Arc<dyn PhysicalExpr>],
            Arc::new(Field::new("f", DataType::Int32, true)),
            Arc::new(ConfigOptions::new()),
        );

        let batch_over = |keys: Vec<i32>, values: &Arc<StringArray>| {
            let dict = DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(keys),
                Arc::clone(values) as ArrayRef,
            )
            .unwrap();
            let schema =
                Schema::new(vec![Field::new("d", dict.data_type().clone(), true)]);
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)]).unwrap()
        };

        let keys = [
            vec![0, 1, 0, 2, 1, 0, 2, 0],
            vec![2, 2, 1, 0, 1, 1, 0, 2],
            vec![1, 0, 2, 2, 0, 1, 1, 0],
        ];
        let outputs: Vec<_> = keys
            .iter()
            .map(|keys| {
                let batch = batch_over(keys.clone(), &values);
                match expr.evaluate(&batch).unwrap() {
                    ColumnarValue::Array(array) => array,
                    _ => panic!("expected an array"),
                }
            })
            .collect();

        // The first batch cannot know the dictionary will come back; the second
        // proves it and is what the result is kept from. The third is free.
        assert_eq!(*seen.lock().unwrap(), vec![3, 3]);
        for (output, keys) in outputs.iter().zip(&keys) {
            assert_eq!(output.as_primitive::<Int32Type>().values(), keys.as_slice());
        }
    }

    #[test]
    fn concurrent_hits_share_the_memo() {
        // One expression is shared by every partition of a plan. Once a result
        // is remembered, readers take it concurrently without evaluating —
        // whatever the interleaving, the counter must not move.
        let values = Arc::new(StringArray::from(vec!["ab", "cd", "ef"]));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let udf = Arc::new(ScalarUDF::from(ObservingUdf {
            signature: Signature::any(1, Volatility::Immutable),
            seen: Arc::clone(&seen),
            saw_dictionary: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            elementwise: true,
            return_type: DataType::Int32,
            strict: false,
            fail_if_contains: None,
        }));
        let expr = Arc::new(ScalarFunctionExpr::new(
            "observing_udf",
            udf,
            vec![Arc::new(Column::new("d", 0)) as Arc<dyn PhysicalExpr>],
            Arc::new(Field::new("f", DataType::Int32, true)),
            Arc::new(ConfigOptions::new()),
        ));

        let batch_over = |keys: Vec<i32>, values: &Arc<StringArray>| {
            let dict = DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(keys),
                Arc::clone(values) as ArrayRef,
            )
            .unwrap();
            let schema =
                Schema::new(vec![Field::new("d", dict.data_type().clone(), true)]);
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)]).unwrap()
        };

        // Prove the dictionary repeats so its result is remembered.
        expr.evaluate(&batch_over(vec![0, 1, 2], &values)).unwrap();
        expr.evaluate(&batch_over(vec![2, 1, 0], &values)).unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![3, 3]);

        let handles: Vec<_> = (0..8)
            .map(|t| {
                let expr = Arc::clone(&expr);
                let batch = batch_over(vec![t % 3, (t + 1) % 3, 0], &values);
                std::thread::spawn(move || {
                    for _ in 0..200 {
                        let out = expr.evaluate(&batch).unwrap();
                        let ColumnarValue::Array(array) = out else {
                            panic!("expected an array");
                        };
                        assert_eq!(
                            array.as_primitive::<Int32Type>().values(),
                            &[t % 3, (t + 1) % 3, 0]
                        );
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        // Every concurrent pass was a hit.
        assert_eq!(*seen.lock().unwrap(), vec![3, 3]);
    }

    #[test]
    fn a_dictionary_too_large_to_keep_is_evaluated_each_time() {
        // The memo holds its entries alive, so it is bounded by bytes as well
        // as by count: values larger than the whole budget are never admitted,
        // and every batch over them is evaluated — correctly, just not freely.
        let big = "x".repeat(5 * 1024 * 1024);
        let values = Arc::new(StringArray::from(vec![big.as_str(), "cd", "ef"]));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let udf = Arc::new(ScalarUDF::from(ObservingUdf {
            signature: Signature::any(1, Volatility::Immutable),
            seen: Arc::clone(&seen),
            saw_dictionary: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            elementwise: true,
            return_type: DataType::Int32,
            strict: false,
            fail_if_contains: None,
        }));
        let expr = ScalarFunctionExpr::new(
            "observing_udf",
            udf,
            vec![Arc::new(Column::new("d", 0)) as Arc<dyn PhysicalExpr>],
            Arc::new(Field::new("f", DataType::Int32, true)),
            Arc::new(ConfigOptions::new()),
        );

        let batch_over = |keys: Vec<i32>, values: &Arc<StringArray>| {
            let dict = DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(keys),
                Arc::clone(values) as ArrayRef,
            )
            .unwrap();
            let schema =
                Schema::new(vec![Field::new("d", dict.data_type().clone(), true)]);
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)]).unwrap()
        };

        // The values hold five megabytes against a four megabyte budget: the
        // repeat is recognised, but its result is never kept.
        assert!(values.get_array_memory_size() > MEMOIZED_BYTES);
        for keys in [
            vec![0, 1, 0, 2, 1, 0, 2, 0],
            vec![2, 2, 1, 0, 1, 1, 0, 2],
            vec![1, 0, 2, 2, 0, 1, 1, 0],
        ] {
            expr.evaluate(&batch_over(keys, &values)).unwrap();
        }
        assert_eq!(*seen.lock().unwrap(), vec![3, 3, 3]);
    }

    #[test]
    fn different_values_are_evaluated_again() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let udf = Arc::new(ScalarUDF::from(ObservingUdf {
            signature: Signature::any(1, Volatility::Immutable),
            seen: Arc::clone(&seen),
            saw_dictionary: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            elementwise: true,
            return_type: DataType::Int32,
            strict: false,
            fail_if_contains: None,
        }));
        let expr = ScalarFunctionExpr::new(
            "observing_udf",
            udf,
            vec![Arc::new(Column::new("d", 0)) as Arc<dyn PhysicalExpr>],
            Arc::new(Field::new("f", DataType::Int32, true)),
            Arc::new(ConfigOptions::new()),
        );

        for values in [vec!["ab", "cd", "ef"], vec!["gh", "ij", "kl"]] {
            let dict = DictionaryArray::<Int32Type>::try_new(
                keys_8_over_3(),
                Arc::new(StringArray::from(values)),
            )
            .unwrap();
            let schema =
                Schema::new(vec![Field::new("d", dict.data_type().clone(), true)]);
            let batch =
                RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)]).unwrap();
            expr.evaluate(&batch).unwrap();
        }

        // Same shape, different contents: the second dictionary is its own work.
        assert_eq!(*seen.lock().unwrap(), vec![3, 3]);
    }

    #[test]
    fn a_dictionary_reaching_the_memo_through_another_arc_is_recognized() {
        // Batches of one column chunk can carry the same values through
        // different `Arc`s; the memo answers on what the buffers are, so the
        // third batch below evaluates nothing.
        let f = PeelSetup::default().build();
        let keys = || Int32Array::from(vec![0, 1, 2, 0, 1, 2, 0, 1]);

        f.expr
            .evaluate(&dictionary_batch(keys(), &f.values))
            .unwrap();
        f.expr
            .evaluate(&dictionary_batch(keys(), &f.values))
            .unwrap();
        assert_eq!(*f.seen.lock().unwrap(), vec![3, 3]);

        // The same buffers behind an `Arc` of its own.
        let same_values = f.values.slice(0, f.values.len());
        assert!(!Arc::ptr_eq(&same_values, &f.values));
        f.expr
            .evaluate(&dictionary_batch(keys(), &same_values))
            .unwrap();

        assert_eq!(*f.seen.lock().unwrap(), vec![3, 3]);
    }

    #[test]
    fn the_oldest_dictionary_leaves_the_memo_once_it_is_full() {
        // The memo is bounded: a dictionary that has fallen out of it is
        // evaluated again.
        let f = PeelSetup::default().build();
        let keys = || Int32Array::from(vec![0, 1, 2, 0, 1, 2, 0, 1]);
        let values_of = |n: usize| -> ArrayRef {
            Arc::new(StringArray::from(vec![
                format!("a{n}"),
                format!("b{n}"),
                format!("c{n}"),
            ]))
        };
        // Twice each: a dictionary is kept only once it has proved it repeats.
        let fill = |values: &ArrayRef| {
            for _ in 0..2 {
                f.expr.evaluate(&dictionary_batch(keys(), values)).unwrap();
            }
        };

        let stored: Vec<ArrayRef> = (0..dictionary::MEMOIZED_DICTIONARIES)
            .map(values_of)
            .collect();
        for values in &stored {
            fill(values);
        }
        let evaluations = f.seen.lock().unwrap().len();

        // Still remembered while the memo has room for it.
        f.expr
            .evaluate(&dictionary_batch(keys(), &stored[0]))
            .unwrap();
        assert_eq!(f.seen.lock().unwrap().len(), evaluations);

        // One dictionary too many. Entries leave in the order they arrived,
        // and the lookup above did not refresh the first one.
        fill(&values_of(dictionary::MEMOIZED_DICTIONARIES));
        let before_return = f.seen.lock().unwrap().len();
        f.expr
            .evaluate(&dictionary_batch(keys(), &stored[0]))
            .unwrap();
        // Exactly one evaluation more: the entry is gone, so it is recomputed.
        assert_eq!(f.seen.lock().unwrap().len(), before_return + 1);
    }

    #[test]
    fn a_dictionary_forgotten_before_it_repeated_is_not_kept() {
        // Sightings are remembered in a ring of their own. A dictionary whose
        // first sighting has aged out of it has to be seen twice again before
        // its result is worth keeping.
        let f = PeelSetup::default().build();
        let keys = || Int32Array::from(vec![0, 1, 2, 0, 1, 2, 0, 1]);
        let values_of = |n: usize| -> ArrayRef {
            Arc::new(StringArray::from(vec![
                format!("a{n}"),
                format!("b{n}"),
                format!("c{n}"),
            ]))
        };
        let evaluate = |values: &ArrayRef| {
            f.expr.evaluate(&dictionary_batch(keys(), values)).unwrap();
        };

        let first = values_of(0);
        evaluate(&first);
        // Enough other dictionaries to push that sighting out of the ring.
        // They are kept alive: sightings are recorded by buffer address, and
        // a freed dictionary's address can be handed to the next one.
        let others: Vec<ArrayRef> = (1..=dictionary::MEMOIZED_DICTIONARIES)
            .map(values_of)
            .collect();
        for values in &others {
            evaluate(values);
        }

        // Its second sighting therefore reads as a first one: nothing kept.
        evaluate(&first);
        let evaluations = f.seen.lock().unwrap().len();
        // The third proves the repeat and keeps the result...
        evaluate(&first);
        assert_eq!(f.seen.lock().unwrap().len(), evaluations + 1);
        // ...which the fourth is served from.
        evaluate(&first);
        assert_eq!(f.seen.lock().unwrap().len(), evaluations + 1);
    }

    #[test]
    fn values_that_differ_only_in_their_nulls_are_told_apart() {
        // Two values arrays can share their data buffers and differ only in
        // which slots are null. The memo must not answer for one with the
        // other's result.
        let f = PeelSetup::default().build();
        let keys = || Int32Array::from(vec![0, 1, 2, 0, 1, 2, 0, 1]);

        f.expr
            .evaluate(&dictionary_batch(keys(), &f.values))
            .unwrap();
        f.expr
            .evaluate(&dictionary_batch(keys(), &f.values))
            .unwrap();
        assert_eq!(*f.seen.lock().unwrap(), vec![3, 3]);

        // The same buffers, with the middle value marked null.
        let data = f.values.to_data();
        let nulled = arrow::array::make_array(
            data.into_builder()
                .nulls(Some(arrow::buffer::NullBuffer::from(vec![
                    true, false, true,
                ])))
                .build()
                .unwrap(),
        );

        f.expr.evaluate(&dictionary_batch(keys(), &nulled)).unwrap();

        // Not the stored dictionary: evaluated rather than answered.
        assert_eq!(*f.seen.lock().unwrap(), vec![3, 3, 3]);
    }

    #[test]
    fn peel_boundary_is_two_rows_per_referenced_value() {
        // The flat-return bound is `values * 2 <= rows`: three values are
        // peeled over six rows, and expanded over five.
        let peeled = peel_fixture(
            Int32Array::from(vec![0, 1, 2, 0, 1, 2]),
            Volatility::Immutable,
            true,
            DataType::Int32,
        );
        peeled.expr.evaluate(&peeled.batch).unwrap();
        assert_eq!(*peeled.seen.lock().unwrap(), vec![3]);

        let expanded = peel_fixture(
            Int32Array::from(vec![0, 1, 2, 0, 1]),
            Volatility::Immutable,
            true,
            DataType::Int32,
        );
        expanded.expr.evaluate(&expanded.batch).unwrap();
        assert_eq!(*expanded.seen.lock().unwrap(), vec![5]);
    }

    #[test]
    fn functions_that_did_not_opt_in_still_receive_the_dictionary() {
        let f = peel_fixture(
            keys_8_over_3(),
            Volatility::Immutable,
            false,
            DataType::Int32,
        );
        f.expr.evaluate(&f.batch).unwrap();
        assert!(f.saw_dictionary.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn peel_rewraps_when_the_planned_type_is_a_dictionary() {
        let return_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32));
        let f = peel_fixture(keys_8_over_3(), Volatility::Immutable, true, return_type);
        let out = f.expr.evaluate(&f.batch).unwrap();

        assert_eq!(*f.seen.lock().unwrap(), vec![3]);
        let ColumnarValue::Array(array) = out else {
            panic!("expected an array");
        };
        let dictionary = array.as_any_dictionary();
        assert_eq!(dictionary.len(), 8);
        assert_eq!(dictionary.values().len(), 3);
    }

    #[test]
    fn two_dictionary_arguments_are_left_alone() {
        // Peeling one argument holds the other rows fixed, which is only sound
        // when there is exactly one; two dictionaries go through as they are.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let saw_dictionary = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let udf = Arc::new(ScalarUDF::from(ObservingUdf {
            signature: Signature::any(2, Volatility::Immutable),
            seen: Arc::clone(&seen),
            saw_dictionary: Arc::clone(&saw_dictionary),
            elementwise: true,
            return_type: DataType::Int32,
            strict: false,
            fail_if_contains: None,
        }));
        let values: ArrayRef = Arc::new(StringArray::from(vec!["ab", "cd", "ef"]));
        let left =
            DictionaryArray::<Int32Type>::try_new(keys_8_over_3(), Arc::clone(&values))
                .unwrap();
        let right =
            DictionaryArray::<Int32Type>::try_new(keys_8_over_3(), values).unwrap();
        let schema = Schema::new(vec![
            Field::new("a", left.data_type().clone(), true),
            Field::new("b", right.data_type().clone(), true),
        ]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(left), Arc::new(right)])
                .unwrap();
        let expr = ScalarFunctionExpr::new(
            "observing_udf",
            udf,
            vec![
                Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
                Arc::new(Column::new("b", 1)) as Arc<dyn PhysicalExpr>,
            ],
            Arc::new(Field::new("f", DataType::Int32, true)),
            Arc::new(ConfigOptions::new()),
        );
        expr.evaluate(&batch).unwrap();
        assert!(saw_dictionary.load(std::sync::atomic::Ordering::Relaxed));
        assert_eq!(*seen.lock().unwrap(), vec![8]);
    }

    #[test]
    fn a_dictionary_return_compacts_when_larger_than_the_batch() {
        // A dictionary result re-wraps in O(1), so a batch referencing a slice
        // of a large dictionary is still peeled — through compaction, which
        // hands the function only the values the batch uses.
        let return_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let saw_dictionary = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let udf = Arc::new(ScalarUDF::from(ObservingUdf {
            signature: Signature::any(1, Volatility::Immutable),
            seen: Arc::clone(&seen),
            saw_dictionary: Arc::clone(&saw_dictionary),
            elementwise: true,
            return_type: return_type.clone(),
            strict: false,
            fail_if_contains: None,
        }));
        let values = Arc::new(StringArray::from(
            (0..100).map(|i| format!("v{i}")).collect::<Vec<_>>(),
        ));
        let keys = Int32Array::from(vec![7, 42, 7]);
        let dict = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
        let schema = Schema::new(vec![Field::new("d", dict.data_type().clone(), true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dict)]).unwrap();
        let expr = ScalarFunctionExpr::new(
            "observing_udf",
            udf,
            vec![Arc::new(Column::new("d", 0)) as Arc<dyn PhysicalExpr>],
            Arc::new(Field::new("f", return_type.clone(), true)),
            Arc::new(ConfigOptions::new()),
        );
        let out = expr.evaluate(&batch).unwrap();
        let ColumnarValue::Array(array) = out else {
            panic!("expected an array");
        };
        assert_eq!(array.data_type(), &return_type);
        assert_eq!(array.len(), 3);
        // Only the two referenced values reached the function.
        assert_eq!(*seen.lock().unwrap(), vec![2]);
        assert!(!saw_dictionary.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn dictionary_encoded_scalars_are_unwrapped() {
        // There is nothing to peel in a scalar, but its encoding is still one
        // the function did not ask to see.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let saw_dictionary = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let udf = Arc::new(ScalarUDF::from(ObservingUdf {
            signature: Signature::any(1, Volatility::Immutable),
            seen: Arc::clone(&seen),
            saw_dictionary: Arc::clone(&saw_dictionary),
            elementwise: true,
            return_type: DataType::Int32,
            strict: false,
            fail_if_contains: None,
        }));
        let scalar = ScalarValue::Dictionary(
            Box::new(DataType::Int32),
            Box::new(ScalarValue::from("ab")),
        );
        let schema = Schema::new(vec![Field::new("x", DataType::Int32, true)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let expr = ScalarFunctionExpr::new(
            "observing_udf",
            udf,
            vec![Arc::new(Literal::new(scalar)) as Arc<dyn PhysicalExpr>],
            Arc::new(Field::new("f", DataType::Int32, true)),
            Arc::new(ConfigOptions::new()),
        );
        expr.evaluate(&batch).unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![1]);
        assert!(!saw_dictionary.load(std::sync::atomic::Ordering::Relaxed));
    }

    #[test]
    fn test_scalar_function_volatile_node() {
        // Create a volatile UDF
        let volatile_udf = Arc::new(ScalarUDF::from(MockScalarUDF {
            signature: Signature::uniform(
                1,
                vec![DataType::Float32],
                Volatility::Volatile,
            ),
        }));

        // Create a non-volatile UDF
        let stable_udf = Arc::new(ScalarUDF::from(MockScalarUDF {
            signature: Signature::uniform(1, vec![DataType::Float32], Volatility::Stable),
        }));

        let schema = Schema::new(vec![Field::new("a", DataType::Float32, false)]);
        let args = vec![Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>];
        let config_options = Arc::new(ConfigOptions::new());

        // Test volatile function
        let volatile_expr = ScalarFunctionExpr::try_new(
            volatile_udf,
            args.clone(),
            &schema,
            Arc::clone(&config_options),
        )
        .unwrap();

        assert!(volatile_expr.is_volatile_node());
        let volatile_arc: Arc<dyn PhysicalExpr> = Arc::new(volatile_expr);
        assert!(is_volatile(&volatile_arc));

        // Test non-volatile function
        let stable_expr =
            ScalarFunctionExpr::try_new(stable_udf, args, &schema, config_options)
                .unwrap();

        assert!(!stable_expr.is_volatile_node());
        let stable_arc: Arc<dyn PhysicalExpr> = Arc::new(stable_expr);
        assert!(!is_volatile(&stable_arc));
    }
}
