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
use std::sync::Arc;

use crate::PhysicalExpr;
use crate::expressions::Literal;

use arrow::array::{Array, RecordBatch};
use arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion_common::config::{ConfigEntry, ConfigOptions};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::ExprProperties;
use datafusion_expr::type_coercion::functions::fields_with_udf;
use datafusion_expr::{
    ColumnarValue, ExpressionPlacement, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Volatility, expr_vec_fmt,
};

/// Physical expression of a scalar function
pub struct ScalarFunctionExpr {
    fun: Arc<ScalarUDF>,
    name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
    return_field: FieldRef,
    config_options: Arc<ConfigOptions>,
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

        // evaluate the function
        let output = self.fun.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: batch.num_rows(),
            return_field: Arc::clone(&self.return_field),
            config_options: Arc::clone(&self.config_options),
        })?;

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
        // The default UDF bounds carry no type information. Recover the resolved
        // output type for property inference without discarding explicit bounds.
        let range = if range.data_type() == DataType::Null && range.is_unbounded() {
            Interval::make_unbounded(self.return_type()).unwrap_or(range)
        } else {
            range
        };

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
    use super::*;
    use crate::expressions::Column;
    use arrow::datatypes::{Field, TimeUnit};
    use datafusion_expr::sort_properties::SortProperties;
    use datafusion_expr::{ScalarUDFImpl, Signature};
    use datafusion_physical_expr_common::physical_expr::is_volatile;

    /// Test UDF with configurable volatility and return type, using default bounds.
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct MockScalarUDF {
        signature: Signature,
        return_type: DataType,
    }

    impl ScalarUDFImpl for MockScalarUDF {
        fn name(&self) -> &str {
            "mock_function"
        }

        fn signature(&self) -> &Signature {
            &self.signature
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(self.return_type.clone())
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from(
                &self.return_type,
            )?))
        }
    }

    fn default_bounds_expr(return_type: DataType) -> ScalarFunctionExpr {
        ScalarFunctionExpr::try_new(
            Arc::new(ScalarUDF::from(MockScalarUDF {
                signature: Signature::exact(vec![], Volatility::Immutable),
                return_type,
            })),
            vec![],
            &Schema::empty(),
            Arc::new(ConfigOptions::default()),
        )
        .unwrap()
    }

    #[test]
    fn properties_recover_return_type_without_changing_bounds_evaluation() {
        for data_type in [
            DataType::Null,
            DataType::Boolean,
            DataType::Int32,
            DataType::UInt64,
            DataType::Float64,
            DataType::Utf8,
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("America/Goose_Bay".into())),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        ] {
            let expr = default_bounds_expr(data_type.clone());
            let properties = expr.get_properties(&[]).unwrap();
            assert_eq!(
                properties.range,
                Interval::make_unbounded(&data_type).unwrap()
            );
            assert_eq!(properties.sort_properties, SortProperties::Unordered);
            assert!(!properties.preserves_lex_ordering);
            assert!(!properties.strictly_order_preserving);
            // The constraint solver calls this method directly, bypassing the fallback.
            assert_eq!(
                expr.evaluate_bounds(&[]).unwrap(),
                Interval::make_unbounded(&DataType::Null).unwrap()
            );
        }
    }

    #[test]
    fn properties_keep_unknown_bounds_for_unsupported_return_type() {
        // This unit is not supported for Time32 by ScalarValue/Interval.
        let data_type = DataType::Time32(TimeUnit::Nanosecond);
        assert!(Interval::make_unbounded(&data_type).is_err());
        let properties = default_bounds_expr(data_type).get_properties(&[]).unwrap();
        assert_eq!(
            properties.range,
            Interval::make_unbounded(&DataType::Null).unwrap()
        );
    }

    #[test]
    fn datetime_properties_use_resolved_return_types() {
        use datafusion_functions::datetime::date_bin::DateBinFunc;
        use datafusion_functions::datetime::from_unixtime::FromUnixtimeFunc;

        for source_type in [
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            DataType::Time32(TimeUnit::Millisecond),
            DataType::Time64(TimeUnit::Microsecond),
        ] {
            let expr = ScalarFunctionExpr::try_new(
                Arc::new(ScalarUDF::from(DateBinFunc::new())),
                vec![
                    Arc::new(Literal::new(ScalarValue::new_interval_mdn(
                        0,
                        0,
                        60_000_000_000,
                    ))),
                    Arc::new(Column::new("ts", 0)),
                ],
                &Schema::new(vec![Field::new("ts", source_type.clone(), true)]),
                Arc::new(ConfigOptions::default()),
            )
            .unwrap();
            let properties = expr
                .get_properties(&[
                    ExprProperties::new_unknown().with_order(SortProperties::Singleton),
                    ExprProperties::new_unknown()
                        .with_range(Interval::make_unbounded(&source_type).unwrap()),
                ])
                .unwrap();
            assert_eq!(
                properties.range,
                Interval::make_unbounded(&source_type).unwrap()
            );
        }

        for (session_tz, explicit_tz, expected_tz) in [
            (None, None, None),
            (Some("America/Denver"), None, Some("America/Denver")),
            (
                Some("America/Denver"),
                Some("America/Goose_Bay"),
                Some("America/Goose_Bay"),
            ),
        ] {
            let mut config = ConfigOptions::default();
            config.execution.time_zone = session_tz.map(str::to_owned);
            let udf = FromUnixtimeFunc::new_with_config(&config);
            let mut args: Vec<Arc<dyn PhysicalExpr>> =
                vec![Arc::new(Column::new("c", 0))];
            if let Some(tz) = explicit_tz {
                args.push(Arc::new(Literal::new(ScalarValue::from(tz))));
            }
            let children = vec![ExprProperties::new_unknown(); args.len()];
            let expr = ScalarFunctionExpr::try_new(
                Arc::new(ScalarUDF::from(udf)),
                args,
                &Schema::new(vec![Field::new("c", DataType::Int64, true)]),
                Arc::new(config),
            )
            .unwrap();
            // The literal's timezone was resolved during construction, even
            // though the child intervals here contain no value information.
            let expected =
                DataType::Timestamp(TimeUnit::Second, expected_tz.map(Into::into));
            assert_eq!(
                expr.get_properties(&children).unwrap().range,
                Interval::make_unbounded(&expected).unwrap()
            );
        }
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct BoundsUDF {
        inner: MockScalarUDF,
        fail: bool,
    }

    impl ScalarUDFImpl for BoundsUDF {
        fn name(&self) -> &str {
            "bounds_function"
        }

        fn signature(&self) -> &Signature {
            self.inner.signature()
        }

        fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
            self.inner.return_type(arg_types)
        }

        fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            self.inner.invoke_with_args(args)
        }

        fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
            if self.fail {
                return internal_err!("bounds evaluation failed");
            }
            Ok(inputs[0].clone())
        }
    }

    #[test]
    fn properties_preserve_udf_bounds_and_errors() {
        for fail in [false, true] {
            let expr = ScalarFunctionExpr::try_new(
                Arc::new(ScalarUDF::from(BoundsUDF {
                    inner: MockScalarUDF {
                        signature: Signature::exact(
                            vec![DataType::Int32],
                            Volatility::Immutable,
                        ),
                        return_type: DataType::Int32,
                    },
                    fail,
                })),
                vec![Arc::new(Column::new("a", 0))],
                &Schema::new(vec![Field::new("a", DataType::Int32, true)]),
                Arc::new(ConfigOptions::default()),
            )
            .unwrap();
            for (lower, upper) in [
                (Some(0), Some(100)),
                (Some(1), Some(1)),
                (None, Some(100)),
                (Some(0), None),
                (None, None),
            ] {
                let bounds = Interval::make::<i32>(lower, upper).unwrap();
                let child = ExprProperties::new_unknown().with_range(bounds.clone());
                let properties = expr.get_properties(&[child]);
                let direct = expr.evaluate_bounds(&[&bounds]);
                if fail {
                    assert!(
                        properties
                            .unwrap_err()
                            .to_string()
                            .contains("bounds evaluation failed")
                    );
                    assert!(
                        direct
                            .unwrap_err()
                            .to_string()
                            .contains("bounds evaluation failed")
                    );
                } else {
                    assert_eq!(properties.unwrap().range, bounds);
                    assert_eq!(direct.unwrap(), bounds);
                }
            }
        }
    }

    #[test]
    fn test_scalar_function_volatile_node() {
        // Create a volatile UDF
        let volatile_udf = Arc::new(ScalarUDF::from(MockScalarUDF {
            return_type: DataType::Int32,
            signature: Signature::uniform(
                1,
                vec![DataType::Float32],
                Volatility::Volatile,
            ),
        }));

        // Create a non-volatile UDF
        let stable_udf = Arc::new(ScalarUDF::from(MockScalarUDF {
            return_type: DataType::Int32,
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
