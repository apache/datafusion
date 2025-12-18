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

//! Custom metric value type.

use std::{any::Any, fmt::Debug, fmt::Display, sync::Arc};

/// A trait for implementing custom metric values.
///
/// This trait enables defining application- or operator-specific metric types
/// that can be aggregated and displayed alongside standard metrics. These
/// custom metrics integrate with [`MetricValue::Custom`] and support
/// aggregation logic, introspection, and optional numeric representation.
///
/// # Requirements
/// Implementations of `CustomMetricValue` must satisfy the following:
///
/// 1. [`Self::aggregate`]: Defines how two metric values are combined
/// 2. [`Self::new_empty`]: Returns a new, zero-value instance for accumulation
/// 3. [`Self::as_any`]: Enables dynamic downcasting for type-specific operations
/// 4. [`Self::as_usize`]: Optionally maps the value to a `usize` (for sorting, display, etc.)
/// 5. [`Self::is_eq`]: Implements comparison between two values, this isn't reusing the std
///    PartialEq trait because this trait is used dynamically in the context of
///    [`MetricValue::Custom`]
///
/// # Examples
/// ```
/// # use std::sync::Arc;
/// # use std::fmt::{Debug, Display};
/// # use std::any::Any;
/// # use std::sync::atomic::{AtomicUsize, Ordering};
///
/// # use datafusion_execution::metrics::CustomMetricValue;
///
/// #[derive(Debug, Default)]
/// struct MyCounter {
///     count: AtomicUsize,
/// }
///
/// impl Display for MyCounter {
///     fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
///         write!(f, "count: {}", self.count.load(Ordering::Relaxed))
///     }
/// }
///
/// impl CustomMetricValue for MyCounter {
///     fn new_empty(&self) -> Arc<dyn CustomMetricValue> {
///         Arc::new(Self::default())
///     }
///
///     fn aggregate(&self, other: Arc<dyn CustomMetricValue>) {
///         let other = other.as_any().downcast_ref::<Self>().unwrap();
///         self.count
///             .fetch_add(other.count.load(Ordering::Relaxed), Ordering::Relaxed);
///     }
///
///     fn as_any(&self) -> &dyn Any {
///         self
///     }
///
///     fn as_usize(&self) -> usize {
///         self.count.load(Ordering::Relaxed)
///     }
///
///     fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool {
///         let Some(other) = other.as_any().downcast_ref::<Self>() else {
///             return false;
///         };
///
///         self.count.load(Ordering::Relaxed) == other.count.load(Ordering::Relaxed)
///     }
/// }
/// ```
///
/// [`MetricValue::Custom`]: super::MetricValue::Custom
pub trait CustomMetricValue: Display + Debug + Send + Sync {
    /// Returns a new, zero-initialized version of this metric value.
    ///
    /// This value is used during metric aggregation to accumulate results.
    fn new_empty(&self) -> Arc<dyn CustomMetricValue>;

    /// Merges another metric value into this one.
    ///
    /// The type of `other` could be of a different custom type as long as it's aggregatable into self.
    fn aggregate(&self, other: Arc<dyn CustomMetricValue + 'static>);

    /// Returns this value as a [`Any`] to support dynamic downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Optionally returns a numeric representation of the value, if meaningful.
    /// Otherwise will default to zero.
    ///
    /// This is used for sorting and summarizing metrics.
    fn as_usize(&self) -> usize {
        0
    }

    /// Compares this value with another custom value.
    fn is_eq(&self, other: &Arc<dyn CustomMetricValue>) -> bool;
}
