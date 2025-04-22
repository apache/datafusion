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

use futures::FutureExt;
use futures::future::BoxFuture;
use std::any::Any;
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::future::Future;
use tokio::sync::OnceCell;

/// A trait for injecting instrumentation into either asynchronous futures or
/// blocking closures at runtime.
pub trait JoinSetTracer: Send + Sync + 'static {
    /// Function pointer type for tracing a future.
    ///
    /// This function takes a boxed future (with its output type erased)
    /// and returns a boxed future (with its output still erased). The
    /// tracer must apply instrumentation without altering the output.
    fn trace_future(
        &self,
        fut: BoxFuture<'static, Box<dyn Any + Send>>,
    ) -> BoxFuture<'static, Box<dyn Any + Send>>;

    /// Function pointer type for tracing a blocking closure.
    ///
    /// This function takes a boxed closure (with its return type erased)
    /// and returns a boxed closure (with its return type still erased). The
    /// tracer must apply instrumentation without changing the return value.
    fn trace_block(
        &self,
        f: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
    ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>;
}

/// A no-op tracer that does not modify or instrument any futures or closures.
/// This is used as a fallback if no custom tracer is set.
struct NoopTracer;

impl JoinSetTracer for NoopTracer {
    fn trace_future(
        &self,
        fut: BoxFuture<'static, Box<dyn Any + Send>>,
    ) -> BoxFuture<'static, Box<dyn Any + Send>> {
        fut
    }

    fn trace_block(
        &self,
        f: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
    ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send> {
        f
    }
}

/// A custom error type for tracer injection failures.
#[derive(Debug)]
pub enum JoinSetTracerError {
    /// The global tracer has already been set.
    AlreadySet,
}

impl Display for JoinSetTracerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            JoinSetTracerError::AlreadySet => {
                write!(f, "The global JoinSetTracer is already set")
            }
        }
    }
}

impl Error for JoinSetTracerError {}

/// Global storage for an injected tracer. If no tracer is injected, a no-op
/// tracer is used instead. This ensures that calls to [`trace_future`] or
/// [`trace_block`] never panic due to missing instrumentation.
static GLOBAL_TRACER: OnceCell<&'static dyn JoinSetTracer> = OnceCell::const_new();

/// A no-op tracer singleton that is returned by [`get_tracer`] if no custom
/// tracer has been registered.
static NOOP_TRACER: NoopTracer = NoopTracer;

/// Return the currently registered tracer, or the no-op tracer if none was
/// registered.
#[inline]
fn get_tracer() -> &'static dyn JoinSetTracer {
    GLOBAL_TRACER.get().copied().unwrap_or(&NOOP_TRACER)
}

/// Set the custom tracer for both futures and blocking closures.
///
/// This should be called once at startup. If called more than once, an
/// `Err(JoinSetTracerError)` is returned. If not called at all, a no-op tracer that does nothing
/// is used.
pub fn set_join_set_tracer(
    tracer: &'static dyn JoinSetTracer,
) -> Result<(), JoinSetTracerError> {
    GLOBAL_TRACER
        .set(tracer)
        .map_err(|_set_err| JoinSetTracerError::AlreadySet)
}

/// Optionally instruments a future with custom tracing.
///
/// If a tracer has been injected via `set_tracer`, the future's output is
/// boxed (erasing its type), passed to the tracer, and then downcast back
/// to the expected type. If no tracer is set, the original future is returned.
///
/// # Type Parameters
/// * `T` - The concrete output type of the future.
/// * `F` - The future type.
///
/// # Parameters
/// * `future` - The future to potentially instrument.
pub fn trace_future<T, F>(future: F) -> BoxFuture<'static, T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    // Erase the future’s output type first:
    let erased_future = async move {
        let result = future.await;
        Box::new(result) as Box<dyn Any + Send>
    }
    .boxed();

    // Forward through the global tracer:
    get_tracer()
        .trace_future(erased_future)
        // Downcast from `Box<dyn Any + Send>` back to `T`:
        .map(|any_box| {
            *any_box
                .downcast::<T>()
                .expect("Tracer must preserve the future’s output type!")
        })
        .boxed()
}

/// Optionally instruments a blocking closure with custom tracing.
///
/// If a tracer has been injected via `set_tracer`, the closure is wrapped so that
/// its return value is boxed (erasing its type), passed to the tracer, and then the
/// result is downcast back to the original type. If no tracer is set, the closure is
/// returned unmodified (except for being boxed).
///
/// # Type Parameters
/// * `T` - The concrete return type of the closure.
/// * `F` - The closure type.
///
/// # Parameters
/// * `f` - The blocking closure to potentially instrument.
pub fn trace_block<T, F>(f: F) -> Box<dyn FnOnce() -> T + Send>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    // Erase the closure’s return type first:
    let erased_closure = Box::new(|| {
        let result = f();
        Box::new(result) as Box<dyn Any + Send>
    });

    // Forward through the global tracer:
    let traced_closure = get_tracer().trace_block(erased_closure);

    // Downcast from `Box<dyn Any + Send>` back to `T`:
    Box::new(move || {
        let any_box = traced_closure();
        *any_box
            .downcast::<T>()
            .expect("Tracer must preserve the closure’s return type!")
    })
}
