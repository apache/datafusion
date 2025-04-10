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

use futures::future::BoxFuture;
use futures::FutureExt;
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

/// Return the currently registered tracer, or the no-op tracer if none was
/// registered.
#[inline]
fn get_tracer() -> Option<&'static dyn JoinSetTracer> {
    GLOBAL_TRACER.get().copied()
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

/// A trait for objects spawning futures and blocking closures.
pub trait Spawner<Output: Send + 'static> {
    /// Handle returned by the spawner for spawned tasks.
    type ReturnHandle;

    /// The spawner's implementation of spawning a future.
    fn spawn_impl<F>(&mut self, future: F) -> Self::ReturnHandle
    where
        F: Future<Output = Output> + Send + 'static;

    /// The spawner's implementation of spawning a blocking closure.
    fn spawn_blocking_impl<F>(&mut self, f: F) -> Self::ReturnHandle
    where
        F: FnOnce() -> Output + Send + 'static;
}

/// Spawns and optionally instruments a future with custom tracing.
///
/// If a tracer has been injected via `set_tracer`, the future's output is
/// boxed (erasing its type), passed to the tracer, and then downcast back
/// to the expected type. If no tracer is set, the original future is spawned.
///
/// # Type Parameters
/// * `T` - The concrete output type of the future.
/// * `F` - The future type.
/// * `S` - The spawner type.
///
/// # Parameters
/// * `future` - The future to potentially instrument.
/// * `spawner` - The spawner with which the closure is spawned.
pub fn trace_spawn<F, S>(future: F, spawner: &mut S) -> S::ReturnHandle
where
    S: Spawner<F::Output>,
    F: Future + Send + 'static,
    F::Output: Send + 'static {
    let Some(tracer) = get_tracer() else {
        return spawner.spawn_impl(future);
    };

    // Erase the future’s output type first:
    let erased_future = async move {
        let result = future.await;
        Box::new(result) as Box<dyn Any + Send>
    }
    .boxed();

    // Forward through the global tracer:
    spawner.spawn_impl(tracer
        .trace_future(erased_future)
        // Downcast from `Box<dyn Any + Send>` back to `T`:
        .map(|any_box| {
            *any_box
                .downcast::<F::Output>()
                .expect("Tracer must preserve the future’s output type!")
        }))
}

/// Spawns and optionally instruments a blocking closure with custom tracing.
///
/// If a tracer has been injected via `set_tracer`, the closure is wrapped so that
/// its return value is boxed (erasing its type), passed to the tracer, and then the
/// result is downcast back to the original type. If no tracer is set, the closure is
/// spawned unmodified.
///
/// # Type Parameters
/// * `T` - The concrete return type of the closure.
/// * `F` - The closure type.
/// * `S` - The spawner type.
///
/// # Parameters
/// * `f` - The blocking closure to spawn and potentially instrument.
/// * `spawner` - The spawner with which the closure is spawned.
pub fn trace_spawn_blocking<T, F, S>(f: F, spawner: &mut S) -> S::ReturnHandle
where
    S: Spawner<T>,
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let Some(tracer) = get_tracer() else {
        return spawner.spawn_blocking_impl(f);
    };
    // Erase the closure’s return type first:
    let erased_closure = Box::new(|| {
        let result = f();
        Box::new(result) as Box<dyn Any + Send>
    });

    // Forward through the global tracer:
    let traced_closure = tracer.trace_block(erased_closure);

    // Downcast from `Box<dyn Any + Send>` back to `T`:
    spawner.spawn_blocking_impl(move || {
        let any_box = traced_closure();
        *any_box
            .downcast::<T>()
            .expect("Tracer must preserve the closure’s return type!")
    })
}
