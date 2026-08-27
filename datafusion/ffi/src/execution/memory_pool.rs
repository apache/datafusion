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

//! FFI support for [`MemoryPool`].
//!
//! Sharing the pool across the boundary means an [`ExecutionPlan`] shared over
//! FFI allocates from the same budget as the session executing it, so
//! `datafusion.execution.memory_limit` applies to it and its allocations are
//! visible in the session's accounting.
//!
//! # How reservations are tracked
//!
//! [`MemoryPool`] methods take a `&MemoryReservation`, which cannot cross the
//! boundary. What they actually need from it is the consumer identity and the
//! current size, so only the consumer's process-unique
//! [`MemoryConsumer::id`] is sent. The providing side keeps one real
//! [`MemoryReservation`] per foreign consumer and forwards `grow` / `shrink` /
//! `try_grow` onto it, which keeps the real pool's accounting exactly in step
//! with the foreign one. Dropping the entry on `unregister` releases any
//! outstanding bytes, so a foreign library that leaks a reservation cannot
//! permanently consume the host's budget beyond its own lifetime.
//!
//! Consumer ids are only unique within the library that generated them, so each
//! [`FFI_MemoryPool`] clone keeps its own map. A consumer registered through one
//! handle always grows and shrinks through that same handle, so the maps never
//! need to agree.
//!
//! [`ExecutionPlan`]: datafusion_physical_plan::ExecutionPlan

use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::{Arc, Mutex};

use datafusion_common::{DataFusionError, Result};
use datafusion_execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use stabby::string::String as SString;

/// An FFI-safe [`MemoryLimit`].
#[repr(C, u8)]
#[derive(Debug, Clone, Copy)]
pub enum FFI_MemoryLimit {
    Infinite,
    Finite(u64),
    Unknown,
}

impl From<MemoryLimit> for FFI_MemoryLimit {
    fn from(limit: MemoryLimit) -> Self {
        match limit {
            MemoryLimit::Infinite => FFI_MemoryLimit::Infinite,
            MemoryLimit::Finite(size) => FFI_MemoryLimit::Finite(size as u64),
            MemoryLimit::Unknown => FFI_MemoryLimit::Unknown,
        }
    }
}

impl From<FFI_MemoryLimit> for MemoryLimit {
    fn from(limit: FFI_MemoryLimit) -> Self {
        match limit {
            FFI_MemoryLimit::Infinite => MemoryLimit::Infinite,
            FFI_MemoryLimit::Finite(size) => MemoryLimit::Finite(size as usize),
            FFI_MemoryLimit::Unknown => MemoryLimit::Unknown,
        }
    }
}

/// The result of a [`MemoryPool::try_grow`] call.
///
/// [`DataFusionError::ResourcesExhausted`] is carried as its own variant rather
/// than as a generic error, so spilling operators on the far side still
/// recognise a rejected allocation as recoverable and spill to disk instead of
/// failing the query.
#[repr(C, u8)]
#[derive(Debug, Clone)]
pub enum FFI_TryGrowResult {
    Ok,
    ResourcesExhausted(SString),
    Other(SString),
}

impl From<Result<()>> for FFI_TryGrowResult {
    fn from(result: Result<()>) -> Self {
        match result {
            Ok(()) => FFI_TryGrowResult::Ok,
            Err(DataFusionError::ResourcesExhausted(msg)) => {
                FFI_TryGrowResult::ResourcesExhausted(msg.as_str().into())
            }
            Err(e) => FFI_TryGrowResult::Other(e.to_string().as_str().into()),
        }
    }
}

impl From<FFI_TryGrowResult> for Result<()> {
    fn from(result: FFI_TryGrowResult) -> Self {
        match result {
            FFI_TryGrowResult::Ok => Ok(()),
            FFI_TryGrowResult::ResourcesExhausted(msg) => {
                Err(DataFusionError::ResourcesExhausted(msg.to_string()))
            }
            FFI_TryGrowResult::Other(msg) => datafusion_common::ffi_err!("{msg}"),
        }
    }
}

/// A stable struct for sharing [`MemoryPool`] across FFI boundaries.
#[repr(C)]
#[derive(Debug)]
pub struct FFI_MemoryPool {
    /// Return the pool name.
    pub name: unsafe extern "C" fn(pool: &Self) -> SString,

    /// Register a consumer, identified by its process-unique id.
    pub register: unsafe extern "C" fn(
        pool: &Self,
        consumer_id: u64,
        name: SString,
        can_spill: bool,
    ),

    /// Release a previously registered consumer along with any bytes it still
    /// holds.
    pub unregister: unsafe extern "C" fn(pool: &Self, consumer_id: u64),

    /// Infallibly grow the consumer's reservation.
    pub grow: unsafe extern "C" fn(pool: &Self, consumer_id: u64, additional: u64),

    /// Infallibly shrink the consumer's reservation.
    pub shrink: unsafe extern "C" fn(pool: &Self, consumer_id: u64, shrink: u64),

    /// Attempt to grow the consumer's reservation.
    pub try_grow: unsafe extern "C" fn(
        pool: &Self,
        consumer_id: u64,
        additional: u64,
    ) -> FFI_TryGrowResult,

    /// Return the total number of bytes reserved across the whole pool.
    pub reserved: unsafe extern "C" fn(pool: &Self) -> u64,

    /// Return the pool's memory limit.
    pub memory_limit: unsafe extern "C" fn(pool: &Self) -> FFI_MemoryLimit,

    /// Used to create a clone on the provider of the pool. This should
    /// only need to be called by the receiver of the pool.
    pub clone: unsafe extern "C" fn(pool: &Self) -> Self,

    /// Release the memory of the private data when it is no longer being used.
    pub release: unsafe extern "C" fn(arg: &mut Self),

    /// Internal data. This is only to be accessed by the provider of the pool.
    /// The foreign library should never attempt to access this data.
    pub private_data: *mut c_void,

    /// Utility to identify when FFI objects are accessed locally through
    /// the foreign interface. See [`crate::get_library_marker_id`] and
    /// the crate's `README.md` for more information.
    pub library_marker_id: extern "C" fn() -> usize,
}

unsafe impl Send for FFI_MemoryPool {}
unsafe impl Sync for FFI_MemoryPool {}

struct MemoryPoolPrivateData {
    pool: Arc<dyn MemoryPool>,
    /// One real reservation per foreign consumer id. See the module docs.
    reservations: Mutex<HashMap<u64, MemoryReservation>>,
}

impl MemoryPoolPrivateData {
    /// Run `f` against the reservation for `consumer_id`.
    ///
    /// A missing entry means the foreign side grew a consumer it never
    /// registered. That should not happen, since `MemoryConsumer::register` is
    /// the only way to obtain a reservation, but dropping the allocation
    /// silently would understate usage and defeat the memory limit. Register it
    /// late instead and warn.
    fn with_reservation<T>(
        &self,
        consumer_id: u64,
        f: impl FnOnce(&MemoryReservation) -> T,
    ) -> Option<T> {
        let mut reservations = match self.reservations.lock() {
            Ok(guard) => guard,
            Err(e) => {
                log::error!("FFI memory pool reservation map is poisoned: {e}");
                return None;
            }
        };

        let reservation = reservations.entry(consumer_id).or_insert_with(|| {
            log::warn!(
                "Foreign memory consumer {consumer_id} was used before being registered; \
                 registering it now so its usage is still accounted for"
            );
            MemoryConsumer::new(format!("ffi_consumer_{consumer_id}"))
                .register(&self.pool)
        });

        Some(f(reservation))
    }
}

impl FFI_MemoryPool {
    fn private_data(&self) -> &MemoryPoolPrivateData {
        unsafe { &*(self.private_data as *const MemoryPoolPrivateData) }
    }

    /// Create a new [`FFI_MemoryPool`] from a local pool.
    pub fn new(pool: Arc<dyn MemoryPool>) -> Self {
        Self {
            name: name_fn_wrapper,
            register: register_fn_wrapper,
            unregister: unregister_fn_wrapper,
            grow: grow_fn_wrapper,
            shrink: shrink_fn_wrapper,
            try_grow: try_grow_fn_wrapper,
            reserved: reserved_fn_wrapper,
            memory_limit: memory_limit_fn_wrapper,
            clone: clone_fn_wrapper,
            release: release_fn_wrapper,
            private_data: Box::into_raw(Box::new(MemoryPoolPrivateData {
                pool,
                reservations: Mutex::new(HashMap::new()),
            })) as *mut c_void,
            library_marker_id: crate::get_library_marker_id,
        }
    }

    /// If this pool originated in the current library, return the underlying
    /// [`MemoryPool`] directly.
    pub fn as_local(&self) -> Option<Arc<dyn MemoryPool>> {
        ((self.library_marker_id)() == crate::get_library_marker_id())
            .then(|| Arc::clone(&self.private_data().pool))
    }
}

unsafe extern "C" fn name_fn_wrapper(pool: &FFI_MemoryPool) -> SString {
    pool.private_data().pool.name().into()
}

unsafe extern "C" fn register_fn_wrapper(
    pool: &FFI_MemoryPool,
    consumer_id: u64,
    name: SString,
    can_spill: bool,
) {
    let private_data = pool.private_data();
    let reservation = MemoryConsumer::new(name.to_string())
        .with_can_spill(can_spill)
        .register(&private_data.pool);

    match private_data.reservations.lock() {
        Ok(mut reservations) => {
            reservations.insert(consumer_id, reservation);
        }
        Err(e) => log::error!("FFI memory pool reservation map is poisoned: {e}"),
    }
}

unsafe extern "C" fn unregister_fn_wrapper(pool: &FFI_MemoryPool, consumer_id: u64) {
    let private_data = pool.private_data();
    match private_data.reservations.lock() {
        // Dropping the reservation frees any outstanding bytes back to the pool
        // and unregisters the consumer.
        Ok(mut reservations) => {
            reservations.remove(&consumer_id);
        }
        Err(e) => log::error!("FFI memory pool reservation map is poisoned: {e}"),
    }
}

unsafe extern "C" fn grow_fn_wrapper(
    pool: &FFI_MemoryPool,
    consumer_id: u64,
    additional: u64,
) {
    pool.private_data()
        .with_reservation(consumer_id, |reservation| {
            reservation.grow(additional as usize)
        });
}

unsafe extern "C" fn shrink_fn_wrapper(
    pool: &FFI_MemoryPool,
    consumer_id: u64,
    shrink: u64,
) {
    pool.private_data()
        .with_reservation(consumer_id, |reservation| {
            // `MemoryReservation::shrink` panics if asked to free more than it
            // holds. The foreign side tracks its own size and should never do
            // that, but a panic across the FFI boundary is undefined behaviour,
            // so clamp instead.
            let capacity = (shrink as usize).min(reservation.size());
            if capacity > 0 {
                reservation.shrink(capacity);
            }
        });
}

unsafe extern "C" fn try_grow_fn_wrapper(
    pool: &FFI_MemoryPool,
    consumer_id: u64,
    additional: u64,
) -> FFI_TryGrowResult {
    pool.private_data()
        .with_reservation(consumer_id, |reservation| {
            FFI_TryGrowResult::from(reservation.try_grow(additional as usize))
        })
        .unwrap_or_else(|| {
            FFI_TryGrowResult::Other("FFI memory pool is unavailable".into())
        })
}

unsafe extern "C" fn reserved_fn_wrapper(pool: &FFI_MemoryPool) -> u64 {
    pool.private_data().pool.reserved() as u64
}

unsafe extern "C" fn memory_limit_fn_wrapper(pool: &FFI_MemoryPool) -> FFI_MemoryLimit {
    pool.private_data().pool.memory_limit().into()
}

unsafe extern "C" fn clone_fn_wrapper(pool: &FFI_MemoryPool) -> FFI_MemoryPool {
    // A clone gets its own reservation map: consumers registered through one
    // handle always grow and shrink through that same handle.
    FFI_MemoryPool::new(Arc::clone(&pool.private_data().pool))
}

unsafe extern "C" fn release_fn_wrapper(pool: &mut FFI_MemoryPool) {
    unsafe {
        debug_assert!(!pool.private_data.is_null());
        drop(Box::from_raw(
            pool.private_data as *mut MemoryPoolPrivateData,
        ));
        pool.private_data = std::ptr::null_mut();
    }
}

impl Clone for FFI_MemoryPool {
    fn clone(&self) -> Self {
        unsafe { (self.clone)(self) }
    }
}

impl Drop for FFI_MemoryPool {
    fn drop(&mut self) {
        unsafe { (self.release)(self) }
    }
}

/// A [`MemoryPool`] backed by a foreign [`FFI_MemoryPool`].
#[derive(Debug)]
pub struct ForeignMemoryPool {
    pool: FFI_MemoryPool,
    /// The underlying pool's name, fetched once at construction.
    ///
    /// [`MemoryPool::name`] returns a borrowed `&str`, but the name arrives
    /// owned from across the boundary and so cannot be fetched per call. The
    /// name appears in resource-exhaustion messages, so keeping the real one
    /// makes those messages name the pool that actually rejected the
    /// allocation.
    name: String,
}

unsafe impl Send for ForeignMemoryPool {}
unsafe impl Sync for ForeignMemoryPool {}

impl From<FFI_MemoryPool> for ForeignMemoryPool {
    fn from(pool: FFI_MemoryPool) -> Self {
        let name = unsafe { (pool.name)(&pool) }.to_string();
        Self { pool, name }
    }
}

impl From<FFI_MemoryPool> for Arc<dyn MemoryPool> {
    fn from(pool: FFI_MemoryPool) -> Self {
        match pool.as_local() {
            Some(local) => local,
            None => Arc::new(ForeignMemoryPool::from(pool)),
        }
    }
}

impl std::fmt::Display for ForeignMemoryPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl MemoryPool for ForeignMemoryPool {
    fn name(&self) -> &str {
        &self.name
    }

    fn register(&self, consumer: &MemoryConsumer) {
        unsafe {
            (self.pool.register)(
                &self.pool,
                consumer.id() as u64,
                consumer.name().into(),
                consumer.can_spill(),
            )
        }
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        unsafe { (self.pool.unregister)(&self.pool, consumer.id() as u64) }
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        unsafe {
            (self.pool.grow)(
                &self.pool,
                reservation.consumer().id() as u64,
                additional as u64,
            )
        }
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        unsafe {
            (self.pool.shrink)(
                &self.pool,
                reservation.consumer().id() as u64,
                shrink as u64,
            )
        }
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let result = unsafe {
            (self.pool.try_grow)(
                &self.pool,
                reservation.consumer().id() as u64,
                additional as u64,
            )
        };
        result.into()
    }

    fn reserved(&self) -> usize {
        unsafe { (self.pool.reserved)(&self.pool) as usize }
    }

    fn memory_limit(&self) -> MemoryLimit {
        unsafe { (self.pool.memory_limit)(&self.pool) }.into()
    }
}

#[cfg(test)]
mod tests {
    use datafusion_execution::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};

    use super::*;

    fn foreign_pool(pool: Arc<dyn MemoryPool>) -> Arc<dyn MemoryPool> {
        let mut ffi = FFI_MemoryPool::new(pool);
        ffi.library_marker_id = crate::mock_foreign_marker_id;
        Arc::new(ForeignMemoryPool::from(ffi))
    }

    #[test]
    fn grow_and_shrink_are_reflected_in_the_real_pool() {
        let real = Arc::new(GreedyMemoryPool::new(1_000)) as Arc<dyn MemoryPool>;
        let foreign = foreign_pool(Arc::clone(&real));

        let reservation = MemoryConsumer::new("test").register(&foreign);
        reservation.grow(400);
        assert_eq!(real.reserved(), 400);
        assert_eq!(foreign.reserved(), 400);

        reservation.shrink(150);
        assert_eq!(real.reserved(), 250);

        drop(reservation);
        assert_eq!(real.reserved(), 0);
    }

    /// The point of forwarding the pool: a limit configured on the host must
    /// actually constrain a foreign consumer.
    #[test]
    fn memory_limit_is_enforced_across_the_boundary() {
        let real = Arc::new(GreedyMemoryPool::new(1_000)) as Arc<dyn MemoryPool>;
        let foreign = foreign_pool(Arc::clone(&real));

        let reservation = MemoryConsumer::new("test").register(&foreign);
        assert!(reservation.try_grow(900).is_ok());

        let err = reservation.try_grow(200).unwrap_err();
        assert!(
            matches!(err, DataFusionError::ResourcesExhausted(_)),
            "expected ResourcesExhausted so operators spill, got {err:?}"
        );
    }

    #[test]
    fn memory_limit_is_reported() {
        let real = Arc::new(GreedyMemoryPool::new(4_096)) as Arc<dyn MemoryPool>;
        let foreign = foreign_pool(real);
        assert!(matches!(foreign.memory_limit(), MemoryLimit::Finite(4_096)));

        let unbounded = foreign_pool(Arc::new(UnboundedMemoryPool::default()));
        assert!(matches!(
            unbounded.memory_limit(),
            MemoryLimit::Infinite | MemoryLimit::Unknown
        ));
    }

    /// Dropping a reservation on the foreign side must release the bytes it
    /// held on the host side, otherwise a foreign plan would leak the host's
    /// budget.
    #[test]
    fn unregister_releases_outstanding_bytes() {
        let real = Arc::new(GreedyMemoryPool::new(1_000)) as Arc<dyn MemoryPool>;
        let foreign = foreign_pool(Arc::clone(&real));

        let reservation = MemoryConsumer::new("leaky").register(&foreign);
        reservation.grow(500);
        assert_eq!(real.reserved(), 500);

        // Drop without shrinking first.
        drop(reservation);
        assert_eq!(real.reserved(), 0);
    }

    #[test]
    fn multiple_consumers_are_tracked_independently() {
        let real = Arc::new(GreedyMemoryPool::new(1_000)) as Arc<dyn MemoryPool>;
        let foreign = foreign_pool(Arc::clone(&real));

        let a = MemoryConsumer::new("a").register(&foreign);
        let b = MemoryConsumer::new("b").register(&foreign);

        a.grow(300);
        b.grow(200);
        assert_eq!(real.reserved(), 500);

        drop(a);
        assert_eq!(real.reserved(), 200);
        drop(b);
        assert_eq!(real.reserved(), 0);
    }

    #[test]
    fn shrink_beyond_size_does_not_panic() {
        let real = Arc::new(GreedyMemoryPool::new(1_000)) as Arc<dyn MemoryPool>;
        let foreign = foreign_pool(Arc::clone(&real));

        let ffi = FFI_MemoryPool::new(Arc::clone(&real));
        // Grow by 10 then ask the wrapper to shrink by far more. A panic here
        // would be undefined behaviour across a real FFI boundary.
        unsafe {
            (ffi.register)(&ffi, 42, "clamped".into(), false);
            (ffi.grow)(&ffi, 42, 10);
            (ffi.shrink)(&ffi, 42, 10_000);
        }
        assert_eq!(real.reserved(), 0);

        drop(foreign);
    }

    #[test]
    fn local_pool_is_unwrapped() {
        let original = Arc::new(GreedyMemoryPool::new(1_000)) as Arc<dyn MemoryPool>;
        let ffi = FFI_MemoryPool::new(Arc::clone(&original));

        let recovered = ffi.as_local().expect("local pool should unwrap");
        assert!(Arc::ptr_eq(&original, &recovered));
    }

    #[test]
    fn try_grow_result_round_trip() {
        let exhausted: Result<()> = FFI_TryGrowResult::from(Err::<(), _>(
            DataFusionError::ResourcesExhausted("over budget".to_string()),
        ))
        .into();
        match exhausted.unwrap_err() {
            DataFusionError::ResourcesExhausted(msg) => assert_eq!(msg, "over budget"),
            other => panic!("expected ResourcesExhausted, got {other:?}"),
        }

        let ok: Result<()> =
            FFI_TryGrowResult::from(Ok::<(), DataFusionError>(())).into();
        assert!(ok.is_ok());
    }
}
