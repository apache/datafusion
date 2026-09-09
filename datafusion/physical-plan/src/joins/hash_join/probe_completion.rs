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

//! Completion protocol shared by the probe partitions of a hash join.
//!
//! The probe partitions of one `HashJoinExec` run concurrently over a shared
//! build side. Two facts have to cross partition boundaries:
//!
//! - which partition emits the build-side rows in the final stage, namely the
//!   last one to finish, and
//! - what the partitions collectively saw, which the null-aware `NOT IN` rules
//!   need: did any partition see a row, and did any see a NULL join key.
//!
//! [`ProbeCompletion`] owns both, because reading the second without ordering
//! it against the first produced wrong results. A partition that read the NULL
//! flag *before* decrementing the counter could observe `false`, have a
//! sibling record a NULL and finish, then become the last partition itself and
//! emit build rows that `NOT IN` must suppress.
//!
//! The invariant is therefore: **whichever caller observes itself to be last
//! sees every fact recorded by every other partition.** It rests on two
//! properties, and the type is shaped to keep both.
//!
//! 1. *Order.* The facts are reachable only through the value returned by
//!    [`ProbeCompletion::report_completed`], so no caller can read them before
//!    its own decrement.
//! 2. *Visibility.* That decrement is `AcqRel`, so the last partition
//!    synchronizes with the release of every partition that finished before
//!    it. Were the counter `Relaxed`, nothing would order a sibling's store
//!    before the final load even when the decrement happened first.
//!
//! # Testing
//!
//! Neither property can be pinned down by an ordinary concurrent test. The
//! window is a few instructions wide, and on x86 a `Relaxed` decrement lowers
//! to the same instruction as an `AcqRel` one, so a stress test would pass on
//! the broken version. The invariant is instead model-checked with [loom],
//! which enumerates the thread interleavings *and* the store visibility the
//! memory model permits, in `loom_tests` below.
//!
//! Because loom can only explore its own atomic types, the protocol is written
//! once in [`define_probe_completion`] and instantiated twice: over
//! [`std::sync::atomic`] for the real join, and over `loom::sync::atomic` for
//! the model. Both instantiations share this single copy of the orderings and
//! the call sequence, so weakening the decrement to `Relaxed`, or reading the
//! facts before it, fails the model. That is what makes these tests regression
//! coverage rather than a restatement of the fix.
//!
//! [loom]: https://docs.rs/loom

use std::sync::atomic::Ordering;

/// What every probe partition together saw, as observed by the last one to
/// finish.
///
/// Obtainable only from [`ProbeCompletion::report_completed`], which is what
/// stops the final stage from reading the shared state too early.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProbeSideSummary {
    /// Some probe partition saw a row. An entirely empty probe side is not the
    /// same as one that matched nothing: `NULL NOT IN (empty)` is TRUE.
    pub(super) non_empty: bool,
    /// Some probe partition saw a NULL join key.
    pub(super) has_null: bool,
}

/// Defines [`ProbeCompletion`] over a given pair of atomic types.
///
/// The indirection exists so the loom model below can instantiate the very
/// same logic over loom's atomics. See the [module docs](self); do not add a
/// second copy of these orderings anywhere.
macro_rules! define_probe_completion {
    ($atomic_usize:ty, $atomic_bool:ty) => {
        /// Tracks how many probe partitions are still running, together with
        /// the null-aware facts they contribute.
        ///
        /// See the [module docs](self) for the invariant this upholds.
        #[derive(Debug)]
        pub(super) struct ProbeCompletion {
            /// Probe partitions that have not finished yet.
            running: $atomic_usize,
            /// Set once any probe partition has seen a row.
            saw_row: $atomic_bool,
            /// Set once any probe partition has seen a NULL join key.
            saw_null_key: $atomic_bool,
        }

        impl ProbeCompletion {
            /// Creates the protocol state for `probe_threads` partitions.
            pub(super) fn new(probe_threads: usize) -> Self {
                Self {
                    running: <$atomic_usize>::new(probe_threads),
                    saw_row: <$atomic_bool>::new(false),
                    saw_null_key: <$atomic_bool>::new(false),
                }
            }

            /// Records what one probe partition saw in one batch.
            ///
            /// `Relaxed` suffices: the `AcqRel` decrement in
            /// [`Self::report_completed`] publishes these stores to the last
            /// partition.
            pub(super) fn record_batch(&self, non_empty: bool, has_null: bool) {
                if non_empty {
                    self.saw_row.store(true, Ordering::Relaxed);
                }
                if has_null {
                    self.saw_null_key.store(true, Ordering::Relaxed);
                }
            }

            /// Whether some partition has already recorded a NULL join key.
            ///
            /// A hint for skipping work during the probe phase, which may lag
            /// behind sibling partitions. It must never decide what the final
            /// stage emits; use the [`ProbeSideSummary`] from
            /// [`Self::report_completed`] for that.
            pub(super) fn saw_null_key_hint(&self) -> bool {
                self.saw_null_key.load(Ordering::Relaxed)
            }

            /// Marks the calling partition finished, returning `Some` only for
            /// the last one, together with what all partitions saw.
            ///
            /// The `AcqRel` decrement publishes this partition's own stores
            /// and acquires those of the partitions that finished earlier, so
            /// the summary handed to the last caller is complete.
            pub(super) fn report_completed(&self) -> Option<ProbeSideSummary> {
                let was_last = self.running.fetch_sub(1, Ordering::AcqRel) == 1;
                was_last.then(|| ProbeSideSummary {
                    non_empty: self.saw_row.load(Ordering::Relaxed),
                    has_null: self.saw_null_key.load(Ordering::Relaxed),
                })
            }
        }
    };
}

define_probe_completion!(
    std::sync::atomic::AtomicUsize,
    std::sync::atomic::AtomicBool
);

#[cfg(test)]
mod tests {
    use super::*;

    /// The sequential contract: only the last caller is handed the summary,
    /// and it carries what the earlier partitions recorded.
    ///
    /// The concurrent invariant is covered by `loom_tests` instead.
    #[test]
    fn only_the_last_partition_receives_the_summary() {
        let completion = ProbeCompletion::new(3);

        completion.record_batch(true, false);
        assert_eq!(completion.report_completed(), None);

        completion.record_batch(true, true);
        assert_eq!(completion.report_completed(), None);

        assert_eq!(
            completion.report_completed(),
            Some(ProbeSideSummary {
                non_empty: true,
                has_null: true,
            })
        );
    }

    /// A probe side that never saw a row leaves both facts clear, which is how
    /// `NULL NOT IN (empty)` stays TRUE.
    #[test]
    fn an_untouched_probe_side_reports_nothing_seen() {
        let completion = ProbeCompletion::new(1);

        assert_eq!(
            completion.report_completed(),
            Some(ProbeSideSummary {
                non_empty: false,
                has_null: false,
            })
        );
    }

    /// The hint may lag, but it must never report a NULL that was not
    /// recorded.
    #[test]
    fn the_hint_reflects_recorded_nulls() {
        let completion = ProbeCompletion::new(1);
        assert!(!completion.saw_null_key_hint());

        completion.record_batch(true, false);
        assert!(!completion.saw_null_key_hint());

        completion.record_batch(true, true);
        assert!(completion.saw_null_key_hint());
    }
}

/// Model-checked concurrency tests for the protocol.
///
/// These instantiate the protocol over loom's atomics (see the
/// [module docs](self)) and let loom enumerate the interleavings and store
/// visibility the memory model allows. They fail if the decrement in
/// `report_completed` is weakened to `Relaxed`.
#[cfg(test)]
mod loom_tests {
    use super::ProbeSideSummary;
    use loom::sync::Arc;
    use std::sync::atomic::Ordering;

    define_probe_completion!(
        loom::sync::atomic::AtomicUsize,
        loom::sync::atomic::AtomicBool
    );

    /// The invariant behind the fix: whichever partition observes itself to be
    /// last sees the NULL its sibling recorded.
    ///
    /// This is the regression test for the wrong-results bug. One partition
    /// records a NULL and finishes while the other finishes having recorded
    /// nothing, and the last one must not conclude that no NULL was seen.
    #[test]
    fn the_last_partition_observes_a_sibling_null() {
        loom::model(|| {
            let completion = Arc::new(ProbeCompletion::new(2));

            let recorder = {
                let completion = Arc::clone(&completion);
                loom::thread::spawn(move || {
                    completion.record_batch(true, true);
                    completion.report_completed()
                })
            };

            let observer = completion.report_completed();
            let recorded = recorder.join().unwrap();

            let summary = match (recorded, observer) {
                (Some(summary), None) | (None, Some(summary)) => summary,
                (Some(_), Some(_)) => panic!("two partitions both finished last"),
                (None, None) => panic!("no partition finished last"),
            };

            assert!(
                summary.has_null,
                "the last partition missed the NULL its sibling recorded"
            );
            assert!(
                summary.non_empty,
                "the last partition missed the row its sibling recorded"
            );
        });
    }

    /// With both partitions recording, the summary must be the union of what
    /// they saw, whichever finishes last.
    #[test]
    fn the_summary_unions_what_every_partition_recorded() {
        loom::model(|| {
            let completion = Arc::new(ProbeCompletion::new(2));

            let other = {
                let completion = Arc::clone(&completion);
                loom::thread::spawn(move || {
                    // Rows, but no NULL key.
                    completion.record_batch(true, false);
                    completion.report_completed()
                })
            };

            // A NULL key in an otherwise empty batch.
            completion.record_batch(false, true);
            let here = completion.report_completed();
            let there = other.join().unwrap();

            let summary = here.or(there).expect("some partition must finish last");
            assert_eq!(
                summary,
                ProbeSideSummary {
                    non_empty: true,
                    has_null: true,
                },
                "the summary lost a fact one of the partitions recorded"
            );
        });
    }

    /// Exactly one partition may take the final stage. Two would emit the
    /// build side twice; none would drop it.
    #[test]
    fn exactly_one_partition_finishes_last() {
        loom::model(|| {
            let completion = Arc::new(ProbeCompletion::new(2));

            let other = {
                let completion = Arc::clone(&completion);
                loom::thread::spawn(move || completion.report_completed().is_some())
            };

            let here = completion.report_completed().is_some();
            let there = other.join().unwrap();

            assert!(here ^ there, "exactly one partition must finish last");
        });
    }

    /// The probe-phase hint never invents a NULL: if no partition recorded
    /// one, no observation of the hint may report one.
    #[test]
    fn the_hint_never_reports_an_unrecorded_null() {
        loom::model(|| {
            let completion = Arc::new(ProbeCompletion::new(2));

            let other = {
                let completion = Arc::clone(&completion);
                loom::thread::spawn(move || {
                    completion.record_batch(true, false);
                    completion.saw_null_key_hint()
                })
            };

            let here = completion.saw_null_key_hint();
            let there = other.join().unwrap();

            assert!(!here && !there, "the hint reported an unrecorded NULL");
        });
    }
}
