// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Decides how many times a benchmark runs one query.
//!
//! | `--max-time` | Behavior                                                    |
//! |--------------|-------------------------------------------------------------|
//! | not set      | Run exactly `--iterations` times.                           |
//! | set          | Run at least `--iterations` times, then stop at the first   |
//! |              | of: the median converges to `--target-precision`, the query |
//! |              | used `--max-time` seconds, or `--max-iterations` runs.      |
//!
//! There is no warmup. A warmup run of a slow query costs as much as a
//! measured run, and the convergence check already absorbs a slow first run.

use datafusion_common::instant::Instant;
use std::time::Duration;

/// Fewest samples for which [`median_ci_rel_half_width`] gives a 95% interval
/// narrower than the full sample range.
const MIN_SAMPLES_FOR_CI: usize = 6;

/// Controls the iteration loop of one query. See the [module docs](self).
///
/// ```ignore
/// let mut sampler = common.sampler();
/// while let Some(i) = sampler.next_iteration() {
///     let start = Instant::now();
///     run_query().await?;
///     sampler.record(start.elapsed());
/// }
/// ```
#[derive(Debug)]
pub struct Sampler {
    min_iterations: usize,
    max_iterations: usize,
    adaptive: Option<Adaptive>,
    started: Instant,
    samples: Vec<f64>,
}

#[derive(Debug, Clone, Copy)]
struct Adaptive {
    budget: Duration,
    target_precision: f64,
}

impl Sampler {
    /// A sampler that runs exactly `iterations` times.
    pub fn fixed(iterations: usize) -> Self {
        Self {
            min_iterations: iterations,
            max_iterations: iterations,
            adaptive: None,
            started: Instant::now(),
            samples: Vec::with_capacity(iterations),
        }
    }

    /// A sampler that runs between `min_iterations` and `max_iterations`
    /// times. It stops early when the relative half-width of the 95%
    /// confidence interval of the median is at most `target_precision`, or
    /// when `budget` has passed since the first iteration started.
    pub fn adaptive(
        min_iterations: usize,
        max_iterations: usize,
        budget: Duration,
        target_precision: f64,
    ) -> Self {
        Self {
            min_iterations,
            max_iterations: max_iterations.max(min_iterations),
            adaptive: Some(Adaptive {
                budget,
                target_precision,
            }),
            started: Instant::now(),
            samples: vec![],
        }
    }

    /// Returns the index of the next iteration to run, or `None` to stop.
    pub fn next_iteration(&mut self) -> Option<usize> {
        let n = self.samples.len();
        if n == 0 {
            self.started = Instant::now();
        }
        if n < self.min_iterations {
            return Some(n);
        }
        if n >= self.max_iterations {
            return None;
        }
        let adaptive = self.adaptive?;
        if self.started.elapsed() >= adaptive.budget {
            return None;
        }
        match median_ci_rel_half_width(&self.samples) {
            Some(width) if width <= adaptive.target_precision => None,
            _ => Some(n),
        }
    }

    /// Records the duration of the iteration that [`Self::next_iteration`]
    /// last returned.
    pub fn record(&mut self, elapsed: Duration) {
        self.samples.push(elapsed.as_secs_f64());
    }
}

/// Returns the half-width of a distribution-free 95% confidence interval of
/// the median, relative to the median. Returns `None` for fewer than
/// [`MIN_SAMPLES_FOR_CI`] samples.
///
/// The interval uses order statistics: the count of samples below the true
/// median is Binomial(n, 0.5), so the ranks `n/2 ± 0.98·√n` bound it with
/// about 95% probability. Unlike a mean ± stddev interval, this does not
/// assume normal timings, and one slow outlier does not widen it.
fn median_ci_rel_half_width(samples: &[f64]) -> Option<f64> {
    let n = samples.len();
    if n < MIN_SAMPLES_FOR_CI {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let half = n as f64 / 2.0;
    let spread = 0.98 * (n as f64).sqrt();
    let lo = (half - spread).floor().max(0.0) as usize;
    let hi = ((half + spread).ceil() as usize).min(n - 1);
    let median = if n.is_multiple_of(2) {
        f64::midpoint(sorted[n / 2 - 1], sorted[n / 2])
    } else {
        sorted[n / 2]
    };
    if median <= 0.0 {
        return Some(0.0);
    }
    Some((sorted[hi] - sorted[lo]) / 2.0 / median)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(sampler: &mut Sampler, mut duration: impl FnMut(usize) -> f64) -> usize {
        while let Some(i) = sampler.next_iteration() {
            sampler.record(Duration::from_secs_f64(duration(i)));
        }
        sampler.samples.len()
    }

    #[test]
    fn fixed_runs_exact_count() {
        assert_eq!(run(&mut Sampler::fixed(5), |_| 1.0), 5);
        assert_eq!(run(&mut Sampler::fixed(0), |_| 1.0), 0);
    }

    #[test]
    fn adaptive_stops_at_convergence() {
        let mut sampler = Sampler::adaptive(3, 1000, Duration::from_secs(3600), 0.02);
        // Identical timings converge as soon as the interval is defined.
        assert_eq!(run(&mut sampler, |_| 0.01), MIN_SAMPLES_FOR_CI);
    }

    #[test]
    fn adaptive_respects_min_iterations() {
        let mut sampler = Sampler::adaptive(10, 1000, Duration::from_secs(3600), 0.02);
        assert_eq!(run(&mut sampler, |_| 0.01), 10);
    }

    #[test]
    fn adaptive_stops_at_max_iterations_when_noisy() {
        let mut sampler = Sampler::adaptive(3, 50, Duration::from_secs(3600), 0.0001);
        // Alternating 1x / 3x timings never converge to 0.01%.
        assert_eq!(run(&mut sampler, |i| [0.001, 0.003][i % 2]), 50);
    }

    #[test]
    fn adaptive_stops_at_budget() {
        let mut sampler = Sampler::adaptive(3, 1000, Duration::ZERO, 0.0001);
        assert_eq!(run(&mut sampler, |i| [0.001, 0.003][i % 2]), 3);
    }

    #[test]
    fn noisy_timings_need_more_samples() {
        let noisy = |i: usize| 1.0 + 0.3 * ((i * 7919) % 11) as f64 / 10.0;
        let mut sampler = Sampler::adaptive(3, 10_000, Duration::from_secs(3600), 0.02);
        let n = run(&mut sampler, noisy);
        assert!(n > 50, "converged after only {n} samples");
        assert!(n < 10_000, "never converged");
    }

    #[test]
    fn ci_width_values() {
        assert_eq!(median_ci_rel_half_width(&[1.0; 5]), None);
        assert_eq!(median_ci_rel_half_width(&[1.0; 6]), Some(0.0));
        // n = 6: ranks floor(3 - 2.4) = 0 and ceil(3 + 2.4) = 6 -> 5.
        let samples = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
        assert_eq!(
            median_ci_rel_half_width(&samples),
            Some((6.0 - 1.0) / 2.0 / 3.5)
        );
    }
}
