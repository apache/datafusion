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

//! Progress bar display functionality

use crate::progress::{ProgressInfo, ProgressStyle, ProgressUnit};
use datafusion_common::instant::Instant;
use std::io::{self, Write};
use std::time::Duration;

/// Displays progress information to the terminal
pub struct ProgressDisplay {
    style: ProgressStyle,
    start_time: Instant,
    last_display: Option<String>,
}

impl ProgressDisplay {
    pub fn new(style: ProgressStyle) -> Self {
        Self {
            style,
            start_time: Instant::now(),
            last_display: None,
        }
    }

    /// Update the progress display
    pub fn update(&mut self, progress: &ProgressInfo, eta: Option<Duration>) {
        let display_text =
            match progress.percent.is_some() && self.style == ProgressStyle::Bar {
                true => self.format_progress_bar(progress, eta),
                false => self.format_spinner(progress),
            };

        // Only update if the display text has changed
        if self.last_display.as_ref() != Some(&display_text) {
            self.print_line(&display_text);
            self.last_display = Some(display_text);
        }
    }

    /// Finish the progress display and clean up
    pub fn finish(&mut self) {
        if self.last_display.is_some() {
            // Clear the progress line and move to next line
            print!("\r\x1b[K");
            let _ = io::stdout().flush();
            self.last_display = None;
        }
    }

    /// Format a progress bar with percentage
    fn format_progress_bar(
        &self,
        progress: &ProgressInfo,
        eta: Option<Duration>,
    ) -> String {
        let percent = progress.percent.unwrap_or(0.0);
        let bar = self.create_bar(percent);

        let current_formatted = progress.unit.format_value(progress.current);
        let total_formatted = progress
            .total
            .map(|t| progress.unit.format_value(t))
            .unwrap_or_else(|| "?".to_string());

        let throughput = self.calculate_throughput(progress);
        let eta_text = eta
            .map(format_duration)
            .unwrap_or_else(|| "??:??".to_string());
        let elapsed = format_duration(self.start_time.elapsed());

        let base_format = format!(
            "\r{}  {:5.1}%  {} / {}  •  {}  •  ETA {} / {}",
            bar,
            percent,
            current_formatted,
            total_formatted,
            throughput,
            eta_text,
            elapsed
        );

        // Add phase information for better user experience
        match progress.phase {
            crate::progress::ExecutionPhase::Reading => base_format,
            crate::progress::ExecutionPhase::Processing => {
                format!("{} 🔄 sorting/joining data", base_format)
            }
            crate::progress::ExecutionPhase::Finalizing => {
                format!("{} ✨ finalizing results", base_format)
            }
        }
    }

    /// Format a spinner without percentage
    fn format_spinner(&self, progress: &ProgressInfo) -> String {
        let spinner = self.get_spinner_char();
        let current_formatted = progress.unit.format_value(progress.current);
        let elapsed = format_duration(self.start_time.elapsed());

        let base_format = format!(
            "\r{}  {}: {}  elapsed: {}",
            spinner,
            match progress.unit {
                ProgressUnit::Bytes => "bytes",
                ProgressUnit::Rows => "rows",
            },
            current_formatted,
            elapsed
        );

        // Add phase information for spinner mode too
        match progress.phase {
            crate::progress::ExecutionPhase::Reading => base_format,
            crate::progress::ExecutionPhase::Processing => {
                format!("{} (sorting/joining)", base_format)
            }
            crate::progress::ExecutionPhase::Finalizing => {
                format!("{} (finalizing)", base_format)
            }
        }
    }

    /// Create a visual progress bar
    fn create_bar(&self, percent: f64) -> String {
        const BAR_WIDTH: usize = 20;
        let filled = ((percent / 100.0) * BAR_WIDTH as f64) as usize;
        let empty = BAR_WIDTH - filled;

        let mut bar = String::with_capacity(BAR_WIDTH);

        // Full blocks
        for _ in 0..filled {
            bar.push('▉');
        }

        // Partial block if needed
        if filled < BAR_WIDTH {
            let partial_progress = (percent / 100.0) * BAR_WIDTH as f64 - filled as f64;
            if partial_progress > 0.0 {
                let partial_char = match (partial_progress * 8.0) as usize {
                    0 => '▏',
                    1 => '▎',
                    2 => '▍',
                    3 => '▌',
                    4 => '▋',
                    5 => '▊',
                    6 => '▉',
                    _ => '▉',
                };
                bar.push(partial_char);
            }
        }

        // Empty blocks
        for _ in 0..empty.saturating_sub(if filled < BAR_WIDTH && percent > 0.0 {
            1
        } else {
            0
        }) {
            bar.push('░');
        }

        bar
    }

    /// Get the current spinner character
    fn get_spinner_char(&self) -> char {
        const SPINNER_CHARS: &[char] =
            &['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];
        let index =
            (self.start_time.elapsed().as_millis() / 100) as usize % SPINNER_CHARS.len();
        SPINNER_CHARS[index]
    }

    /// Calculate throughput string
    fn calculate_throughput(&self, progress: &ProgressInfo) -> String {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed < 0.1 {
            return "-- /s".to_string();
        }

        let rate = progress.current as f64 / elapsed;
        match progress.unit {
            ProgressUnit::Bytes => format!("{}/s", format_bytes(rate as usize)),
            ProgressUnit::Rows => format!("{} rows/s", format_number(rate as usize)),
        }
    }

    /// Print a line, overwriting the previous one
    fn print_line(&self, text: &str) {
        print!("{}", text);
        let _ = io::stdout().flush();
    }
}

impl Drop for ProgressDisplay {
    fn drop(&mut self) {
        self.finish();
    }
}

/// Format a duration as MM:SS
fn format_duration(duration: Duration) -> String {
    let total_seconds = duration.as_secs();
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;
    format!("{:02}:{:02}", minutes, seconds)
}

/// Format bytes in human-readable form (helper function)
fn format_bytes(bytes: usize) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;

    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }

    if unit_index == 0 {
        format!("{} {}", bytes, UNITS[unit_index])
    } else {
        format!("{:.1} {}", size, UNITS[unit_index])
    }
}

/// Format a number with thousands separators (helper function)
fn format_number(num: usize) -> String {
    let s = num.to_string();
    let mut result = String::new();

    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, c);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::from_secs(0)), "00:00");
        assert_eq!(format_duration(Duration::from_secs(30)), "00:30");
        assert_eq!(format_duration(Duration::from_secs(90)), "01:30");
        assert_eq!(format_duration(Duration::from_secs(3665)), "61:05");
    }

    #[test]
    fn test_create_bar() {
        let display = ProgressDisplay::new(ProgressStyle::Bar);

        let bar_0 = display.create_bar(0.0);
        assert!(bar_0.chars().all(|c| c == '░'));

        let bar_100 = display.create_bar(100.0);
        assert!(bar_100.chars().all(|c| c == '▉'));

        let bar_50 = display.create_bar(50.0);
        assert!(bar_50.contains('▉') && bar_50.contains('░'));
    }
}
