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

//! Shared styling for benchmark runner clap help and formatted output.

use clap::builder::styling::{AnsiColor, Style, Styles};
use std::fmt::Display;

/// Shared clap color palette for benchmark runner help output.
pub(crate) const HELP_STYLES: Styles = Styles::styled()
    .header(AnsiColor::Green.on_default().bold())
    .usage(AnsiColor::Green.on_default().bold())
    .literal(AnsiColor::Cyan.on_default().bold())
    .placeholder(AnsiColor::Cyan.on_default());

/// Formats section headings with the runner's heading color.
pub(crate) fn header(text: impl Display) -> String {
    styled(AnsiColor::Green.on_default().bold(), text)
}

/// Formats command-line flags, commands, and other literal text.
pub(crate) fn literal(text: impl Display) -> String {
    styled(AnsiColor::Cyan.on_default().bold(), text)
}

/// Formats placeholder names such as SQL replacement variables.
pub(crate) fn placeholder(text: impl Display) -> String {
    styled(AnsiColor::Cyan.on_default(), text)
}

/// Formats selected values in informational output.
pub(crate) fn value(text: impl Display) -> String {
    styled(AnsiColor::Green.on_default(), text)
}

/// Formats field labels in informational output.
pub(crate) fn label(text: impl Display) -> String {
    styled(Style::new().bold(), text)
}

/// Applies one clap style and resets it after the text.
fn styled(style: Style, text: impl Display) -> String {
    format!("{style}{text}{style:#}")
}
