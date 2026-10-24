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

//! Named secret values shared by session APIs and logical plans.

use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};

/// Options associated with a named secret.
///
/// The [`Debug`] representation omits all option names and values. Applications
/// that are authorized to use the secret can explicitly access its options with
/// [`Self::options`] or [`Self::get`]. Option names are case-sensitive.
///
/// This type stores ordinary strings in memory: it does not encrypt or zeroize
/// them, and cloning the value copies those strings. It is not an access-control
/// boundary. Applications must restrict access to the session and its secrets.
#[derive(Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Secret {
    options: BTreeMap<String, String>,
}

impl Secret {
    /// Create a secret from its options.
    pub fn new(options: BTreeMap<String, String>) -> Self {
        Self { options }
    }

    /// Return the options, including their plaintext values.
    pub fn options(&self) -> &BTreeMap<String, String> {
        &self.options
    }

    /// Return the plaintext value for an exact, case-sensitive option name.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.options.get(name).map(String::as_str)
    }
}

impl Debug for Secret {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Secret")
            .field("options", &"<redacted>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_are_explicit_and_case_sensitive() {
        let options = BTreeMap::from([
            ("TOKEN".to_string(), "upper-value".to_string()),
            ("token".to_string(), "lower-value".to_string()),
        ]);
        let secret = Secret::new(options.clone());

        assert_eq!(secret.options(), &options);
        assert_eq!(secret.get("TOKEN"), Some("upper-value"));
        assert_eq!(secret.get("token"), Some("lower-value"));
        assert_eq!(secret.get("Token"), None);
        assert_eq!(secret.clone(), secret);
    }

    #[test]
    fn debug_redacts_all_option_names_and_values() {
        let secret = Secret::new(BTreeMap::from([(
            "private-option-canary".to_string(),
            "private-value-canary".to_string(),
        )]));

        for debug in [format!("{secret:?}"), format!("{secret:#?}")] {
            assert!(debug.contains("redacted"));
            assert!(!debug.contains("private-option-canary"));
            assert!(!debug.contains("private-value-canary"));
        }
    }
}
