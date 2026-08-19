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

//! Parsing of `# configMatrix:` directives in SLT files.
//!
//! A directive has the form:
//!
//! ```text
//! # configMatrix: <key>=<v1>,<v2>[,<v3>...]
//! ```
//!
//! Multiple directives combine as a cartesian product. The runner will
//! execute the whole file once per combination, applying the given values to
//! `SessionContext` config before each run.

use std::fs;
use std::path::Path;

use datafusion::common::{DataFusionError, Result, exec_datafusion_err};
use itertools::Itertools;

/// Comment prefix that marks a config-matrix directive. Case-insensitive
/// on the marker itself; the key/values are left untouched.
const DIRECTIVE_MARKER: &str = "configmatrix:";

/// A single `configMatrix` directive: one key and the list of values that
/// should be swept over.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigMatrix {
    pub key: String,
    pub values: Vec<String>,
}

/// A single point in the matrix, expressed as an ordered list of
/// `(key, value)` pairs to apply before running the file.
pub type ConfigMatrixCombination = Vec<(String, String)>;

/// Read the file at `path` and extract all `# configMatrix:` directives.
///
/// Returns an empty vec if no directives are present. Fails with a
/// user-friendly error if a directive is malformed (missing key, missing
/// values, empty value list).
pub fn parse_config_matrix_from_file(path: &Path) -> Result<Vec<ConfigMatrix>> {
    let content = fs::read_to_string(path).map_err(|e| {
        exec_datafusion_err!(
            "Failed to read {} for configMatrix parsing: {e}",
            path.display()
        )
    })?;
    parse_config_matrix(&content, path)
}

/// Parse `# configMatrix:` directives out of the given file contents.
///
/// `path` is used only for error messages.
///
/// Values inside a directive are deduplicated (first occurrence wins).
/// Directives that reuse a key are merged into a single matrix entry, again
/// preserving first-seen order of both keys and values. This makes a
/// repeated key idempotent instead of silently multiplying the run count.
pub fn parse_config_matrix(content: &str, path: &Path) -> Result<Vec<ConfigMatrix>> {
    let mut result: Vec<ConfigMatrix> = Vec::new();
    for (idx, line) in content.lines().enumerate() {
        let Some(rest) = strip_directive_prefix(line) else {
            continue;
        };
        let line_num = idx + 1;
        let (key, values_str) = rest.split_once('=').ok_or_else(|| {
            DataFusionError::Configuration(format!(
                "Invalid configMatrix directive in {}:{}: expected \
                 `# configMatrix: <key>=<v1>,<v2>[,...]`, got `{}`",
                path.display(),
                line_num,
                rest
            ))
        })?;

        let key = key.trim().to_string();
        if key.is_empty() {
            return Err(DataFusionError::Configuration(format!(
                "Invalid configMatrix directive in {}:{}: missing config key",
                path.display(),
                line_num
            )));
        }

        let values: Vec<String> = values_str
            .split(',')
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .unique()
            .collect();
        if values.is_empty() {
            return Err(DataFusionError::Configuration(format!(
                "Invalid configMatrix directive in {}:{}: no values provided for `{}`",
                path.display(),
                line_num,
                key
            )));
        }

        // Merge into any earlier entry for the same key so a repeated
        // directive is idempotent (union of values, first-seen order).
        if let Some(existing) = result.iter_mut().find(|m| m.key == key) {
            for v in values {
                if !existing.values.contains(&v) {
                    existing.values.push(v);
                }
            }
        } else {
            result.push(ConfigMatrix { key, values });
        }
    }
    Ok(result)
}

/// Strip a leading comment marker and any `configMatrix:` prefix (case
/// insensitive). Returns the remainder if the line is a directive,
/// otherwise `None`.
fn strip_directive_prefix(line: &str) -> Option<&str> {
    // Trim whitespace, then require at least one `#` to treat this as a
    // comment line (matches sqllogictest's own comment rule).
    let trimmed = line.trim_start();
    let after_hash = trimmed.strip_prefix('#')?;
    let after_hash = after_hash.trim_start_matches('#').trim_start();

    // Case-insensitive match of the marker prefix. We only lowercase the
    // relevant slice — the key/values that follow keep their original case.
    let (marker, rest) = after_hash.split_at_checked(DIRECTIVE_MARKER.len())?;
    if !marker.eq_ignore_ascii_case(DIRECTIVE_MARKER) {
        return None;
    }
    Some(rest.trim())
}

/// Return the cartesian product of all matrix values.
///
/// Empty input yields an empty vec (i.e. "run once, no matrix applied"
/// remains the caller's responsibility to detect via `matrices.is_empty()`).
pub fn iter_matrix_combinations(
    matrices: &[ConfigMatrix],
) -> Vec<ConfigMatrixCombination> {
    if matrices.is_empty() {
        return Vec::new();
    }
    matrices
        .iter()
        .map(|m| m.values.iter().map(|v| (m.key.clone(), v.clone())))
        .multi_cartesian_product()
        .collect()
}

/// Render a combination as `k1=v1, k2=v2` for inclusion in error messages
/// and progress bars.
pub fn describe_combination(combo: &[(String, String)]) -> String {
    combo.iter().map(|(k, v)| format!("{k}={v}")).join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn p() -> PathBuf {
        PathBuf::from("test.slt")
    }

    #[test]
    fn parses_no_directives() {
        let content = "# ordinary comment\nquery I\nSELECT 1\n----\n1\n";
        assert!(parse_config_matrix(content, &p()).unwrap().is_empty());
    }

    #[test]
    fn parses_single_directive() {
        let content = "# configMatrix: datafusion.optimizer.enable_piecewise_merge_join=true,false\n";
        let matrices = parse_config_matrix(content, &p()).unwrap();
        assert_eq!(matrices.len(), 1);
        assert_eq!(
            matrices[0].key,
            "datafusion.optimizer.enable_piecewise_merge_join"
        );
        assert_eq!(matrices[0].values, vec!["true", "false"]);
    }

    #[test]
    fn parses_case_insensitive_marker() {
        let content = "## CONFIGMATRIX: datafusion.execution.batch_size=1024,4096\n";
        let matrices = parse_config_matrix(content, &p()).unwrap();
        assert_eq!(matrices.len(), 1);
        assert_eq!(matrices[0].key, "datafusion.execution.batch_size");
    }

    #[test]
    fn parses_multiple_directives_and_computes_cartesian_product() {
        let content = "\
# configMatrix: a=1,2\n\
# configMatrix: b=x,y,z\n\
";
        let matrices = parse_config_matrix(content, &p()).unwrap();
        let combos = iter_matrix_combinations(&matrices);
        assert_eq!(combos.len(), 6);
        // First matrix drives the outer loop; every value of `a` pairs with
        // every value of `b`.
        assert_eq!(
            combos[0],
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "x".to_string())
            ]
        );
        assert_eq!(
            combos[5],
            vec![
                ("a".to_string(), "2".to_string()),
                ("b".to_string(), "z".to_string())
            ]
        );
    }

    #[test]
    fn rejects_missing_equals() {
        let content = "# configMatrix: bogus_no_equals\n";
        let err = parse_config_matrix(content, &p()).unwrap_err();
        assert!(err.to_string().contains("<key>=<v1>"));
    }

    #[test]
    fn rejects_missing_key() {
        let content = "# configMatrix: =true,false\n";
        let err = parse_config_matrix(content, &p()).unwrap_err();
        assert!(err.to_string().contains("missing config key"));
    }

    #[test]
    fn rejects_empty_values() {
        let content = "# configMatrix: some.key=\n";
        let err = parse_config_matrix(content, &p()).unwrap_err();
        assert!(err.to_string().contains("no values provided"));
    }

    #[test]
    fn empty_matrix_yields_empty_combinations() {
        assert!(iter_matrix_combinations(&[]).is_empty());
    }

    #[test]
    fn deduplicates_repeated_values_preserving_first_order() {
        let content = "# configMatrix: some.key=false,true,false,true,false\n";
        let matrices = parse_config_matrix(content, &p()).unwrap();
        assert_eq!(matrices.len(), 1);
        assert_eq!(matrices[0].values, vec!["false", "true"]);
    }

    #[test]
    fn strips_whitespace_around_values() {
        // Every value is written with different padding (spaces, tabs,
        // leading and trailing). The parser must return them clean.
        let content = "# configMatrix: k= 1024 ,\t2048  , \t4096\n";
        let matrices = parse_config_matrix(content, &p()).unwrap();
        assert_eq!(matrices[0].values, vec!["1024", "2048", "4096"]);
    }

    #[test]
    fn strips_whitespace_around_key_and_around_equals() {
        // Padding on both sides of the `=` and inside the values list.
        let content =
            "#   configMatrix:   datafusion.execution.batch_size  =  1024 , 2048  \n";
        let matrices = parse_config_matrix(content, &p()).unwrap();
        assert_eq!(matrices.len(), 1);
        assert_eq!(matrices[0].key, "datafusion.execution.batch_size");
        assert_eq!(matrices[0].values, vec!["1024", "2048"]);
    }

    #[test]
    fn dedup_runs_after_whitespace_is_stripped() {
        // `1024` and `1024 ` must be treated as the same value, i.e. the
        // dedup step sees the trimmed forms.
        let content = "# configMatrix: k= 1024 , 1024,\t1024  \n";
        let matrices = parse_config_matrix(content, &p()).unwrap();
        assert_eq!(matrices[0].values, vec!["1024"]);
    }

    #[test]
    fn trailing_and_repeated_commas_are_ignored() {
        // Empty-after-trim entries (trailing comma, double comma) drop out.
        let content = "# configMatrix: k=1024,,2048,\n";
        let matrices = parse_config_matrix(content, &p()).unwrap();
        assert_eq!(matrices[0].values, vec!["1024", "2048"]);
    }

    #[test]
    fn merges_repeated_directives_for_same_key() {
        // Same directive typed twice should behave as if it were typed once.
        let content = "\
# configMatrix: datafusion.execution.batch_size=1024,2048\n\
# configMatrix: datafusion.execution.batch_size=1024,2048\n\
";
        let matrices = parse_config_matrix(content, &p()).unwrap();
        assert_eq!(matrices.len(), 1);
        assert_eq!(matrices[0].values, vec!["1024", "2048"]);
        assert_eq!(iter_matrix_combinations(&matrices).len(), 2);
    }

    #[test]
    fn merges_repeated_directives_unioning_values() {
        // Overlapping value lists on the same key merge into the union
        // while preserving first-seen order.
        let content = "\
# configMatrix: k=1024,2048\n\
# configMatrix: k=2048,4096\n\
";
        let matrices = parse_config_matrix(content, &p()).unwrap();
        assert_eq!(matrices.len(), 1);
        assert_eq!(matrices[0].values, vec!["1024", "2048", "4096"]);
    }

    #[test]
    fn nested_matrices_expand_as_cartesian_product() {
        // Two directives with different keys — the exact shape from the
        // user request. 2 x 2 = 4 combinations, all four keys/value pairs
        // appear in every combination.
        let content = "\
# configMatrix: datafusion.execution.batch_size=1024,2048\n\
# configMatrix: datafusion.execution.param1=true,false\n\
";
        let matrices = parse_config_matrix(content, &p()).unwrap();
        assert_eq!(matrices.len(), 2);
        let combos = iter_matrix_combinations(&matrices);
        assert_eq!(combos.len(), 4);
        for combo in &combos {
            assert_eq!(combo.len(), 2);
            assert_eq!(combo[0].0, "datafusion.execution.batch_size");
            assert_eq!(combo[1].0, "datafusion.execution.param1");
        }
        assert_eq!(
            combos[0],
            vec![
                (
                    "datafusion.execution.batch_size".to_string(),
                    "1024".to_string(),
                ),
                (
                    "datafusion.execution.param1".to_string(),
                    "true".to_string(),
                ),
            ]
        );
        assert_eq!(
            combos[3],
            vec![
                (
                    "datafusion.execution.batch_size".to_string(),
                    "2048".to_string(),
                ),
                (
                    "datafusion.execution.param1".to_string(),
                    "false".to_string(),
                ),
            ]
        );
    }

    #[test]
    fn ignores_non_comment_lines_with_marker_text() {
        // Not a comment - must not be picked up as a directive.
        let content = "SELECT 'configMatrix: foo=bar';\n";
        assert!(parse_config_matrix(content, &p()).unwrap().is_empty());
    }

    #[test]
    fn describe_combination_formats_key_value_pairs() {
        let combo = vec![
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "x".to_string()),
        ];
        assert_eq!(describe_combination(&combo), "a=1, b=x");
    }
}
