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

const DIRECTIVE_MARKER: &str = "configmatrix:";

/// A single point in the matrix, expressed as an ordered list of
/// `(key, value)` pairs to apply before running the file.
pub type ConfigMatrixCombination = Vec<(String, String)>;

/// Read the file at `path`, extract all `# configMatrix:` directives, and
/// expand them into the cartesian product of `(key, value)` combinations.
///
/// Returns an empty vec if no directives are present. Fails with a
/// user-friendly error if a directive is malformed (missing key, missing
/// values, empty value list).
pub fn parse_config_matrix_from_file(
    path: &Path,
) -> Result<Vec<ConfigMatrixCombination>> {
    let content = fs::read_to_string(path).map_err(|e| {
        exec_datafusion_err!(
            "Failed to read {} for configMatrix parsing: {e}",
            path.display()
        )
    })?;
    parse_config_matrix(&content, path)
}

/// Render a combination as `[configMatrix: k1=v1, k2=v2]` for CI logs.
pub fn matrix_tag(combo: &[(String, String)]) -> String {
    format!(
        "[configMatrix: {}]",
        combo.iter().map(|(k, v)| format!("{k}={v}")).join(", ")
    )
}

/// Parse `# configMatrix:` directives out of the given file contents and
/// return the fully expanded cartesian product. `path` is used only for
/// error messages.
///
/// Values inside a directive are trimmed and deduplicated (first occurrence
/// wins). Directives that reuse a key are merged into a single dimension.
fn parse_config_matrix(
    content: &str,
    path: &Path,
) -> Result<Vec<ConfigMatrixCombination>> {
    let mut dims: Vec<(String, Vec<String>)> = Vec::new();
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
        if let Some((_, existing)) = dims.iter_mut().find(|(k, _)| k == &key) {
            for v in values {
                if !existing.contains(&v) {
                    existing.push(v);
                }
            }
        } else {
            dims.push((key, values));
        }
    }

    if dims.is_empty() {
        return Ok(Vec::new());
    }
    Ok(dims
        .into_iter()
        .map(|(k, vs)| {
            vs.into_iter()
                .map(move |v| (k.clone(), v))
                .collect::<Vec<_>>()
        })
        .multi_cartesian_product()
        .collect())
}

/// Strip a leading comment marker and any `configMatrix:` prefix (case
/// insensitive). Returns the remainder if the line is a directive.
fn strip_directive_prefix(line: &str) -> Option<&str> {
    let after_hash = line.trim_start().strip_prefix('#')?;
    let after_hash = after_hash.trim_start_matches('#').trim_start();

    // Case-insensitive match of the marker; key/values keep their original case.
    let (marker, rest) = after_hash.split_at_checked(DIRECTIVE_MARKER.len())?;
    if !marker.eq_ignore_ascii_case(DIRECTIVE_MARKER) {
        return None;
    }
    Some(rest.trim())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn p() -> PathBuf {
        PathBuf::from("test.slt")
    }

    fn parse(content: &str) -> Vec<ConfigMatrixCombination> {
        parse_config_matrix(content, &p()).unwrap()
    }

    #[test]
    fn parses_no_directives() {
        let content = "# ordinary comment\nquery I\nSELECT 1\n----\n1\n";
        assert!(parse(content).is_empty());
    }

    #[test]
    fn parses_single_directive() {
        let combos = parse(
            "# configMatrix: datafusion.optimizer.enable_piecewise_merge_join=true,false\n",
        );
        assert_eq!(combos.len(), 2);
        assert_eq!(
            combos[0],
            vec![(
                "datafusion.optimizer.enable_piecewise_merge_join".to_string(),
                "true".to_string(),
            )]
        );
        assert_eq!(
            combos[1],
            vec![(
                "datafusion.optimizer.enable_piecewise_merge_join".to_string(),
                "false".to_string(),
            )]
        );
    }

    #[test]
    fn parses_case_insensitive_marker() {
        let combos =
            parse("## CONFIGMATRIX: datafusion.execution.batch_size=1024,4096\n");
        assert_eq!(combos.len(), 2);
        assert_eq!(combos[0][0].0, "datafusion.execution.batch_size");
    }

    #[test]
    fn parses_multiple_directives_and_computes_cartesian_product() {
        let combos = parse("# configMatrix: a=1,2\n# configMatrix: b=x,y,z\n");
        assert_eq!(combos.len(), 6);
        assert_eq!(
            combos[0],
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "x".to_string()),
            ]
        );
        assert_eq!(
            combos[5],
            vec![
                ("a".to_string(), "2".to_string()),
                ("b".to_string(), "z".to_string()),
            ]
        );
    }

    #[test]
    fn rejects_missing_equals() {
        let err =
            parse_config_matrix("# configMatrix: bogus_no_equals\n", &p()).unwrap_err();
        assert!(err.to_string().contains("<key>=<v1>"));
    }

    #[test]
    fn rejects_missing_key() {
        let err = parse_config_matrix("# configMatrix: =true,false\n", &p()).unwrap_err();
        assert!(err.to_string().contains("missing config key"));
    }

    #[test]
    fn rejects_empty_values() {
        let err = parse_config_matrix("# configMatrix: some.key=\n", &p()).unwrap_err();
        assert!(err.to_string().contains("no values provided"));
    }

    #[test]
    fn deduplicates_repeated_values_preserving_first_order() {
        let combos = parse("# configMatrix: some.key=false,true,false,true,false\n");
        assert_eq!(combos.len(), 2);
        assert_eq!(combos[0][0].1, "false");
        assert_eq!(combos[1][0].1, "true");
    }

    #[test]
    fn strips_whitespace_around_values() {
        let combos = parse("# configMatrix: k= 1024 ,\t2048  , \t4096\n");
        let values: Vec<&str> = combos.iter().map(|c| c[0].1.as_str()).collect();
        assert_eq!(values, vec!["1024", "2048", "4096"]);
    }

    #[test]
    fn strips_whitespace_around_key_and_around_equals() {
        let combos = parse(
            "#   configMatrix:   datafusion.execution.batch_size  =  1024 , 2048  \n",
        );
        assert_eq!(combos.len(), 2);
        assert_eq!(combos[0][0].0, "datafusion.execution.batch_size");
    }

    #[test]
    fn dedup_runs_after_whitespace_is_stripped() {
        // `1024`, ` 1024`, and `\t1024  ` all normalize to the same value.
        let combos = parse("# configMatrix: k= 1024 , 1024,\t1024  \n");
        assert_eq!(combos.len(), 1);
        assert_eq!(combos[0][0].1, "1024");
    }

    #[test]
    fn trailing_and_repeated_commas_are_ignored() {
        let combos = parse("# configMatrix: k=1024,,2048,\n");
        let values: Vec<&str> = combos.iter().map(|c| c[0].1.as_str()).collect();
        assert_eq!(values, vec!["1024", "2048"]);
    }

    #[test]
    fn merges_repeated_directives_for_same_key() {
        let combos = parse(
            "# configMatrix: datafusion.execution.batch_size=1024,2048\n\
             # configMatrix: datafusion.execution.batch_size=1024,2048\n",
        );
        assert_eq!(combos.len(), 2);
    }

    #[test]
    fn merges_repeated_directives_unioning_values() {
        let combos = parse("# configMatrix: k=1024,2048\n# configMatrix: k=2048,4096\n");
        let values: Vec<&str> = combos.iter().map(|c| c[0].1.as_str()).collect();
        assert_eq!(values, vec!["1024", "2048", "4096"]);
    }

    #[test]
    fn nested_matrices_expand_as_cartesian_product() {
        let combos = parse(
            "# configMatrix: datafusion.execution.batch_size=1024,2048\n\
             # configMatrix: datafusion.execution.param1=true,false\n",
        );
        assert_eq!(combos.len(), 4);
        for combo in &combos {
            assert_eq!(combo.len(), 2);
            assert_eq!(combo[0].0, "datafusion.execution.batch_size");
            assert_eq!(combo[1].0, "datafusion.execution.param1");
        }
        assert_eq!(combos[0][0].1, "1024");
        assert_eq!(combos[0][1].1, "true");
        assert_eq!(combos[3][0].1, "2048");
        assert_eq!(combos[3][1].1, "false");
    }

    #[test]
    fn ignores_non_comment_lines_with_marker_text() {
        assert!(parse("SELECT 'configMatrix: foo=bar';\n").is_empty());
    }

    #[test]
    fn matrix_tag_formats_key_value_pairs() {
        let combo = vec![
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "x".to_string()),
        ];
        assert_eq!(matrix_tag(&combo), "[configMatrix: a=1, b=x]");
    }
}
