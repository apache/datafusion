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

//! `# configMatrix:` directives sweep DataFusion config across repeated runs of
//! one SLT file:
//!
//! ```text
//! # configMatrix: <key>=<v1>,<v2>[,...]
//! ```
//!
//! This module validates directives, dedups their values, and expands them into
//! the [`TestConfiguration`]s the runner applies to a session. Repeated
//! directives combine as a cartesian product.

use std::fmt;
use std::fs;
use std::future::Future;
use std::path::Path;

use datafusion::common::{DataFusionError, Result, exec_datafusion_err};
use itertools::Itertools;

/// Compared case-insensitively; this spelling is the documented one.
const DIRECTIVE_MARKER: &str = "configMatrix:";

/// Config values to apply before a single run of an SLT file.
///
/// Empty when the file declared no directives, which means "run once,
/// unmodified".
#[derive(Debug, Clone)]
pub struct TestConfiguration(Vec<(String, String)>);

impl TestConfiguration {
    /// The `key = value` pairs to set on the session.
    pub fn settings(&self) -> &[(String, String)] {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Prefix a failure with the configuration that produced it, so a sweep
    /// names the values that broke. No-op when no directives were declared.
    fn attribute_failure<T>(&self, result: Result<T>) -> Result<T> {
        if self.is_empty() {
            return result;
        }
        result.map_err(|e| e.context(self.to_string()))
    }
}

impl fmt::Display for TestConfiguration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[configMatrix: {}]",
            self.0.iter().map(|(k, v)| format!("{k}={v}")).join(", ")
        )
    }
}

/// Validate, dedup, and expand the directives declared by `path`.
///
/// Never empty: a file without directives yields one empty
/// [`TestConfiguration`], so callers keep a single loop shape.
pub fn test_configurations(path: &Path) -> Result<Vec<TestConfiguration>> {
    let content = fs::read_to_string(path).map_err(|e| {
        exec_datafusion_err!(
            "Failed to read {} for configMatrix parsing: {e}",
            path.display()
        )
    })?;
    parse_configurations(&content, path)
}

/// Run `run_one` once per configuration, continuing past failures so one broken
/// combination never hides the others. Each failure is prefixed with the
/// configuration that produced it (see `TestConfiguration::attribute_failure`).
///
/// Returns `Ok(())` if all passed, the lone failure verbatim if exactly one
/// failed (so a matrix-free file keeps its original error), or an aggregate
/// naming every failing combination.
///
/// `run_one` takes the configuration by value so its future holds no
/// higher-ranked borrow and stays `Send` for the runner's spawned task.
pub async fn run_each_configuration<F, Fut>(
    configurations: Vec<TestConfiguration>,
    mut run_one: F,
) -> Result<()>
where
    F: FnMut(TestConfiguration) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut failures = Vec::new();
    for configuration in configurations {
        // Keep a copy to attribute the failure after `run_one` consumes it.
        let label = configuration.clone();
        if let Err(err) = label.attribute_failure(run_one(configuration).await) {
            failures.push(err);
        }
    }
    combine_configuration_failures(failures)
}

/// Collapse per-configuration failures into a single [`Result`], returned by
/// [`run_each_configuration`].
fn combine_configuration_failures(mut failures: Vec<DataFusionError>) -> Result<()> {
    match failures.len() {
        0 => Ok(()),
        1 => Err(failures.pop().unwrap()),
        n => {
            let combined = failures.iter().join("\n\n");
            Err(DataFusionError::External(
                format!("{n} configMatrix combinations failed:\n\n{combined}").into(),
            ))
        }
    }
}

/// `path` is used only for error messages.
fn parse_configurations(content: &str, path: &Path) -> Result<Vec<TestConfiguration>> {
    // Dimensions borrow `content`; values are owned only in the product below.
    let mut dimensions: Vec<(&str, Vec<&str>)> = Vec::new();

    for (idx, line) in content.lines().enumerate() {
        let Some(directive) = strip_directive_prefix(line) else {
            continue;
        };
        let invalid = |detail: String| {
            DataFusionError::Configuration(format!(
                "Invalid configMatrix directive in {}:{}: {detail}",
                path.display(),
                idx + 1
            ))
        };

        let (key, values) = directive.split_once('=').ok_or_else(|| {
            invalid(format!(
                "expected `# configMatrix: <key>=<v1>,<v2>[,...]`, got `{directive}`"
            ))
        })?;

        let key = key.trim();
        if key.is_empty() {
            return Err(invalid("missing config key".to_string()));
        }

        let values: Vec<&str> = values
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .collect();
        if values.is_empty() {
            return Err(invalid(format!("no values provided for `{key}`")));
        }

        // A repeated key unions its values instead of adding a dimension.
        match dimensions.iter_mut().find(|(seen, _)| *seen == key) {
            Some((_, merged)) => merged.extend(values),
            None => dimensions.push((key, values)),
        }
    }

    Ok(dimensions
        .into_iter()
        .map(|(key, values)| {
            // Dedup keeps first-seen order.
            values
                .into_iter()
                .unique()
                .map(|value| (key.to_string(), value.to_string()))
                .collect_vec()
        })
        .multi_cartesian_product()
        .map(TestConfiguration)
        .collect())
}

/// Text after `configMatrix:` when `line` is a directive comment. Tolerates
/// `##` banners and any marker casing.
fn strip_directive_prefix(line: &str) -> Option<&str> {
    let after_hash = line.trim_start().strip_prefix('#')?;
    let after_hash = after_hash.trim_start_matches('#').trim_start();

    let (marker, rest) = after_hash.split_at_checked(DIRECTIVE_MARKER.len())?;
    if !marker.eq_ignore_ascii_case(DIRECTIVE_MARKER) {
        return None;
    }
    Some(rest.trim())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::exec_err;
    use std::io::Write;

    /// Settings of every configuration parsed from `content`.
    fn parse(content: &str) -> Vec<Vec<(String, String)>> {
        parse_configurations(content, Path::new("test.slt"))
            .unwrap()
            .into_iter()
            .map(|test_configuration| test_configuration.0)
            .collect()
    }

    fn test_configuration(settings: &[(&str, &str)]) -> TestConfiguration {
        TestConfiguration(
            settings
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        )
    }

    #[test]
    fn parses_no_directives() {
        // Zero dimensions expand to exactly one empty configuration.
        let content = "# ordinary comment\nquery I\nSELECT 1\n----\n1\n";
        assert_eq!(parse(content), vec![Vec::new()]);
    }

    #[test]
    fn parses_single_directive() {
        let configurations = parse(
            "# configMatrix: datafusion.optimizer.enable_piecewise_merge_join=true,false\n",
        );
        assert_eq!(configurations.len(), 2);
        assert_eq!(
            configurations[0],
            vec![(
                "datafusion.optimizer.enable_piecewise_merge_join".to_string(),
                "true".to_string(),
            )]
        );
        assert_eq!(
            configurations[1],
            vec![(
                "datafusion.optimizer.enable_piecewise_merge_join".to_string(),
                "false".to_string(),
            )]
        );
    }

    #[test]
    fn parses_case_insensitive_marker() {
        let configurations =
            parse("## CONFIGMATRIX: datafusion.execution.batch_size=1024,4096\n");
        assert_eq!(configurations.len(), 2);
        assert_eq!(configurations[0][0].0, "datafusion.execution.batch_size");
    }

    #[test]
    fn parses_multiple_directives_and_computes_cartesian_product() {
        let configurations = parse("# configMatrix: a=1,2\n# configMatrix: b=x,y,z\n");
        assert_eq!(configurations.len(), 6);
        assert_eq!(
            configurations[0],
            vec![
                ("a".to_string(), "1".to_string()),
                ("b".to_string(), "x".to_string()),
            ]
        );
        assert_eq!(
            configurations[5],
            vec![
                ("a".to_string(), "2".to_string()),
                ("b".to_string(), "z".to_string()),
            ]
        );
    }

    #[test]
    fn rejects_missing_equals() {
        let err = parse_configurations(
            "# configMatrix: bogus_no_equals\n",
            Path::new("test.slt"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("<key>=<v1>"));
    }

    #[test]
    fn rejects_missing_key() {
        let err =
            parse_configurations("# configMatrix: =true,false\n", Path::new("test.slt"))
                .unwrap_err();
        assert!(err.to_string().contains("missing config key"));
    }

    #[test]
    fn rejects_empty_values() {
        let err =
            parse_configurations("# configMatrix: some.key=\n", Path::new("test.slt"))
                .unwrap_err();
        assert!(err.to_string().contains("no values provided"));
    }

    #[test]
    fn rejects_empty_values_on_a_repeated_key() {
        let err = parse_configurations(
            "# configMatrix: k=1,2\n# configMatrix: k=\n",
            Path::new("test.slt"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("no values provided"));
    }

    #[test]
    fn deduplicates_repeated_values_preserving_first_order() {
        let configurations =
            parse("# configMatrix: some.key=false,true,false,true,false\n");
        assert_eq!(configurations.len(), 2);
        assert_eq!(configurations[0][0].1, "false");
        assert_eq!(configurations[1][0].1, "true");
    }

    #[test]
    fn strips_whitespace_around_values() {
        let configurations = parse("# configMatrix: k= 1024 ,\t2048  , \t4096\n");
        let values: Vec<&str> = configurations.iter().map(|c| c[0].1.as_str()).collect();
        assert_eq!(values, vec!["1024", "2048", "4096"]);
    }

    #[test]
    fn strips_whitespace_around_key_and_around_equals() {
        let configurations = parse(
            "#   configMatrix:   datafusion.execution.batch_size  =  1024 , 2048  \n",
        );
        assert_eq!(configurations.len(), 2);
        assert_eq!(configurations[0][0].0, "datafusion.execution.batch_size");
    }

    #[test]
    fn dedup_runs_after_whitespace_is_stripped() {
        // `1024`, ` 1024`, and `\t1024  ` all normalize to the same value.
        let configurations = parse("# configMatrix: k= 1024 , 1024,\t1024  \n");
        assert_eq!(configurations.len(), 1);
        assert_eq!(configurations[0][0].1, "1024");
    }

    #[test]
    fn trailing_and_repeated_commas_are_ignored() {
        let configurations = parse("# configMatrix: k=1024,,2048,\n");
        let values: Vec<&str> = configurations.iter().map(|c| c[0].1.as_str()).collect();
        assert_eq!(values, vec!["1024", "2048"]);
    }

    #[test]
    fn merges_repeated_directives_for_same_key() {
        let configurations = parse(
            "# configMatrix: datafusion.execution.batch_size=1024,2048\n\
             # configMatrix: datafusion.execution.batch_size=1024,2048\n",
        );
        assert_eq!(configurations.len(), 2);
    }

    #[test]
    fn merges_repeated_directives_unioning_values() {
        let configurations =
            parse("# configMatrix: k=1024,2048\n# configMatrix: k=2048,4096\n");
        let values: Vec<&str> = configurations.iter().map(|c| c[0].1.as_str()).collect();
        assert_eq!(values, vec!["1024", "2048", "4096"]);
    }

    #[test]
    fn nested_matrices_expand_as_cartesian_product() {
        let configurations = parse(
            "# configMatrix: datafusion.execution.batch_size=1024,2048\n\
             # configMatrix: datafusion.execution.param1=true,false\n",
        );
        assert_eq!(configurations.len(), 4);
        for configuration in &configurations {
            assert_eq!(configuration.len(), 2);
            assert_eq!(configuration[0].0, "datafusion.execution.batch_size");
            assert_eq!(configuration[1].0, "datafusion.execution.param1");
        }
        assert_eq!(configurations[0][0].1, "1024");
        assert_eq!(configurations[0][1].1, "true");
        assert_eq!(configurations[3][0].1, "2048");
        assert_eq!(configurations[3][1].1, "false");
    }

    #[test]
    fn ignores_non_comment_lines_with_marker_text() {
        assert_eq!(parse("SELECT 'configMatrix: foo=bar';\n"), vec![Vec::new()]);
    }

    #[test]
    fn displays_key_value_pairs() {
        assert_eq!(
            test_configuration(&[("a", "1"), ("b", "x")]).to_string(),
            "[configMatrix: a=1, b=x]"
        );
    }

    #[test]
    fn test_configurations_yields_one_empty_entry_when_no_matrix() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(b"# just a comment\nquery I\nSELECT 1\n----\n1\n")
            .unwrap();
        let configurations = test_configurations(file.path()).unwrap();
        assert_eq!(configurations.len(), 1);
        assert!(configurations[0].is_empty());
    }

    #[test]
    fn test_configurations_yields_every_combination() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(
            b"# configMatrix: a=1,2\n\
              # configMatrix: b=x,y\n\
              query I\nSELECT 1\n----\n1\n",
        )
        .unwrap();
        let configurations = test_configurations(file.path()).unwrap();
        assert_eq!(configurations.len(), 4);
        assert!(configurations.iter().all(|c| !c.is_empty()));
    }

    #[test]
    fn attribute_failure_is_noop_without_directives() {
        let err: Result<()> = Err(exec_datafusion_err!("boom"));
        assert_eq!(
            test_configuration(&[])
                .attribute_failure(err)
                .unwrap_err()
                .to_string(),
            exec_datafusion_err!("boom").to_string()
        );
    }

    #[test]
    fn attribute_failure_names_the_configuration() {
        let err: Result<()> = Err(exec_datafusion_err!("boom"));
        let msg = test_configuration(&[("k", "1")])
            .attribute_failure(err)
            .unwrap_err()
            .to_string();
        assert!(msg.starts_with("[configMatrix: k=1]"), "got {msg}");
        assert!(msg.contains("boom"), "got {msg}");
    }

    #[tokio::test]
    async fn run_each_configuration_runs_every_combination_past_failures() {
        use std::sync::{Arc, Mutex};

        // Drives the exact helper both runner paths use: every configuration
        // must run and be attributed even though an earlier one fails.
        let configurations = vec![
            test_configuration(&[("k", "1")]),
            test_configuration(&[("k", "2")]),
            test_configuration(&[("k", "3")]),
        ];
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);

        let result = run_each_configuration(configurations, |configuration| {
            let recorder = Arc::clone(&recorder);
            async move {
                let value = configuration.settings()[0].1.clone();
                recorder.lock().unwrap().push(value.clone());
                // The middle combination fails; the last must still run.
                if value == "2" {
                    exec_err!("boom")
                } else {
                    Ok(())
                }
            }
        })
        .await;

        // All three ran even though the second failed.
        assert_eq!(*seen.lock().unwrap(), vec!["1", "2", "3"]);
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("[configMatrix: k=2]"), "got {msg}");
        assert!(msg.contains("boom"), "got {msg}");
    }

    #[tokio::test]
    async fn run_each_configuration_is_ok_when_all_pass() {
        let configurations = vec![
            test_configuration(&[("k", "1")]),
            test_configuration(&[("k", "2")]),
        ];
        let result = run_each_configuration(configurations, |_| async { Ok(()) }).await;
        assert!(result.is_ok());
    }

    #[test]
    fn combine_configuration_failures_is_ok_when_empty() {
        assert!(combine_configuration_failures(vec![]).is_ok());
    }

    #[test]
    fn combine_configuration_failures_returns_a_lone_failure_verbatim() {
        // A file without a matrix expands to one empty configuration, so its
        // failure must be reported exactly as before (no combination prefix,
        // no aggregate header).
        let boom: Result<()> = exec_err!("boom");
        let failure = test_configuration(&[]).attribute_failure(boom).unwrap_err();
        let msg = combine_configuration_failures(vec![failure])
            .unwrap_err()
            .to_string();
        assert_eq!(msg, exec_datafusion_err!("boom").to_string());
    }

    #[test]
    fn combine_configuration_failures_aggregates_and_attributes_all_failures() {
        let boom_1: Result<()> = exec_err!("boom-1");
        let boom_2: Result<()> = exec_err!("boom-2");
        let failures = vec![
            test_configuration(&[("k", "1")])
                .attribute_failure(boom_1)
                .unwrap_err(),
            test_configuration(&[("k", "2")])
                .attribute_failure(boom_2)
                .unwrap_err(),
        ];

        let msg = combine_configuration_failures(failures)
            .unwrap_err()
            .to_string();

        assert!(
            msg.contains("2 configMatrix combinations failed"),
            "got {msg}"
        );
        assert!(msg.contains("[configMatrix: k=1]"), "got {msg}");
        assert!(msg.contains("boom-1"), "got {msg}");
        assert!(msg.contains("[configMatrix: k=2]"), "got {msg}");
        assert!(msg.contains("boom-2"), "got {msg}");
    }
}
