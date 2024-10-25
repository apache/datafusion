use std::collections::hash_map::Entry;
use std::collections::HashMap;

use arrow::error::ArrowError;

use regex::Regex;

pub(crate) fn compile_and_cache_regex(
    regex: &str,
    flags: Option<&str>,
    regex_cache: &mut HashMap<String, Regex>,
) -> Result<Regex, ArrowError> {
    match regex_cache.entry(regex.to_string()) {
        Entry::Vacant(entry) => {
            let compiled = compile_regex(regex, flags)?;
            entry.insert(compiled.clone());
            Ok(compiled)
        }
        Entry::Occupied(entry) => Ok(entry.get().to_owned()),
    }
}

pub(crate) fn compile_regex(
    regex: &str,
    flags: Option<&str>,
) -> Result<Regex, ArrowError> {
    let pattern = match flags {
        None | Some("") => regex.to_string(),
        Some(flags) => {
            if flags.contains("g") {
                return Err(ArrowError::ComputeError(
                    "regexp_count() does not support global flag".to_string(),
                ));
            }
            format!("(?{}){}", flags, regex)
        }
    };

    Regex::new(&pattern).map_err(|_| {
        ArrowError::ComputeError(format!(
            "Regular expression did not compile: {}",
            pattern
        ))
    })
}
