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

//! Utility types for JSON processing

use std::io::{BufRead, Read};

// ============================================================================
// JsonArrayToNdjsonReader - Streaming JSON Array to NDJSON Converter
// ============================================================================
//
// Architecture:
//
// ```text
// ┌─────────────────────────────────────────────────────────────┐
// │  JSON Array File (potentially very large, e.g. 1GB)        │
// │  [{"a":1}, {"a":2}, {"a":3}, ...... {"a":1000000}]         │
// └─────────────────────────────────────────────────────────────┘
//                           │
//                           ▼ read small chunks at a time (e.g. 64KB)
//                 ┌───────────────────┐
//                 │ JsonArrayToNdjson │  ← character substitution only:
//                 │      Reader       │    '[' skip, ',' → '\n', ']' stop
//                 └───────────────────┘
//                           │
//                           ▼ outputs NDJSON format
//                 ┌───────────────────┐
//                 │   Arrow Reader    │  ← internal buffer, batch parsing
//                 │  batch_size=1024  │
//                 └───────────────────┘
//                           │
//                           ▼ outputs RecordBatch for every batch_size rows
//                 ┌───────────────────┐
//                 │   RecordBatch     │
//                 │   (1024 rows)     │
//                 └───────────────────┘
// ```
//
// Memory Efficiency:
//
// | Approach                              | Memory for 1GB file | Parse count |
// |---------------------------------------|---------------------|-------------|
// | Load entire file + serde_json         | ~5GB                | 3x          |
// | Streaming with JsonArrayToNdjsonReader| ~few MB             | 1x          |
//

/// Default buffer size for JsonArrayToNdjsonReader (64KB)
const DEFAULT_BUF_SIZE: usize = 64 * 1024;

/// Parser state for JSON array streaming
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JsonArrayState {
    /// Initial state, looking for opening '['
    Start,
    /// Inside the JSON array, processing objects
    InArray,
    /// Reached the closing ']', finished
    Done,
}

/// A streaming reader that converts JSON array format to NDJSON format.
///
/// This reader wraps an underlying reader containing JSON array data
/// `[{...}, {...}, ...]` and transforms it on-the-fly to newline-delimited
/// JSON format that Arrow's JSON reader can process.
///
/// Implements both `Read` and `BufRead` traits for compatibility with Arrow's
/// `ReaderBuilder::build()` which requires `BufRead`.
///
/// # Transformation Rules
///
/// - Skip leading `[` and whitespace before it
/// - Convert top-level `,` (between objects) to `\n`
/// - Skip whitespace at top level (between objects)
/// - Stop at trailing `]`
/// - Preserve everything inside objects (including nested `[`, `]`, `,`)
/// - Properly handle strings (ignore special chars inside quotes)
///
/// # Example
///
/// ```text
/// Input:  [{"a":1}, {"b":[1,2]}, {"c":"x,y"}]
/// Output: {"a":1}
///         {"b":[1,2]}
///         {"c":"x,y"}
/// ```
pub struct JsonArrayToNdjsonReader<R: Read> {
    inner: R,
    state: JsonArrayState,
    /// Tracks nesting depth of `{` and `[` to identify top-level commas
    depth: i32,
    /// Whether we're currently inside a JSON string
    in_string: bool,
    /// Whether the next character is escaped (after `\`)
    escape_next: bool,
    /// Internal buffer for BufRead implementation
    buffer: Vec<u8>,
    /// Current position in the buffer
    pos: usize,
    /// Number of valid bytes in the buffer
    filled: usize,
    /// Whether trailing non-whitespace content was detected after ']'
    has_trailing_content: bool,
}

impl<R: Read> JsonArrayToNdjsonReader<R> {
    /// Create a new streaming reader that converts JSON array to NDJSON.
    pub fn new(reader: R) -> Self {
        Self {
            inner: reader,
            state: JsonArrayState::Start,
            depth: 0,
            in_string: false,
            escape_next: false,
            buffer: vec![0; DEFAULT_BUF_SIZE],
            pos: 0,
            filled: 0,
            has_trailing_content: false,
        }
    }

    /// Check if the JSON array was properly terminated.
    ///
    /// This should be called after all data has been read.
    ///
    /// Returns an error if:
    /// - Unbalanced braces/brackets (depth != 0)
    /// - Unterminated string
    /// - Missing closing `]`
    /// - Unexpected trailing content after `]`
    pub fn validate_complete(&self) -> std::io::Result<()> {
        if self.depth != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Malformed JSON array: unbalanced braces or brackets",
            ));
        }
        if self.in_string {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Malformed JSON array: unterminated string",
            ));
        }
        if self.state != JsonArrayState::Done {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Incomplete JSON array: expected closing bracket ']'",
            ));
        }
        if self.has_trailing_content {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Malformed JSON: unexpected trailing content after ']'",
            ));
        }
        Ok(())
    }

    /// Process a single byte and return the transformed byte (if any)
    fn process_byte(&mut self, byte: u8) -> Option<u8> {
        match self.state {
            JsonArrayState::Start => {
                // Looking for the opening '[', skip whitespace
                if byte == b'[' {
                    self.state = JsonArrayState::InArray;
                }
                // Skip whitespace and the '[' itself
                None
            }
            JsonArrayState::InArray => {
                // Handle escape sequences in strings
                if self.escape_next {
                    self.escape_next = false;
                    return Some(byte);
                }

                if self.in_string {
                    // Inside a string: handle escape and closing quote
                    match byte {
                        b'\\' => self.escape_next = true,
                        b'"' => self.in_string = false,
                        _ => {}
                    }
                    Some(byte)
                } else {
                    // Outside strings: track depth and transform
                    match byte {
                        b'"' => {
                            self.in_string = true;
                            Some(byte)
                        }
                        b'{' | b'[' => {
                            self.depth += 1;
                            Some(byte)
                        }
                        b'}' => {
                            self.depth -= 1;
                            Some(byte)
                        }
                        b']' => {
                            if self.depth == 0 {
                                // Top-level ']' means end of array
                                self.state = JsonArrayState::Done;
                                None
                            } else {
                                // Nested ']' inside an object
                                self.depth -= 1;
                                Some(byte)
                            }
                        }
                        b',' if self.depth == 0 => {
                            // Top-level comma between objects → newline
                            Some(b'\n')
                        }
                        _ => {
                            // At depth 0, skip whitespace between objects
                            if self.depth == 0 && byte.is_ascii_whitespace() {
                                None
                            } else {
                                Some(byte)
                            }
                        }
                    }
                }
            }
            JsonArrayState::Done => {
                // After ']', check for non-whitespace trailing content
                if !byte.is_ascii_whitespace() {
                    self.has_trailing_content = true;
                }
                None
            }
        }
    }

    /// Fill the internal buffer with transformed data
    fn fill_internal_buffer(&mut self) -> std::io::Result<()> {
        // Read raw data from inner reader
        let mut raw_buf = vec![0u8; DEFAULT_BUF_SIZE];
        let mut write_pos = 0;

        loop {
            let bytes_read = self.inner.read(&mut raw_buf)?;
            if bytes_read == 0 {
                break; // EOF
            }

            for &byte in &raw_buf[..bytes_read] {
                if let Some(transformed) = self.process_byte(byte)
                    && write_pos < self.buffer.len()
                {
                    self.buffer[write_pos] = transformed;
                    write_pos += 1;
                }
                // Note: process_byte is called for all bytes to track state,
                // even when buffer is full or in Done state
            }

            // Only stop if buffer is full
            if write_pos >= self.buffer.len() {
                break;
            }
        }

        self.pos = 0;
        self.filled = write_pos;
        Ok(())
    }
}

impl<R: Read> Read for JsonArrayToNdjsonReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // If buffer is empty, fill it
        if self.pos >= self.filled {
            self.fill_internal_buffer()?;
            if self.filled == 0 {
                return Ok(0); // EOF
            }
        }

        // Copy from internal buffer to output
        let available = self.filled - self.pos;
        let to_copy = std::cmp::min(available, buf.len());
        buf[..to_copy].copy_from_slice(&self.buffer[self.pos..self.pos + to_copy]);
        self.pos += to_copy;
        Ok(to_copy)
    }
}

impl<R: Read> BufRead for JsonArrayToNdjsonReader<R> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        if self.pos >= self.filled {
            self.fill_internal_buffer()?;
        }
        Ok(&self.buffer[self.pos..self.filled])
    }

    fn consume(&mut self, amt: usize) {
        self.pos = std::cmp::min(self.pos + amt, self.filled);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_array_to_ndjson_simple() {
        let input = r#"[{"a":1}, {"a":2}, {"a":3}]"#;
        let mut reader = JsonArrayToNdjsonReader::new(input.as_bytes());
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();
        assert_eq!(output, "{\"a\":1}\n{\"a\":2}\n{\"a\":3}");
    }

    #[test]
    fn test_json_array_to_ndjson_nested() {
        let input = r#"[{"a":{"b":1}}, {"c":[1,2,3]}]"#;
        let mut reader = JsonArrayToNdjsonReader::new(input.as_bytes());
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();
        assert_eq!(output, "{\"a\":{\"b\":1}}\n{\"c\":[1,2,3]}");
    }

    #[test]
    fn test_json_array_to_ndjson_strings_with_special_chars() {
        let input = r#"[{"a":"[1,2]"}, {"b":"x,y"}]"#;
        let mut reader = JsonArrayToNdjsonReader::new(input.as_bytes());
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();
        assert_eq!(output, "{\"a\":\"[1,2]\"}\n{\"b\":\"x,y\"}");
    }

    #[test]
    fn test_json_array_to_ndjson_escaped_quotes() {
        let input = r#"[{"a":"say \"hello\""}, {"b":1}]"#;
        let mut reader = JsonArrayToNdjsonReader::new(input.as_bytes());
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();
        assert_eq!(output, "{\"a\":\"say \\\"hello\\\"\"}\n{\"b\":1}");
    }

    #[test]
    fn test_json_array_to_ndjson_empty() {
        let input = r#"[]"#;
        let mut reader = JsonArrayToNdjsonReader::new(input.as_bytes());
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();
        assert_eq!(output, "");
    }

    #[test]
    fn test_json_array_to_ndjson_single_element() {
        let input = r#"[{"a":1}]"#;
        let mut reader = JsonArrayToNdjsonReader::new(input.as_bytes());
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();
        assert_eq!(output, "{\"a\":1}");
    }

    #[test]
    fn test_json_array_to_ndjson_bufread() {
        let input = r#"[{"a":1}, {"a":2}]"#;
        let mut reader = JsonArrayToNdjsonReader::new(input.as_bytes());

        let buf = reader.fill_buf().unwrap();
        assert!(!buf.is_empty());

        let first_len = buf.len();
        reader.consume(first_len);

        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();
    }

    #[test]
    fn test_json_array_to_ndjson_whitespace() {
        let input = r#"  [  {"a":1}  ,  {"a":2}  ]  "#;
        let mut reader = JsonArrayToNdjsonReader::new(input.as_bytes());
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();
        // Top-level whitespace is skipped, internal whitespace preserved
        assert_eq!(output, "{\"a\":1}\n{\"a\":2}");
    }

    #[test]
    fn test_validate_complete_valid_json() {
        let valid_json = r#"[{"a":1},{"a":2}]"#;
        let mut reader = JsonArrayToNdjsonReader::new(valid_json.as_bytes());
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();
        reader.validate_complete().unwrap();
    }

    #[test]
    fn test_json_array_with_trailing_junk() {
        let input = r#" [ {"a":1} , {"a":2} ] some { junk [ here ] "#;
        let mut reader = JsonArrayToNdjsonReader::new(input.as_bytes());
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();

        // Should extract the valid array content
        assert_eq!(output, "{\"a\":1}\n{\"a\":2}");

        // But validation should catch the trailing junk
        let result = reader.validate_complete();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("trailing content")
                || err_msg.contains("Unexpected trailing"),
            "Expected trailing content error, got: {err_msg}"
        );
    }

    #[test]
    fn test_validate_complete_incomplete_array() {
        let invalid_json = r#"[{"a":1},{"a":2}"#; // Missing closing ]
        let mut reader = JsonArrayToNdjsonReader::new(invalid_json.as_bytes());
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();

        let result = reader.validate_complete();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("expected closing bracket")
                || err_msg.contains("missing closing"),
            "Expected missing bracket error, got: {err_msg}"
        );
    }

    #[test]
    fn test_validate_complete_unbalanced_braces() {
        let invalid_json = r#"[{"a":1},{"a":2]"#; // Wrong closing bracket
        let mut reader = JsonArrayToNdjsonReader::new(invalid_json.as_bytes());
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();

        let result = reader.validate_complete();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("unbalanced")
                || err_msg.contains("expected closing bracket"),
            "Expected unbalanced or missing bracket error, got: {err_msg}"
        );
    }

    #[test]
    fn test_validate_complete_valid_with_trailing_whitespace() {
        let input = r#"[{"a":1},{"a":2}]
    "#; // Trailing whitespace is OK
        let mut reader = JsonArrayToNdjsonReader::new(input.as_bytes());
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();

        // Whitespace after ] should be allowed
        reader.validate_complete().unwrap();
    }
}
