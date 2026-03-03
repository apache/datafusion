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

use bytes::Bytes;

// ============================================================================
// JsonArrayToNdjsonReader - Streaming JSON Array to NDJSON Converter
// ============================================================================
//
// Architecture:
//
// ```text
// ┌─────────────────────────────────────────────────────────────┐
// │  JSON Array File (potentially very large, e.g. 33GB)       │
// │  [{"a":1}, {"a":2}, {"a":3}, ...... {"a":1000000}]         │
// └─────────────────────────────────────────────────────────────┘
//                           │
//                           ▼ read chunks via ChannelReader
//                 ┌───────────────────┐
//                 │ JsonArrayToNdjson │  ← character substitution only:
//                 │      Reader       │    '[' skip, ',' → '\n', ']' stop
//                 └───────────────────┘
//                           │
//                           ▼ outputs NDJSON format
//                 ┌───────────────────┐
//                 │   Arrow Reader    │  ← internal buffer, batch parsing
//                 │  batch_size=8192  │
//                 └───────────────────┘
//                           │
//                           ▼ outputs RecordBatch
//                 ┌───────────────────┐
//                 │   RecordBatch     │
//                 └───────────────────┘
// ```
//
// Memory Efficiency:
//
// | Approach                              | Memory for 33GB file | Parse count |
// |---------------------------------------|----------------------|-------------|
// | Load entire file + serde_json         | ~100GB+              | 3x          |
// | Streaming with JsonArrayToNdjsonReader| ~32MB (configurable) | 1x          |
//
// Design Note:
//
// This implementation uses `inner: R` directly (not `BufReader<R>`) and manages
// its own input buffer. This is critical for compatibility with `SyncIoBridge`
// and `ChannelReader` in `spawn_blocking` contexts.
//

/// Default buffer size for JsonArrayToNdjsonReader (2MB for better throughput)
const DEFAULT_BUF_SIZE: usize = 2 * 1024 * 1024;

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
    /// Inner reader - we use R directly (not `BufReader<R>`) for SyncIoBridge compatibility
    inner: R,
    state: JsonArrayState,
    /// Tracks nesting depth of `{` and `[` to identify top-level commas
    depth: i32,
    /// Whether we're currently inside a JSON string
    in_string: bool,
    /// Whether the next character is escaped (after `\`)
    escape_next: bool,
    /// Input buffer - stores raw bytes read from inner reader
    input_buffer: Vec<u8>,
    /// Current read position in input buffer
    input_pos: usize,
    /// Number of valid bytes in input buffer
    input_filled: usize,
    /// Output buffer - stores transformed NDJSON bytes
    output_buffer: Vec<u8>,
    /// Current read position in output buffer
    output_pos: usize,
    /// Number of valid bytes in output buffer
    output_filled: usize,
    /// Whether trailing non-whitespace content was detected after ']'
    has_trailing_content: bool,
    /// Whether leading non-whitespace content was detected before '['
    has_leading_content: bool,
}

impl<R: Read> JsonArrayToNdjsonReader<R> {
    /// Create a new streaming reader that converts JSON array to NDJSON.
    pub fn new(reader: R) -> Self {
        Self::with_capacity(reader, DEFAULT_BUF_SIZE)
    }

    /// Create a new streaming reader with custom buffer size.
    ///
    /// Larger buffers improve throughput but use more memory.
    /// Total memory usage is approximately 2 * capacity (input + output buffers).
    pub fn with_capacity(reader: R, capacity: usize) -> Self {
        Self {
            inner: reader,
            state: JsonArrayState::Start,
            depth: 0,
            in_string: false,
            escape_next: false,
            input_buffer: vec![0; capacity],
            input_pos: 0,
            input_filled: 0,
            output_buffer: vec![0; capacity],
            output_pos: 0,
            output_filled: 0,
            has_trailing_content: false,
            has_leading_content: false,
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
        if self.has_leading_content {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Malformed JSON: unexpected leading content before '['",
            ));
        }
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
    #[inline]
    fn process_byte(&mut self, byte: u8) -> Option<u8> {
        match self.state {
            JsonArrayState::Start => {
                // Looking for the opening '[', skip whitespace
                if byte == b'[' {
                    self.state = JsonArrayState::InArray;
                } else if !byte.is_ascii_whitespace() {
                    self.has_leading_content = true;
                }
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

    /// Refill input buffer from inner reader if needed.
    /// Returns true if there's data available, false on EOF.
    fn refill_input_if_needed(&mut self) -> std::io::Result<bool> {
        if self.input_pos >= self.input_filled {
            // Input buffer exhausted, read more from inner
            let bytes_read = self.inner.read(&mut self.input_buffer)?;
            if bytes_read == 0 {
                return Ok(false); // EOF
            }
            self.input_pos = 0;
            self.input_filled = bytes_read;
        }
        Ok(true)
    }

    /// Fill the output buffer with transformed data.
    ///
    /// This method manages its own input buffer, reading from the inner reader
    /// as needed. When the output buffer is full, we stop processing but preserve
    /// the current position in the input buffer for the next call.
    fn fill_output_buffer(&mut self) -> std::io::Result<()> {
        let mut write_pos = 0;

        while write_pos < self.output_buffer.len() {
            // Refill input buffer if exhausted
            if !self.refill_input_if_needed()? {
                break; // EOF
            }

            // Process bytes from input buffer
            while self.input_pos < self.input_filled
                && write_pos < self.output_buffer.len()
            {
                let byte = self.input_buffer[self.input_pos];
                self.input_pos += 1;

                if let Some(transformed) = self.process_byte(byte) {
                    self.output_buffer[write_pos] = transformed;
                    write_pos += 1;
                }
            }
        }

        self.output_pos = 0;
        self.output_filled = write_pos;
        Ok(())
    }
}

impl<R: Read> Read for JsonArrayToNdjsonReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // If output buffer is empty, fill it
        if self.output_pos >= self.output_filled {
            self.fill_output_buffer()?;
            if self.output_filled == 0 {
                return Ok(0); // EOF
            }
        }

        // Copy from output buffer to caller's buffer
        let available = self.output_filled - self.output_pos;
        let to_copy = std::cmp::min(available, buf.len());
        buf[..to_copy].copy_from_slice(
            &self.output_buffer[self.output_pos..self.output_pos + to_copy],
        );
        self.output_pos += to_copy;
        Ok(to_copy)
    }
}

impl<R: Read> BufRead for JsonArrayToNdjsonReader<R> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        if self.output_pos >= self.output_filled {
            self.fill_output_buffer()?;
        }
        Ok(&self.output_buffer[self.output_pos..self.output_filled])
    }

    fn consume(&mut self, amt: usize) {
        self.output_pos = std::cmp::min(self.output_pos + amt, self.output_filled);
    }
}

// ============================================================================
// ChannelReader - Sync reader that receives bytes from async channel
// ============================================================================
//
// Architecture:
//
// ```text
// ┌─────────────────────────────────────────────────────────────────────────┐
// │                         S3 / MinIO (async)                              │
// │                    (33GB JSON Array File)                               │
// └─────────────────────────────────────────────────────────────────────────┘
//                                 │
//                                 ▼ async stream (Bytes chunks)
// ┌─────────────────────────────────────────────────────────────────────────┐
// │                      Async Task (tokio runtime)                         │
// │              while let Some(chunk) = stream.next().await                │
// │                     byte_tx.send(chunk)                                 │
// └─────────────────────────────────────────────────────────────────────────┘
//                                 │
//                                 ▼ tokio::sync::mpsc::channel<Bytes>
//                                 │   (bounded, ~32MB buffer)
//                                 ▼
// ┌─────────────────────────────────────────────────────────────────────────┐
// │                   Blocking Task (spawn_blocking)                        │
// │  ┌──────────────┐   ┌────────────────────────┐   ┌──────────────────┐  │
// │  │ChannelReader │ → │JsonArrayToNdjsonReader │ → │ Arrow JsonReader │  │
// │  │   (Read)     │   │  [{},...] → {}\n{}     │   │  (RecordBatch)   │  │
// │  └──────────────┘   └────────────────────────┘   └──────────────────┘  │
// └─────────────────────────────────────────────────────────────────────────┘
//                                 │
//                                 ▼ tokio::sync::mpsc::channel<RecordBatch>
// ┌─────────────────────────────────────────────────────────────────────────┐
// │                      ReceiverStream (async)                             │
// │                   → DataFusion execution engine                         │
// └─────────────────────────────────────────────────────────────────────────┘
// ```
//
// Memory Budget (~32MB total):
// - sync_channel buffer: 128 chunks × ~128KB = ~16MB
// - JsonArrayToNdjsonReader: 2 × 2MB = 4MB
// - Arrow JsonReader internal: ~8MB
// - Miscellaneous: ~4MB
//

/// A synchronous `Read` implementation that receives bytes from an async channel.
///
/// This enables true streaming between async and sync contexts without
/// loading the entire file into memory. Uses `tokio::sync::mpsc::Receiver`
/// with `blocking_recv()` so the async producer never blocks a tokio worker
/// thread, while the sync consumer (running in `spawn_blocking`) safely blocks.
pub struct ChannelReader {
    rx: tokio::sync::mpsc::Receiver<Bytes>,
    current: Option<Bytes>,
    pos: usize,
}

impl ChannelReader {
    /// Create a new ChannelReader from a tokio mpsc receiver.
    pub fn new(rx: tokio::sync::mpsc::Receiver<Bytes>) -> Self {
        Self {
            rx,
            current: None,
            pos: 0,
        }
    }
}

impl Read for ChannelReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            // If we have current chunk with remaining data, read from it
            if let Some(ref chunk) = self.current {
                let remaining = chunk.len() - self.pos;
                if remaining > 0 {
                    let to_copy = std::cmp::min(remaining, buf.len());
                    buf[..to_copy].copy_from_slice(&chunk[self.pos..self.pos + to_copy]);
                    self.pos += to_copy;
                    return Ok(to_copy);
                }
            }

            // Current chunk exhausted, get next from channel
            match self.rx.blocking_recv() {
                Some(bytes) => {
                    self.current = Some(bytes);
                    self.pos = 0;
                    // Loop back to read from new chunk
                }
                None => return Ok(0), // Channel closed = EOF
            }
        }
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
    fn test_json_array_with_leading_junk() {
        let input = r#"junk[{"a":1}, {"a":2}]"#;
        let mut reader = JsonArrayToNdjsonReader::new(input.as_bytes());
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();

        // Should still extract the valid array content
        assert_eq!(output, "{\"a\":1}\n{\"a\":2}");

        // But validation should catch the leading junk
        let result = reader.validate_complete();
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("leading content"),
            "Expected leading content error, got: {err_msg}"
        );
    }

    #[test]
    fn test_json_array_with_leading_whitespace_ok() {
        let input = r#"
  [{"a":1}, {"a":2}]"#;
        let mut reader = JsonArrayToNdjsonReader::new(input.as_bytes());
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();
        assert_eq!(output, "{\"a\":1}\n{\"a\":2}");

        // Leading whitespace should be fine
        reader.validate_complete().unwrap();
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

    /// Test that data is not lost at buffer boundaries.
    ///
    /// This test creates input larger than the internal buffer to verify
    /// that newline characters are not dropped when they occur at buffer boundaries.
    #[test]
    fn test_buffer_boundary_no_data_loss() {
        // Create objects ~9KB each, so 10 objects = ~90KB
        let large_value = "x".repeat(9000);

        let mut objects = vec![];
        for i in 0..10 {
            objects.push(format!(r#"{{"id":{i},"data":"{large_value}"}}"#));
        }

        let input = format!("[{}]", objects.join(","));

        // Use small buffer to force multiple fill cycles
        let mut reader = JsonArrayToNdjsonReader::with_capacity(input.as_bytes(), 8192);
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();

        // Verify correct number of newlines (9 newlines separate 10 objects)
        let newline_count = output.matches('\n').count();
        assert_eq!(
            newline_count, 9,
            "Expected 9 newlines separating 10 objects, got {newline_count}"
        );

        // Verify each line is valid JSON
        for (i, line) in output.lines().enumerate() {
            let parsed: Result<serde_json::Value, _> = serde_json::from_str(line);
            assert!(
                parsed.is_ok(),
                "Line {} is not valid JSON: {}...",
                i,
                &line[..100.min(line.len())]
            );

            // Verify the id field matches expected value
            let value = parsed.unwrap();
            assert_eq!(
                value["id"].as_i64(),
                Some(i as i64),
                "Object {i} has wrong id"
            );
        }
    }

    /// Test with real-world-like data format (with leading whitespace and newlines)
    #[test]
    fn test_real_world_format_large() {
        let large_value = "x".repeat(8000);

        // Format similar to real files: opening bracket on its own line,
        // each object indented with 2 spaces
        let mut objects = vec![];
        for i in 0..10 {
            objects.push(format!(r#"  {{"id":{i},"data":"{large_value}"}}"#));
        }

        let input = format!("[\n{}\n]", objects.join(",\n"));

        let mut reader = JsonArrayToNdjsonReader::with_capacity(input.as_bytes(), 8192);
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();

        let lines: Vec<&str> = output.lines().collect();
        assert_eq!(lines.len(), 10, "Expected 10 objects");

        for (i, line) in lines.iter().enumerate() {
            assert!(
                line.starts_with("{\"id\""),
                "Line {} should start with object, got: {}...",
                i,
                &line[..50.min(line.len())]
            );
        }
    }

    /// Test ChannelReader
    #[test]
    fn test_channel_reader() {
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        // Send some chunks (try_send is non-async)
        tx.try_send(Bytes::from("Hello, ")).unwrap();
        tx.try_send(Bytes::from("World!")).unwrap();
        drop(tx); // Close channel

        let mut reader = ChannelReader::new(rx);
        let mut output = String::new();
        reader.read_to_string(&mut output).unwrap();

        assert_eq!(output, "Hello, World!");
    }

    /// Test ChannelReader with small reads
    #[test]
    fn test_channel_reader_small_reads() {
        let (tx, rx) = tokio::sync::mpsc::channel(4);

        tx.try_send(Bytes::from("ABCDEFGHIJ")).unwrap();
        drop(tx);

        let mut reader = ChannelReader::new(rx);
        let mut buf = [0u8; 3];

        // Read in small chunks
        assert_eq!(reader.read(&mut buf).unwrap(), 3);
        assert_eq!(&buf, b"ABC");

        assert_eq!(reader.read(&mut buf).unwrap(), 3);
        assert_eq!(&buf, b"DEF");

        assert_eq!(reader.read(&mut buf).unwrap(), 3);
        assert_eq!(&buf, b"GHI");

        assert_eq!(reader.read(&mut buf).unwrap(), 1);
        assert_eq!(&buf[..1], b"J");

        // EOF
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }
}
