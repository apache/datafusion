use crate::error::{DataFusionError, Result};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::collections::VecDeque;

/// The ASCII encoding of `"`
const QUOTE: u8 = 34;

/// The ASCII encoding of `\n`
const NEWLINE: u8 = 10;

/// The ASCII encoding of `\`
const ESCAPE: u8 = 92;

/// [`LineDelimiter`] is provided with a stream of [`Bytes`] and returns an iterator
/// of [`Bytes`] containing a whole number of new line delimited records
#[derive(Debug, Default)]
struct LineDelimiter {
    /// Complete chunks of [`Bytes`]
    complete: VecDeque<Bytes>,
    /// Remainder bytes that form the next record
    remainder: Vec<u8>,
    /// True if the last character was the escape character
    is_escape: bool,
    /// True if currently processing a quoted string
    is_quote: bool,
}

impl LineDelimiter {
    /// Creates a new [`LineDelimiter`] with the provided delimiter
    fn new() -> Self {
        Self::default()
    }

    /// Adds the next set of [`Bytes`]
    fn push(&mut self, val: impl Into<Bytes>) {
        let val: Bytes = val.into();

        let is_escape = &mut self.is_escape;
        let is_quote = &mut self.is_quote;
        let mut record_ends = val.iter().enumerate().filter_map(|(idx, v)| {
            if *is_escape {
                *is_escape = false;
                None
            } else if *v == ESCAPE {
                *is_escape = true;
                None
            } else if *v == QUOTE {
                *is_quote = !*is_quote;
                None
            } else if *is_quote {
                None
            } else {
                (*v == NEWLINE).then(|| idx + 1)
            }
        });

        let start_offset = match self.remainder.is_empty() {
            true => 0,
            false => match record_ends.next() {
                Some(idx) => {
                    self.remainder.extend_from_slice(&val[0..idx]);
                    self.complete
                        .push_back(Bytes::from(std::mem::take(&mut self.remainder)));
                    idx
                }
                None => {
                    self.remainder.extend_from_slice(&val);
                    return;
                }
            },
        };
        let end_offset = record_ends.last().unwrap_or(start_offset);
        if start_offset != end_offset {
            self.complete.push_back(val.slice(start_offset..end_offset));
        }

        if end_offset != val.len() {
            self.remainder.extend_from_slice(&val[end_offset..])
        }
    }

    /// Marks the end of the stream, delimiting any remaining bytes
    ///
    /// Returns `true` if there is no remaining data to be read
    fn finish(&mut self) -> Result<bool> {
        if !self.remainder.is_empty() {
            if self.is_quote {
                return Err(DataFusionError::Execution(
                    "encountered unterminated string".to_string(),
                ));
            }

            if self.is_escape {
                return Err(DataFusionError::Execution(
                    "encountered trailing escape character".to_string(),
                ));
            }

            self.complete
                .push_back(Bytes::from(std::mem::take(&mut self.remainder)))
        }
        Ok(self.complete.is_empty())
    }
}

impl Iterator for LineDelimiter {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        self.complete.pop_front()
    }
}

/// Given a [`Stream`] of [`Bytes`] returns a [`Stream`] where each
/// yielded [`Bytes`] contains a whole number of new line delimited records
pub fn newline_delimited_stream<S>(s: S) -> impl Stream<Item = Result<Bytes>>
where
    S: Stream<Item = Result<Bytes>> + Unpin,
{
    let delimiter = LineDelimiter::new();

    futures::stream::unfold((s, delimiter), |(mut s, mut delimiter)| async move {
        loop {
            if let Some(next) = delimiter.next() {
                return Some((Ok(next), (s, delimiter)));
            }

            match s.next().await {
                Some(Ok(bytes)) => delimiter.push(bytes),
                Some(Err(e)) => return Some((Err(e), (s, delimiter))),
                None => match delimiter.finish() {
                    Ok(true) => return None,
                    Ok(false) => continue,
                    Err(e) => return Some((Err(e), (s, delimiter))),
                },
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream::TryStreamExt;

    #[test]
    fn test_delimiter() {
        let mut delimiter = LineDelimiter::new();
        delimiter.push("hello\nworld");
        delimiter.push("\n\n");

        assert_eq!(delimiter.next().unwrap(), Bytes::from("hello\n"));
        assert_eq!(delimiter.next().unwrap(), Bytes::from("world\n"));
        assert_eq!(delimiter.next().unwrap(), Bytes::from("\n"));
        assert!(delimiter.next().is_none());

        delimiter.push("");
        delimiter.push("fo\\\n\"foo");
        delimiter.push("bo\n\"bar\n");
        delimiter.push("\"hello\"\n");
        assert_eq!(
            delimiter.next().unwrap(),
            Bytes::from("fo\\\n\"foobo\n\"bar\n")
        );
        assert_eq!(delimiter.next().unwrap(), Bytes::from("\"hello\"\n"));
        assert!(delimiter.next().is_none());

        delimiter.push("\"foo\nbar\",\"fiz\\\"inner\\\"\"\nhello");
        assert!(!delimiter.finish().unwrap());

        assert_eq!(
            delimiter.next().unwrap(),
            Bytes::from("\"foo\nbar\",\"fiz\\\"inner\\\"\"\n")
        );
        assert_eq!(delimiter.next().unwrap(), Bytes::from("hello"));
        assert!(delimiter.finish().unwrap());
        assert!(delimiter.next().is_none());
    }

    #[tokio::test]
    async fn test_delimiter_stream() {
        let input = vec!["hello\nworld\nbin", "go\ncup", "cakes"];
        let input_stream =
            futures::stream::iter(input.into_iter().map(|s| Ok(Bytes::from(s))));
        let stream = newline_delimited_stream(input_stream);

        let results: Vec<_> = stream.try_collect().await.unwrap();
        assert_eq!(
            results,
            vec![
                Bytes::from("hello\nworld\n"),
                Bytes::from("bingo\n"),
                Bytes::from("cupcakes")
            ]
        )
    }
}
