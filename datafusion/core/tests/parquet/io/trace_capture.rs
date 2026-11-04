// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Captures `tracing` spans and events emitted by DataFusion during a test and
//! renders them as text for `insta` snapshots.
//!
//! The library emits events with a static message and structured fields
//! (`offset_start`, `offset_end`, `column`, `row_group`, ...). This module owns
//! the human-readable rendering, so the library never formats strings for
//! tests.
//!
//! The subscriber is installed with [`tracing::subscriber::set_default`], which
//! is thread-local. `#[tokio::test]` uses a current-thread runtime, so every
//! task of the query runs on the test thread and reports to this subscriber.

use std::collections::BTreeMap;
use std::fmt::{self, Write as _};
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record as SpanRecord};
use tracing::subscriber::DefaultGuard;
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::{LookupSpan, Registry};

use super::ParquetFileLayout;

/// Only spans and events from these target prefixes are captured. Dependencies
/// such as `h2` or `object_store` can emit their own events.
const TARGET_PREFIX: &str = "datafusion";

/// One captured span (rendered when it opens) or event.
#[derive(Debug, Clone)]
struct Record {
    /// Nesting level below the outermost captured span
    depth: usize,
    /// The span name, or the event's `message` field
    name: String,
    fields: BTreeMap<String, String>,
}

#[derive(Default)]
struct FieldVisitor(BTreeMap<String, String>);

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
}

/// Index into [`CaptureLayer::records`] for a span, plus the depth its children
/// get. Stored in the span's extensions.
struct SpanSlot {
    record: usize,
    child_depth: usize,
}

struct CaptureLayer {
    records: Arc<Mutex<Vec<Record>>>,
}

impl CaptureLayer {
    fn parent_depth<S>(ctx: &Context<'_, S>, parent: Option<Id>) -> usize
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        parent
            .and_then(|id| ctx.span(&id))
            .and_then(|span| span.extensions().get::<SpanSlot>().map(|s| s.child_depth))
            .unwrap_or(0)
    }
}

impl<S> Layer<S> for CaptureLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let metadata = attrs.metadata();
        if !metadata.target().starts_with(TARGET_PREFIX) {
            return;
        }
        let parent = if attrs.is_contextual() {
            ctx.current_span().id().cloned()
        } else {
            attrs.parent().cloned()
        };
        let depth = Self::parent_depth(&ctx, parent);
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);

        let mut records = self.records.lock().unwrap();
        records.push(Record {
            depth,
            name: metadata.name().to_string(),
            fields: visitor.0,
        });
        // The query span is the frame of the snapshot, so its children are not
        // indented.
        let child_depth = if metadata.name() == "query" {
            depth
        } else {
            depth + 1
        };
        let span = ctx.span(id).expect("span exists");
        span.extensions_mut().insert(SpanSlot {
            record: records.len() - 1,
            child_depth,
        });
    }

    fn on_record(&self, id: &Id, values: &SpanRecord<'_>, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else { return };
        let extensions = span.extensions();
        let Some(slot) = extensions.get::<SpanSlot>() else {
            return;
        };
        let mut visitor = FieldVisitor::default();
        values.record(&mut visitor);
        self.records.lock().unwrap()[slot.record]
            .fields
            .extend(visitor.0);
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if !event.metadata().target().starts_with(TARGET_PREFIX) {
            return;
        }
        let depth = Self::parent_depth(&ctx, ctx.event_span(event).map(|s| s.id()));
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let mut fields = visitor.0;
        let name = fields.remove("message").unwrap_or_default();
        self.records.lock().unwrap().push(Record {
            depth,
            name,
            fields,
        });
    }
}

/// Spans and events captured on this thread until it is dropped.
pub(crate) struct TraceCapture {
    records: Arc<Mutex<Vec<Record>>>,
    layout: ParquetFileLayout,
    file_size: u64,
    _guard: DefaultGuard,
}

impl TraceCapture {
    pub(super) fn new(layout: ParquetFileLayout, file_size: u64) -> Self {
        let records = Arc::new(Mutex::new(Vec::new()));
        let subscriber = Registry::default().with(CaptureLayer {
            records: Arc::clone(&records),
        });
        let guard = tracing::subscriber::set_default(subscriber);
        Self {
            records,
            layout,
            file_size,
            _guard: guard,
        }
    }

    /// Renders and clears everything captured so far.
    pub(crate) fn render(&self) -> String {
        let records = std::mem::take(&mut *self.records.lock().unwrap());
        let mut output = String::new();
        for record in records {
            let indent = "    ".repeat(record.depth);
            let _ = writeln!(output, "{indent}{}", self.render_record(&record));
        }
        output
    }

    fn render_record(&self, record: &Record) -> String {
        let field = |name: &str| record.fields.get(name).map(String::as_str);
        let offsets =
            field("offset_start")
                .zip(field("offset_end"))
                .map(|(start, end)| {
                    (start.parse::<u64>().unwrap(), end.parse::<u64>().unwrap())
                });

        match (record.name.as_str(), offsets) {
            ("query", _) => format!(
                "--- Query: {} ({} rows) ---",
                field("sql").unwrap_or_default(),
                field("rows").unwrap_or("?"),
            ),
            ("object_store_request", _) => {
                format!("Request {}", field("request").unwrap_or_default())
            }
            // The event names the structure it reads.
            (message, Some((start, end))) if field("column").is_some() => format!(
                "{message} for column '{}' (rg {}) (offsets {start}..{end})",
                field("column").unwrap(),
                field("row_group").unwrap_or("?"),
            ),
            // The event knows only the byte range. The file layout names the
            // structure that the range covers.
            (_, Some((start, end))) => format!(
                "Read {}",
                self.layout.describe_range(&(start..end), self.file_size)
            ),
            (message, None) => match field("rows") {
                Some(rows) => format!("{message} ({rows} rows)"),
                None => message.to_string(),
            },
        }
    }
}
