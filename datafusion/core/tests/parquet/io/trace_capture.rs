// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Captures `tracing` spans and events emitted by DataFusion during a test and
//! renders them as an indented tree for `insta` snapshots.
//!
//! Each line is the span name (or the event's `message`) followed by every
//! field the call site recorded, in call-site order:
//!
//! ```text
//! Load Bloom filters
//!     Read Bloom filter (column=a, row_group=0, range=98..145)
//!     ObjectStore::get_ranges (location=table.parquet, ranges=1)
//!         Read byte range (range=98..145, parquet=Bloom filter for column 'a' (rg 0))
//! ```
//!
//! The subscriber is installed with [`tracing::subscriber::set_default`], which
//! is thread-local. `#[tokio::test]` uses a current-thread runtime, so every
//! task of the query runs on the test thread and reports to this subscriber.

use std::fmt::{self, Write as _};
use std::sync::{Arc, Mutex};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record as SpanRecord};
use tracing::subscriber::DefaultGuard;
use tracing::{Event, Subscriber};
use tracing_subscriber::layer::{Context, Layer, SubscriberExt};
use tracing_subscriber::registry::{LookupSpan, Registry};

/// Only spans and events whose target starts with this prefix are captured.
/// Dependencies such as `h2` emit their own events.
const TARGET_PREFIX: &str = "datafusion";

/// One captured span (rendered where it opens) or event.
#[derive(Debug, Clone)]
struct Record {
    depth: usize,
    /// The span name, or the event's `message` field
    name: String,
    fields: Vec<(String, String)>,
}

#[derive(Default)]
struct FieldVisitor(Vec<(String, String)>);

impl Visit for FieldVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.push((field.name().to_string(), value.to_string()));
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.0
            .push((field.name().to_string(), format!("{value:?}")));
    }
}

/// Stored in a span's extensions: its index in the captured records.
struct SpanSlot {
    record: usize,
    depth: usize,
}

struct CaptureLayer {
    records: Arc<Mutex<Vec<Record>>>,
}

fn child_depth<S>(ctx: &Context<'_, S>, parent: Option<Id>) -> usize
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    parent
        .and_then(|id| ctx.span(&id))
        .and_then(|span| span.extensions().get::<SpanSlot>().map(|s| s.depth + 1))
        .unwrap_or(0)
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
        let depth = child_depth(&ctx, parent);
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);

        let mut records = self.records.lock().unwrap();
        records.push(Record {
            depth,
            name: metadata.name().to_string(),
            fields: visitor.0,
        });
        let span = ctx.span(id).expect("span exists");
        span.extensions_mut().insert(SpanSlot {
            record: records.len() - 1,
            depth,
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
        let depth = child_depth(&ctx, ctx.event_span(event).map(|s| s.id()));
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let mut fields = visitor.0;
        let name = fields
            .iter()
            .position(|(name, _)| name == "message")
            .map(|idx| fields.remove(idx).1)
            .unwrap_or_default();
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
    _guard: DefaultGuard,
}

impl TraceCapture {
    pub(crate) fn new() -> Self {
        let records = Arc::new(Mutex::new(Vec::new()));
        let subscriber = Registry::default().with(CaptureLayer {
            records: Arc::clone(&records),
        });
        let guard = tracing::subscriber::set_default(subscriber);
        Self {
            records,
            _guard: guard,
        }
    }

    /// Renders everything captured so far as an indented tree.
    pub(crate) fn render(&self) -> String {
        let records = self.records.lock().unwrap();
        let mut output = String::new();
        for record in records.iter() {
            let indent = "    ".repeat(record.depth);
            let _ = write!(output, "{indent}{}", record.name);
            if !record.fields.is_empty() {
                let fields: Vec<String> = record
                    .fields
                    .iter()
                    .map(|(name, value)| format!("{name}={value}"))
                    .collect();
                let _ = write!(output, " ({})", fields.join(", "));
            }
            output.push('\n');
        }
        output
    }
}
