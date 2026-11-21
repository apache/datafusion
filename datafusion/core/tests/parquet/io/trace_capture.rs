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
//! One global subscriber records into the capture that is active on the
//! current thread. `#[tokio::test]` uses a current-thread runtime, so every
//! task of the query runs on the test thread.
//!
//! A thread-local subscriber (`tracing::subscriber::set_default`) is not used.
//! `tracing` caches per-callsite interest across threads. A test thread without
//! a subscriber can then disable a callsite for every other test thread, and
//! events go missing at random when tests run in parallel.

use std::cell::RefCell;
use std::fmt::{self, Write as _};
use std::sync::{Arc, Mutex, Once};

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record as SpanRecord};
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

type Records = Arc<Mutex<Vec<Record>>>;

thread_local! {
    /// The records of the capture that is active on this thread
    static ACTIVE: RefCell<Option<Records>> = const { RefCell::new(None) };
}

fn active() -> Option<Records> {
    ACTIVE.with(|active| active.borrow().clone())
}

/// Stored in a span's extensions: where the span's record lives.
struct SpanSlot {
    records: Records,
    record: usize,
    depth: usize,
}

struct CaptureLayer;

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
        let Some(records) = active() else { return };
        let parent = if attrs.is_contextual() {
            ctx.current_span().id().cloned()
        } else {
            attrs.parent().cloned()
        };
        let depth = child_depth(&ctx, parent);
        let mut visitor = FieldVisitor::default();
        attrs.record(&mut visitor);

        let record = {
            let mut records = records.lock().unwrap();
            records.push(Record {
                depth,
                name: metadata.name().to_string(),
                fields: visitor.0,
            });
            records.len() - 1
        };
        let span = ctx.span(id).expect("span exists");
        span.extensions_mut().insert(SpanSlot {
            records,
            record,
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
        slot.records.lock().unwrap()[slot.record]
            .fields
            .extend(visitor.0);
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if !event.metadata().target().starts_with(TARGET_PREFIX) {
            return;
        }
        let Some(records) = active() else { return };
        let depth = child_depth(&ctx, ctx.event_span(event).map(|s| s.id()));
        let mut visitor = FieldVisitor::default();
        event.record(&mut visitor);
        let mut fields = visitor.0;
        let name = fields
            .iter()
            .position(|(name, _)| name == "message")
            .map(|idx| fields.remove(idx).1)
            .unwrap_or_default();
        records.lock().unwrap().push(Record {
            depth,
            name,
            fields,
        });
    }
}

/// Spans and events captured on this thread until it is dropped.
pub(crate) struct TraceCapture {
    records: Records,
}

impl TraceCapture {
    pub(crate) fn new() -> Self {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            tracing::subscriber::set_global_default(
                Registry::default().with(CaptureLayer),
            )
            .expect("no other global subscriber in this test binary");
        });
        let records = Records::default();
        ACTIVE.with(|active| {
            let previous = active.borrow_mut().replace(Arc::clone(&records));
            assert!(previous.is_none(), "one TraceCapture per thread");
        });
        Self { records }
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

impl Drop for TraceCapture {
    fn drop(&mut self) {
        ACTIVE.with(|active| active.borrow_mut().take());
    }
}
