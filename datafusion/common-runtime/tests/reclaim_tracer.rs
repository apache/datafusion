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

//! In its own test binary, since the tracer is registered process-wide.

use std::any::Any;
use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use datafusion_common_runtime::{JoinSetTracer, SpawnedTask, set_join_set_tracer};
use futures::future::BoxFuture;
use tokio::task::Id;

/// Task IDs a traced future saw when it was last polled and when it was dropped
type Observations = Arc<Mutex<Vec<(Option<Id>, Option<Id>)>>>;

struct RecordingTracer(Observations);

struct Traced {
    inner: BoxFuture<'static, Box<dyn Any + Send>>,
    polled_in: Option<Id>,
    observations: Observations,
}

impl Future for Traced {
    type Output = Box<dyn Any + Send>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.polled_in = tokio::task::try_id();
        self.inner.as_mut().poll(cx)
    }
}

impl Drop for Traced {
    fn drop(&mut self) {
        let dropped_in = tokio::task::try_id();
        self.observations
            .lock()
            .unwrap()
            .push((self.polled_in, dropped_in));
    }
}

impl JoinSetTracer for RecordingTracer {
    fn trace_future(
        &self,
        fut: BoxFuture<'static, Box<dyn Any + Send>>,
    ) -> BoxFuture<'static, Box<dyn Any + Send>> {
        Box::pin(Traced {
            inner: fut,
            polled_in: None,
            observations: Arc::clone(&self.0),
        })
    }

    fn trace_block(
        &self,
        f: Box<dyn FnOnce() -> Box<dyn Any + Send> + Send>,
    ) -> Box<dyn FnOnce() -> Box<dyn Any + Send> + Send> {
        f
    }
}

#[tokio::test]
async fn reclaimable_task_drops_its_tracer_inside_the_task() {
    let observations = Observations::default();
    let tracer = Box::leak(Box::new(RecordingTracer(Arc::clone(&observations))));
    set_join_set_tracer(tracer).unwrap();

    let task = SpawnedTask::spawn_reclaimable(pending::<()>());
    // Polled once through the tracer
    tokio::task::yield_now().await;
    drop(task);
    // Tokio drops the cancelled task, and the tracer with it
    tokio::task::yield_now().await;

    let (polled_in, dropped_in) = observations.lock().unwrap().pop().unwrap();
    assert!(polled_in.is_some());
    assert_eq!(dropped_in, polled_in);
}
