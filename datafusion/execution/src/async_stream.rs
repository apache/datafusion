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

use futures::Stream;
use futures::future::FusedFuture;
use futures::stream::FusedStream;
use pin_project_lite::pin_project;
use std::ops::DerefMut;
use std::pin::Pin;
use std::sync::{Arc};
use std::task::{Context, Poll};
use parking_lot::Mutex;

/// A handle for emitting values from an [`async_stream`] generator.
///
/// The generator closure receives an `Emitter<T>` as its argument.
pub struct Emitter<T> {
    slot: Arc<Mutex<Option<T>>>,
}

/// A handle for emitting values from an [`async_try_stream`] generator.
///
/// The generator closure receives a `TryEmitter<T, E>` as its argument.
pub struct TryEmitter<T, E> {
    slot: Arc<Mutex<Option<Result<T, E>>>>,
}

struct Receiver<T> {
    slot: Arc<Mutex<Option<T>>>,
}

impl<T> Emitter<T> {
    /// Returns a `Future` that emits `value` as the next stream item.
    ///
    /// The returned future **must be awaited immediately**. On its first poll it
    /// yields `Poll::Pending`, handing control back to the stream consumer so it
    /// can observe the emitted value. On the next poll (triggered by the
    /// consumer calling `poll_next` again) it completes with `Poll::Ready(())`,
    /// resuming the generator.
    ///
    /// # Panics
    ///
    /// Panics if `emit` is called a second time before the previous future has
    /// been awaited, because doing so would silently overwrite the unconsumed
    /// value.
    pub fn emit(&mut self, value: T) -> impl FusedFuture<Output = ()> {
        let mut guard = self.slot.lock();
        match guard.deref_mut() {
            Some(_) => panic!("Misuse: await was not called after calling emit"),
            slot => *slot = Some(value),
        }

        Emit { done: false }
    }
}

impl<T, E> TryEmitter<T, E> {
    /// Emits `Ok(value)` as the next stream item and suspends the generator.
    ///
    /// Behaves identically to [`Emitter::emit`]: the returned future must be
    /// awaited immediately and yields `Poll::Pending` on its first poll to
    /// transfer control to the stream consumer.
    ///
    /// # Panics
    ///
    /// Panics if called before the previous emit future has been awaited.
    pub fn emit(&mut self, value: T) -> impl FusedFuture<Output = ()> {
        let mut guard = self.slot.lock();
        match guard.deref_mut() {
            Some(_) => panic!("Misuse: await was not called after calling emit"),
            slot => *slot = Some(Ok::<T, E>(value)),
        }

        Emit { done: false }
    }
}

struct Emit {
    done: bool,
}

impl FusedFuture for Emit {
    fn is_terminated(&self) -> bool {
        self.done
    }
}

impl Future for Emit {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if !self.done {
            self.done = true;
            // Poll::Pending causes the generator to yield, returning control back to the
            // calling Stream
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

pin_project! {
    struct AsyncStream<T, U> {
        rx: Receiver<T>,
        done: bool,
        #[pin]
        generator: U,
    }
}

impl<T, U> AsyncStream<T, U> {
    fn new(rx: Receiver<T>, generator: U) -> AsyncStream<T, U> {
        AsyncStream {
            rx,
            done: false,
            generator,
        }
    }
}

impl<T, U> FusedStream for AsyncStream<T, U>
where
    U: Future<Output = ()>,
{
    fn is_terminated(&self) -> bool {
        self.done
    }
}

impl<T, U> Stream for AsyncStream<T, U>
where
    U: Future<Output = ()>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        if *this.done {
            return Poll::Ready(None);
        }

        // The `Option::take` call below ensures the next time poll is called the slot is
        // already set to None
        debug_assert!(this.rx.slot.lock().is_none());
        let res = this.generator.poll(cx);
        *this.done = res.is_ready();

        match this.rx.slot.lock().take() {
            // Generator filled slot -> return next stream item
            Some(v) => Poll::Ready(Some(v)),
            // Generator did not fill slot and completed -> return None to indicate end of stream
            None if *this.done => Poll::Ready(None),
            // Generator did not fill slot and not completed -> return Pending since some Future
            // other than Emit returned Pending.
            None => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.done { (0, Some(0)) } else { (0, None) }
    }
}

fn tx_rx<T>() -> (Emitter<T>, Receiver<T>) {
    let slot = Arc::new(Mutex::new(None));
    (
        Emitter {
            slot: Arc::clone(&slot),
        },
        Receiver { slot },
    )
}

/// Creates a [`Stream`] from an async generator function.
///
/// The `generator` closure receives an [`Emitter<T>`] and runs as an async
/// block. Each `emitter.emit(value).await` call suspends the generator and
/// produces the next item in the stream. The stream ends when the generator
/// future resolves.
///
/// # Example
///
/// ```
/// use datafusion_execution::async_stream;
/// use futures::StreamExt;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let stream = async_stream(|mut emitter| async move {
///     for i in 0_i32..3 {
///         emitter.emit(i).await;
///     }
/// });
///
/// let values: Vec<i32> = stream.collect().await;
/// assert_eq!(values, vec![0, 1, 2]);
/// # }
/// ```
pub fn async_stream<T, F: Future<Output = ()>>(
    generator: impl FnOnce(Emitter<T>) -> F,
) -> impl FusedStream<Item = T> {
    let (emitter, receiver) = tx_rx();
    AsyncStream::new(receiver, generator(emitter))
}

#[expect(
    clippy::type_complexity,
    reason = "three-element tuple is clearer than an alias here"
)]
fn try_tx_rx<T, E>() -> (
    TryEmitter<T, E>,
    Emitter<Result<T, E>>,
    Receiver<Result<T, E>>,
) {
    let slot = Arc::new(Mutex::new(None));
    (
        TryEmitter {
            slot: Arc::clone(&slot),
        },
        Emitter {
            slot: Arc::clone(&slot),
        },
        Receiver { slot },
    )
}

/// Creates a fallible [`Stream`] from an async generator function.
///
/// The `generator` closure receives a [`TryEmitter<T, E>`] and runs as an
/// async block that returns `Result<(), E>`. Each `emitter.emit(value).await`
/// call suspends the generator and produces `Ok(value)` as the next stream
/// item. The `?` operator can be used inside the generator to short-circuit on
/// errors: the error is emitted as the final `Err(e)` item and the stream
/// ends. The stream also ends when the generator future resolves to `Ok(())`.
///
/// # Example
///
/// ```
/// use datafusion_execution::async_try_stream;
/// use futures::StreamExt;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let stream = async_try_stream(|mut emitter| async move {
///     emitter.emit(1_i32).await;
///     emitter.emit(2_i32).await;
///     Err::<(), _>("something went wrong")?;
///     emitter.emit(3_i32).await; // never reached
///     Ok(())
/// });
///
/// let values: Vec<Result<i32, &str>> = stream.collect().await;
/// assert_eq!(values, vec![Ok(1), Ok(2), Err("something went wrong")]);
/// # }
/// ```
pub fn async_try_stream<T, E, F: Future<Output = Result<(), E>>>(
    generator: impl FnOnce(TryEmitter<T, E>) -> F,
) -> impl FusedStream<Item = Result<T, E>> {
    let (try_emitter, mut emitter, receiver) = try_tx_rx::<T, E>();
    AsyncStream::new(receiver, async move {
        if let Err(e) = generator(try_emitter).await {
            emitter.emit(Err(e)).await
        }
    })
}

#[cfg(test)]
mod test {
    use crate::async_stream::Emitter;
    use crate::{async_stream, async_try_stream};
    use futures::stream::FusedStream;
    use futures::{Stream, StreamExt, pin_mut};
    use std::assert_matches;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn noop_stream() {
        let s = async_stream(|_: Emitter<()>| async {});
        pin_mut!(s);

        assert_eq!(s.next().await, None);
    }

    #[tokio::test]
    async fn empty_stream() {
        let mut ran = false;

        {
            let r = &mut ran;
            let s = async_stream(|_: Emitter<()>| async {
                *r = true;
                println!("hello world!");
            });
            pin_mut!(s);

            assert_eq!(s.next().await, None);
        }

        assert!(ran);
    }

    #[tokio::test]
    async fn emit_single_value() {
        let s = async_stream(|mut emitter| async move {
            emitter.emit("hello").await;
        });

        let values: Vec<_> = s.collect().await;

        assert_eq!(1, values.len());
        assert_eq!("hello", values[0]);
    }

    #[tokio::test]
    async fn fused() {
        let s = async_stream(|mut emitter| async move {
            emitter.emit("hello").await;
        });
        pin_mut!(s);

        assert!(!s.is_terminated());
        assert_eq!(s.next().await, Some("hello"));
        assert_eq!(s.next().await, None);

        assert!(s.is_terminated());
        // This should return None from now on
        assert_eq!(s.next().await, None);
    }

    #[tokio::test]
    async fn emit_multi_value() {
        let s = async_stream(|mut emitter| async move {
            emitter.emit("hello").await;
            emitter.emit("world").await;
            emitter.emit("dizzy").await;
        });

        let values: Vec<_> = s.collect().await;

        assert_eq!(3, values.len());
        assert_eq!("hello", values[0]);
        assert_eq!("world", values[1]);
        assert_eq!("dizzy", values[2]);
    }

    #[tokio::test]
    #[should_panic]
    async fn emit_without_await() {
        let s = async_stream(|mut emitter| async move {
            let _ = emitter.emit("hello");
            let _ = emitter.emit("world");
        });

        let _: Vec<_> = s.collect().await;
    }

    #[tokio::test]
    async fn unit_emit_in_select() {
        use tokio::select;

        async fn do_stuff_async() {}

        let s = async_stream(|mut emitter| async move {
            select! {
                _ = do_stuff_async() => emitter.emit(()).await,
                else => emitter.emit(()).await,
            }
        });

        let values: Vec<_> = s.collect().await;
        assert_eq!(values.len(), 1);
    }

    #[tokio::test]
    async fn emit_with_select() {
        use tokio::select;

        async fn do_stuff_async() {}
        async fn more_async_work() {}

        let s = async_stream(|mut emitter| async move {
            select! {
                _ = do_stuff_async() => emitter.emit("hey").await,
                _ = more_async_work() => emitter.emit("hey").await,
                else => emitter.emit("hey").await,
            }
        });

        let values: Vec<_> = s.collect().await;
        assert_eq!(values, vec!["hey"]);
    }

    #[tokio::test]
    async fn return_stream() {
        fn build_stream() -> impl Stream<Item = u32> {
            async_stream(|mut emitter| async move {
                emitter.emit(1).await;
                emitter.emit(2).await;
                emitter.emit(3).await;
            })
        }

        let s = build_stream();

        let values: Vec<_> = s.collect().await;
        assert_eq!(3, values.len());
        assert_eq!(1, values[0]);
        assert_eq!(2, values[1]);
        assert_eq!(3, values[2]);
    }

    #[tokio::test]
    async fn consume_channel() {
        let (tx, mut rx) = mpsc::channel(10);

        let s = async_stream(|mut emitter| async move {
            while let Some(v) = rx.recv().await {
                emitter.emit(v).await;
            }
        });

        pin_mut!(s);

        for i in 0..3 {
            assert_matches!(tx.send(i).await, Ok(_));
            assert_eq!(Some(i), s.next().await);
        }

        drop(tx);
        assert_eq!(None, s.next().await);
    }

    #[tokio::test]
    async fn borrow_self() {
        struct Data(String);

        impl Data {
            fn stream(&self) -> impl Stream<Item = &str> + '_ {
                async_stream(move |mut emitter| async move {
                    emitter.emit(&self.0[..]).await;
                })
            }
        }

        let data = Data("hello".to_string());
        let s = data.stream();
        pin_mut!(s);

        assert_eq!(Some("hello"), s.next().await);
    }

    #[tokio::test]
    async fn stream_in_stream() {
        let s = async_stream(|mut emitter| async move {
            let s = async_stream(|mut inner_emitter| async move {
                for i in 0..3 {
                    inner_emitter.emit(i).await;
                }
            });

            pin_mut!(s);
            while let Some(v) = s.next().await {
                emitter.emit(v).await;
            }
        });

        let values: Vec<_> = s.collect().await;
        assert_eq!(3, values.len());
    }

    // Demonstrates that capturing an outer Emitter<T> inside an inner async_stream with a
    // different item type is no longer undefined behaviour: the outer emitter writes to its own
    // typed slot, so the inner stream never sees any values.  The outer stream receives the
    // "foo" strings instead because they land in its slot.
    #[tokio::test]
    async fn stream_in_stream_misuse() {
        let s = async_stream(|mut emitter| async move {
            let s = async_stream(|_inner_emitter: Emitter<i32>| async move {
                for _i in 0..3 {
                    emitter.emit("foo").await;
                }
            });

            pin_mut!(s);
            while let Some(v) = s.next().await {
                println!("{}", v);
            }
        });

        let values: Vec<_> = s.collect().await;
        assert_eq!(3, values.len());
    }

    #[tokio::test]
    async fn emit_non_unpin_value() {
        let s: Vec<_> = async_stream(|mut emitter| async move {
            for i in 0..3 {
                emitter.emit(async move { i }).await;
            }
        })
        .buffered(1)
        .collect()
        .await;

        assert_eq!(s, vec![0, 1, 2]);
    }

    #[test]
    fn inner_try_stream() {
        use tokio::select;

        async fn do_stuff_async() {}

        let _ = async_stream(|mut emitter| async move {
            select! {
                _ = do_stuff_async() => {
                    let another_s = async_try_stream(|mut inner_emitter| async move {
                        inner_emitter.emit(()).await;
                        Ok(())
                    });
                    let _: Result<(), ()> = Box::pin(another_s).next().await.unwrap();
                },
                else => {},
            }
            emitter.emit(()).await;
        });
    }

    #[tokio::test]
    async fn single_err() {
        let s = async_try_stream(|mut emitter| async move {
            if true {
                Err("hello")?;
            } else {
                emitter.emit("world").await;
            }

            unreachable!();
        });

        let values: Vec<_> = s.collect().await;
        assert_eq!(1, values.len());
        assert_eq!(Err("hello"), values[0]);
    }

    #[tokio::test]
    async fn emit_then_err() {
        let s = async_try_stream(|mut emitter| async move {
            emitter.emit("hello").await;
            Err("world")?;
            unreachable!();
        });

        let values: Vec<_> = s.collect().await;
        assert_eq!(2, values.len());
        assert_eq!(Ok("hello"), values[0]);
        assert_eq!(Err("world"), values[1]);
    }

    #[tokio::test]
    async fn convert_err() {
        struct ErrorA(u8);
        #[derive(PartialEq, Debug)]
        struct ErrorB(u8);
        impl From<ErrorA> for ErrorB {
            fn from(a: ErrorA) -> ErrorB {
                ErrorB(a.0)
            }
        }

        fn test() -> impl Stream<Item = Result<&'static str, ErrorB>> {
            async_try_stream(|mut emitter| async move {
                if true {
                    Err(ErrorA(1))?;
                } else {
                    Err(ErrorB(2))?;
                }
                emitter.emit("unreachable").await;
                Ok(())
            })
        }

        let values: Vec<_> = test().collect().await;
        assert_eq!(1, values.len());
        assert_eq!(Err(ErrorB(1)), values[0]);
    }

    #[tokio::test]
    async fn multi_try() {
        fn test() -> impl Stream<Item = Result<i32, String>> {
            async_try_stream(|mut emitter| async move {
                let a = Ok::<_, String>(Ok::<_, String>(123))??;
                for _ in 1..10 {
                    emitter.emit(a).await;
                }
                Ok(())
            })
        }
        let values: Vec<_> = test().collect().await;
        assert_eq!(9, values.len());
        assert_eq!(
            std::iter::repeat_n(123, 9).map(Ok).collect::<Vec<_>>(),
            values
        );
    }
}
