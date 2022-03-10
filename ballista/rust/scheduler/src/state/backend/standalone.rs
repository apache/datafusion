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

use std::{sync::Arc, task::Poll};

use ballista_core::error::{ballista_error, BallistaError, Result};

use futures::{FutureExt, Stream};
use log::warn;
use sled_package as sled;
use tokio::sync::Mutex;

use crate::state::backend::{Lock, StateBackendClient, Watch, WatchEvent};

/// A [`StateBackendClient`] implementation that uses file-based storage to save cluster configuration.
#[derive(Clone)]
pub struct StandaloneClient {
    db: sled::Db,
    lock: Arc<Mutex<()>>,
}

impl StandaloneClient {
    /// Creates a StandaloneClient that saves data to the specified file.
    pub fn try_new<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        Ok(Self {
            db: sled::open(path).map_err(sled_to_ballista_error)?,
            lock: Arc::new(Mutex::new(())),
        })
    }

    /// Creates a StandaloneClient that saves data to a temp file.
    pub fn try_new_temporary() -> Result<Self> {
        Ok(Self {
            db: sled::Config::new()
                .temporary(true)
                .open()
                .map_err(sled_to_ballista_error)?,
            lock: Arc::new(Mutex::new(())),
        })
    }
}

fn sled_to_ballista_error(e: sled::Error) -> BallistaError {
    match e {
        sled::Error::Io(io) => BallistaError::IoError(io),
        _ => BallistaError::General(format!("{}", e)),
    }
}

#[tonic::async_trait]
impl StateBackendClient for StandaloneClient {
    async fn get(&self, key: &str) -> Result<Vec<u8>> {
        Ok(self
            .db
            .get(key)
            .map_err(|e| ballista_error(&format!("sled error {:?}", e)))?
            .map(|v| v.to_vec())
            .unwrap_or_default())
    }

    async fn get_from_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        Ok(self
            .db
            .scan_prefix(prefix)
            .map(|v| {
                v.map(|(key, value)| {
                    (
                        std::str::from_utf8(&key).unwrap().to_owned(),
                        value.to_vec(),
                    )
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| ballista_error(&format!("sled error {:?}", e)))?)
    }

    async fn put(&self, key: String, value: Vec<u8>) -> Result<()> {
        self.db
            .insert(key, value)
            .map_err(|e| {
                warn!("sled insert failed: {}", e);
                ballista_error("sled insert failed")
            })
            .map(|_| ())
    }

    async fn lock(&self) -> Result<Box<dyn Lock>> {
        Ok(Box::new(self.lock.clone().lock_owned().await))
    }

    async fn watch(&self, prefix: String) -> Result<Box<dyn Watch>> {
        Ok(Box::new(SledWatch {
            subscriber: self.db.watch_prefix(prefix),
        }))
    }
}

struct SledWatch {
    subscriber: sled::Subscriber,
}

#[tonic::async_trait]
impl Watch for SledWatch {
    async fn cancel(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Stream for SledWatch {
    type Item = WatchEvent;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.get_mut().subscriber.poll_unpin(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Ready(Some(sled::Event::Insert { key, value })) => {
                let key = std::str::from_utf8(&key).unwrap().to_owned();
                Poll::Ready(Some(WatchEvent::Put(key, value.to_vec())))
            }
            Poll::Ready(Some(sled::Event::Remove { key })) => {
                let key = std::str::from_utf8(&key).unwrap().to_owned();
                Poll::Ready(Some(WatchEvent::Delete(key)))
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.subscriber.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::{StandaloneClient, StateBackendClient, Watch, WatchEvent};

    use futures::StreamExt;
    use std::result::Result;

    fn create_instance() -> Result<StandaloneClient, Box<dyn std::error::Error>> {
        Ok(StandaloneClient::try_new_temporary()?)
    }

    #[tokio::test]
    async fn put_read() -> Result<(), Box<dyn std::error::Error>> {
        let client = create_instance()?;
        let key = "key";
        let value = "value".as_bytes();
        client.put(key.to_owned(), value.to_vec()).await?;
        assert_eq!(client.get(key).await?, value);
        Ok(())
    }

    #[tokio::test]
    async fn read_empty() -> Result<(), Box<dyn std::error::Error>> {
        let client = create_instance()?;
        let key = "key";
        let empty: &[u8] = &[];
        assert_eq!(client.get(key).await?, empty);
        Ok(())
    }

    #[tokio::test]
    async fn read_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let client = create_instance()?;
        let key = "key";
        let value = "value".as_bytes();
        client.put(format!("{}/1", key), value.to_vec()).await?;
        client.put(format!("{}/2", key), value.to_vec()).await?;
        assert_eq!(
            client.get_from_prefix(key).await?,
            vec![
                ("key/1".to_owned(), value.to_vec()),
                ("key/2".to_owned(), value.to_vec())
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_watch() -> Result<(), Box<dyn std::error::Error>> {
        let client = create_instance()?;
        let key = "key";
        let value = "value".as_bytes();
        let mut watch: Box<dyn Watch> = client.watch(key.to_owned()).await?;
        client.put(key.to_owned(), value.to_vec()).await?;
        assert_eq!(
            watch.next().await,
            Some(WatchEvent::Put(key.to_owned(), value.to_owned()))
        );
        let value2 = "value2".as_bytes();
        client.put(key.to_owned(), value2.to_vec()).await?;
        assert_eq!(
            watch.next().await,
            Some(WatchEvent::Put(key.to_owned(), value2.to_owned()))
        );
        watch.cancel().await?;
        Ok(())
    }
}
