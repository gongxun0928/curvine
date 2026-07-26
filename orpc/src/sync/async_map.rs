// Copyright 2025 OPPO.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::err_box;
use crate::sync::FastDashMap;
use std::error::Error;
use std::fmt::Display;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::Mutex;

struct SharedState<T> {
    resource: Option<Arc<T>>,
    refs: usize,
}

impl<T> Default for SharedState<T> {
    fn default() -> Self {
        Self {
            resource: None,
            refs: 0,
        }
    }
}

/// Per-key async shared resource map with reference counting.
///
/// One async mutex per key serializes create/release work. This keeps the
/// implementation small and avoids exposing resources that are being closed.
///
/// The futures passed to `get_or_create`, `with_resource`, `release`, and
/// `release_with_cleanup` must not call back into this map with the same key;
/// doing so would wait on the same per-key mutex.
pub struct AsyncSharedMap<K, T> {
    inner: Arc<FastDashMap<K, Arc<Mutex<SharedState<T>>>>>,
}

impl<K: Eq + Hash, T> Default for AsyncSharedMap<K, T> {
    fn default() -> Self {
        Self {
            inner: Arc::new(FastDashMap::default()),
        }
    }
}

impl<K: Eq + Hash + Display + Clone, T> AsyncSharedMap<K, T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn keys(&self) -> Vec<K> {
        self.inner.iter().map(|entry| entry.key().clone()).collect()
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.contains_key(key)
    }

    /// Return whether a key has an active resource or an operation currently
    /// owns its per-key lock. An abandoned empty entry is not considered active.
    pub fn has_resource_or_pending(&self, key: &K) -> bool {
        loop {
            let Some(entry) = self.inner.get(key).map(|entry| entry.clone()) else {
                return false;
            };
            let Ok(state) = entry.try_lock() else {
                return true;
            };
            if !self.is_current_entry(key, &entry) {
                continue;
            }
            return state.resource.is_some() || state.refs > 0;
        }
    }

    fn get_entry(&self, k: K) -> Arc<Mutex<SharedState<T>>> {
        self.inner
            .entry(k)
            .or_insert_with(|| Arc::new(Mutex::new(SharedState::default())))
            .clone()
    }

    fn is_current_entry(&self, key: &K, entry: &Arc<Mutex<SharedState<T>>>) -> bool {
        self.inner
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(&current, entry))
    }

    pub async fn insert<E>(&self, key: K, resource: Arc<T>) -> Result<Arc<T>, E>
    where
        E: Error + From<String>,
    {
        loop {
            let entry = self.get_entry(key.clone());
            let mut state = entry.lock().await;
            if !self.is_current_entry(&key, &entry) {
                continue;
            }

            if state.resource.is_some() {
                return err_box!("resource already exists for this key {}", key);
            }
            state.refs = 1;
            state.resource = Some(resource.clone());
            return Ok(resource);
        }
    }

    pub async fn get(&self, key: &K) -> Option<Arc<T>> {
        loop {
            let entry = self.inner.get(key)?.clone();
            let state = entry.lock().await;
            if !self.is_current_entry(key, &entry) {
                continue;
            }
            return state.resource.clone();
        }
    }

    pub async fn get_or_create<E, Fut>(&self, key: K, fut: Fut) -> Result<Arc<T>, E>
    where
        E: Error,
        Fut: Future<Output = Result<Arc<T>, E>>,
    {
        loop {
            let entry = self.get_entry(key.clone());
            let mut state = entry.lock().await;
            if !self.is_current_entry(&key, &entry) {
                continue;
            }

            if let Some(resource) = state.resource.clone() {
                state.refs += 1;
                return Ok(resource);
            }

            let resource = match fut.await {
                Ok(resource) => resource,
                Err(e) => {
                    self.remove_entry(&key, &entry);
                    drop(state);
                    return Err(e);
                }
            };

            state.refs = 1;
            state.resource = Some(resource.clone());
            return Ok(resource);
        }
    }

    pub async fn with_resource<E, F, Fut>(&self, key: &K, f: F) -> Result<bool, E>
    where
        E: Error,
        F: FnOnce(Arc<T>) -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        loop {
            let entry = match self.inner.get(key) {
                Some(entry) => entry.clone(),
                None => return Ok(false),
            };

            let state = entry.lock().await;
            if !self.is_current_entry(key, &entry) {
                continue;
            }

            let Some(resource) = state.resource.clone() else {
                return Ok(false);
            };

            f(resource).await?;
            return Ok(true);
        }
    }

    fn remove_entry(&self, key: &K, entry: &Arc<Mutex<SharedState<T>>>) {
        self.inner
            .remove_if(key, |_, current| Arc::ptr_eq(current, entry));
    }

    pub fn remove(&self, key: K) {
        self.inner.remove(&key);
    }

    pub async fn release<E, Fut>(&self, key: K, fut: Fut) -> (bool, Result<(), E>)
    where
        E: Error,
        Fut: Future<Output = Result<(), E>>,
    {
        let entry = match self.inner.get(&key) {
            Some(entry) => entry.clone(),
            None => return (true, Ok(())),
        };

        let mut state = entry.lock().await;

        if state.refs == 0 || state.resource.is_none() {
            self.remove_entry(&key, &entry);
            drop(state);
            return (true, Ok(()));
        }

        state.refs -= 1;
        if state.refs > 0 {
            return (false, Ok(()));
        }

        // Hide the resource before cleanup so late users wait for a fresh create.
        state.resource.take();
        state.refs = 0;

        let res = fut.await;
        self.remove_entry(&key, &entry);
        drop(state);

        (true, res)
    }

    /// Release one reference after attempting its cleanup.
    ///
    /// `cleanup` receives whether this is the last reference, allowing callers
    /// to choose between per-reference cleanup (for example, flush) and final
    /// cleanup (for example, complete). The reference is consumed even when
    /// cleanup fails because the owner has already released it. The cleanup
    /// result is returned separately so callers can report or schedule retry
    /// work without keeping the resource in the active map.
    pub async fn release_with_cleanup<E, F, Fut>(&self, key: K, cleanup: F) -> (bool, Result<(), E>)
    where
        E: Error,
        F: FnOnce(bool) -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let entry = match self.inner.get(&key) {
            Some(entry) => entry.clone(),
            None => return (true, Ok(())),
        };

        let mut state = entry.lock().await;

        if state.refs == 0 || state.resource.is_none() {
            self.remove_entry(&key, &entry);
            drop(state);
            return (true, Ok(()));
        }

        let last = state.refs == 1;
        let result = cleanup(last).await;

        state.refs -= 1;
        if state.refs > 0 {
            return (false, result);
        }

        state.resource.take();
        self.remove_entry(&key, &entry);
        drop(state);

        (true, result)
    }

    /// Release one reference after cleanup, blocking replacement creation while
    /// a failed final resource drains in the background.
    ///
    /// On a successful final cleanup this behaves like `release_with_cleanup`.
    /// On a failed final cleanup, the active resource is removed immediately,
    /// but its per-key mutex remains locked until `drain` completes. This lets
    /// the caller release ownership without admitting a second resource for the
    /// same key while asynchronous teardown is still running.
    pub async fn release_with_cleanup_and_drain<E, F, Fut, D>(
        &self,
        key: K,
        cleanup: F,
        drain: D,
    ) -> (bool, Result<(), E>)
    where
        K: Send + Sync + 'static,
        T: Send + Sync + 'static,
        E: Error,
        F: FnOnce(bool) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        D: Future<Output = ()> + Send + 'static,
    {
        let entry = match self.inner.get(&key) {
            Some(entry) => entry.clone(),
            None => return (true, Ok(())),
        };

        let mut state = entry.clone().lock_owned().await;

        if state.refs == 0 || state.resource.is_none() {
            self.remove_entry(&key, &entry);
            drop(state);
            return (true, Ok(()));
        }

        let last = state.refs == 1;
        let result = cleanup(last).await;

        state.refs -= 1;
        if state.refs > 0 {
            return (false, result);
        }

        let resource = state.resource.take();
        if result.is_ok() {
            drop(resource);
            self.remove_entry(&key, &entry);
            drop(state);
            return (true, result);
        }

        state.refs = 0;
        let inner = self.inner.clone();
        let entry_for_remove = entry.clone();
        drop(tokio::spawn(async move {
            // The drain future must not own the resource. Dropping the map's
            // reference here lets the caller's remaining owners control when
            // the underlying asynchronous task can actually exit.
            drop(resource);
            drain.await;
            inner.remove_if(&key, |_, current| Arc::ptr_eq(current, &entry_for_remove));
            // Remove the entry before unlocking it. A waiter that already
            // cloned this entry will observe that it is stale and retry
            // instead of publishing a replacement that this task removes.
            drop(state);
        }));

        (true, result)
    }
}

#[cfg(test)]
mod tests {
    use super::AsyncSharedMap;
    use crate::error::StringError;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::{oneshot, Barrier};
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn get_or_create_runs_single_creator_per_key() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        let creators = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(8));

        let mut tasks = Vec::new();
        for _ in 0..8 {
            let map = map.clone();
            let creators = creators.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                map.get_or_create(1, async {
                    creators.fetch_add(1, Ordering::SeqCst);
                    tokio::task::yield_now().await;
                    Ok::<_, StringError>(Arc::new(7))
                })
                .await
                .unwrap()
            }));
        }

        for task in tasks {
            assert_eq!(*task.await.unwrap(), 7);
        }
        assert_eq!(creators.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn get_or_create_waits_for_release_cleanup() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        map.insert::<StringError>(1, Arc::new(1)).await.unwrap();

        let (cleanup_started_tx, cleanup_started_rx) = oneshot::channel();
        let (finish_cleanup_tx, finish_cleanup_rx) = oneshot::channel();
        let release_map = map.clone();
        let release_task = tokio::spawn(async move {
            let (released, cleanup_res) = release_map
                .release(1, async {
                    cleanup_started_tx.send(()).unwrap();
                    finish_cleanup_rx.await.unwrap();
                    Ok::<_, StringError>(())
                })
                .await;
            cleanup_res.unwrap();
            released
        });

        cleanup_started_rx.await.unwrap();

        let creators = Arc::new(AtomicUsize::new(0));
        let create_map = map.clone();
        let create_count = creators.clone();
        let create_task = tokio::spawn(async move {
            create_map
                .get_or_create(1, async {
                    create_count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, StringError>(Arc::new(2))
                })
                .await
                .unwrap()
        });

        tokio::task::yield_now().await;
        assert_eq!(creators.load(Ordering::SeqCst), 0);

        finish_cleanup_tx.send(()).unwrap();
        assert!(release_task.await.unwrap());
        assert_eq!(*create_task.await.unwrap(), 2);
        assert_eq!(creators.load(Ordering::SeqCst), 1);
        assert_eq!(*map.get(&1).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn release_closes_only_last_reference() {
        let map = AsyncSharedMap::<u64, u64>::new();
        let first = map
            .get_or_create(1, async { Ok::<_, StringError>(Arc::new(1)) })
            .await
            .unwrap();
        let second = map
            .get_or_create(1, async { Ok::<_, StringError>(Arc::new(2)) })
            .await
            .unwrap();

        assert!(Arc::ptr_eq(&first, &second));
        let (released, cleanup_res) = map.release(1, async { Ok::<_, StringError>(()) }).await;
        cleanup_res.unwrap();
        assert!(!released);
        assert!(map.get(&1).await.is_some());
        let (released, cleanup_res) = map.release(1, async { Ok::<_, StringError>(()) }).await;
        cleanup_res.unwrap();
        assert!(released);
        assert!(map.get(&1).await.is_none());
    }

    #[tokio::test]
    async fn create_error_wakes_waiter_and_allows_retry() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        let (fail_started_tx, fail_started_rx) = oneshot::channel();
        let (finish_fail_tx, finish_fail_rx) = oneshot::channel();

        let first_map = map.clone();
        let first = tokio::spawn(async move {
            first_map
                .get_or_create(1, async {
                    fail_started_tx.send(()).unwrap();
                    finish_fail_rx.await.unwrap();
                    Err::<Arc<u64>, _>(StringError::from("create failed"))
                })
                .await
        });

        fail_started_rx.await.unwrap();

        let retries = Arc::new(AtomicUsize::new(0));
        let second_map = map.clone();
        let retry_count = retries.clone();
        let second = tokio::spawn(async move {
            second_map
                .get_or_create(1, async {
                    retry_count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, StringError>(Arc::new(2))
                })
                .await
                .unwrap()
        });

        tokio::task::yield_now().await;
        assert_eq!(retries.load(Ordering::SeqCst), 0);

        finish_fail_tx.send(()).unwrap();
        assert!(first.await.unwrap().is_err());
        assert_eq!(
            *timeout(Duration::from_secs(1), second)
                .await
                .unwrap()
                .unwrap(),
            2
        );
        assert_eq!(retries.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelled_create_does_not_leave_entry_busy() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        let (started_tx, started_rx) = oneshot::channel();
        let (_finish_tx, finish_rx) = oneshot::channel::<()>();

        let create_map = map.clone();
        let creating = tokio::spawn(async move {
            create_map
                .get_or_create(1, async {
                    started_tx.send(()).unwrap();
                    finish_rx.await.unwrap();
                    Ok::<_, StringError>(Arc::new(1))
                })
                .await
        });

        started_rx.await.unwrap();
        creating.abort();
        assert!(creating.await.unwrap_err().is_cancelled());
        assert!(!map.has_resource_or_pending(&1));

        let resource = timeout(
            Duration::from_secs(1),
            map.get_or_create(1, async { Ok::<_, StringError>(Arc::new(2)) }),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(*resource, 2);
    }

    #[tokio::test]
    async fn cancelled_release_does_not_leave_entry_busy() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        map.insert::<StringError>(1, Arc::new(1)).await.unwrap();

        let (started_tx, started_rx) = oneshot::channel();
        let (_finish_tx, finish_rx) = oneshot::channel::<()>();
        let release_map = map.clone();
        let releasing = tokio::spawn(async move {
            release_map
                .release(1, async {
                    started_tx.send(()).unwrap();
                    finish_rx.await.unwrap();
                    Ok::<_, StringError>(())
                })
                .await
        });

        started_rx.await.unwrap();
        releasing.abort();
        assert!(releasing.await.unwrap_err().is_cancelled());

        let resource = timeout(
            Duration::from_secs(1),
            map.get_or_create(1, async { Ok::<_, StringError>(Arc::new(2)) }),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(*resource, 2);
    }

    #[tokio::test]
    async fn release_empty_entry_does_not_run_cleanup() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        let (started_tx, started_rx) = oneshot::channel();
        let (_finish_tx, finish_rx) = oneshot::channel::<()>();

        let create_map = map.clone();
        let creating = tokio::spawn(async move {
            create_map
                .get_or_create(1, async {
                    started_tx.send(()).unwrap();
                    finish_rx.await.unwrap();
                    Ok::<_, StringError>(Arc::new(1))
                })
                .await
        });

        started_rx.await.unwrap();
        creating.abort();
        assert!(creating.await.unwrap_err().is_cancelled());

        let cleanup_calls = Arc::new(AtomicUsize::new(0));
        let cleanup_count = cleanup_calls.clone();
        let (released, cleanup_res) = map
            .release(1, async {
                cleanup_count.fetch_add(1, Ordering::SeqCst);
                Ok::<_, StringError>(())
            })
            .await;
        cleanup_res.unwrap();
        assert!(released);
        assert_eq!(cleanup_calls.load(Ordering::SeqCst), 0);
        assert!(map.is_empty());
    }

    #[tokio::test]
    async fn release_error_wakes_waiter_and_allows_retry() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        map.insert::<StringError>(1, Arc::new(1)).await.unwrap();

        let (cleanup_started_tx, cleanup_started_rx) = oneshot::channel();
        let (finish_cleanup_tx, finish_cleanup_rx) = oneshot::channel();
        let release_map = map.clone();
        let release_task = tokio::spawn(async move {
            release_map
                .release(1, async {
                    cleanup_started_tx.send(()).unwrap();
                    finish_cleanup_rx.await.unwrap();
                    Err::<(), _>(StringError::from("cleanup failed"))
                })
                .await
        });

        cleanup_started_rx.await.unwrap();

        let creators = Arc::new(AtomicUsize::new(0));
        let create_map = map.clone();
        let create_count = creators.clone();
        let create_task = tokio::spawn(async move {
            create_map
                .get_or_create(1, async {
                    create_count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, StringError>(Arc::new(2))
                })
                .await
                .unwrap()
        });

        tokio::task::yield_now().await;
        assert_eq!(creators.load(Ordering::SeqCst), 0);

        finish_cleanup_tx.send(()).unwrap();
        let (released, cleanup_res) = release_task.await.unwrap();
        assert!(released);
        assert!(cleanup_res.is_err());
        assert_eq!(
            *timeout(Duration::from_secs(1), create_task)
                .await
                .unwrap()
                .unwrap(),
            2
        );
        assert_eq!(creators.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn release_with_cleanup_error_releases_last_reference() {
        let map = AsyncSharedMap::<u64, u64>::new();
        map.insert::<StringError>(1, Arc::new(7)).await.unwrap();

        let (released, result) = map
            .release_with_cleanup(1, |last| async move {
                assert!(last);
                Err::<(), _>(StringError::from("cleanup failed"))
            })
            .await;
        assert!(released);
        assert!(result.is_err());
        assert!(map.get(&1).await.is_none());
    }

    #[tokio::test]
    async fn release_with_cleanup_error_decrements_shared_reference() {
        let map = AsyncSharedMap::<u64, u64>::new();
        let first = map
            .get_or_create(1, async { Ok::<_, StringError>(Arc::new(7)) })
            .await
            .unwrap();
        let second = map
            .get_or_create(1, async { Ok::<_, StringError>(Arc::new(8)) })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));

        let (released, result) = map
            .release_with_cleanup(1, |last| async move {
                assert!(!last);
                Err::<(), _>(StringError::from("flush failed"))
            })
            .await;
        assert!(!released);
        assert!(result.is_err());
        assert!(map.get(&1).await.is_some());

        let (released, result) = map
            .release_with_cleanup(1, |last| async move {
                assert!(last);
                Ok::<_, StringError>(())
            })
            .await;
        assert!(released);
        assert!(result.is_ok());
        assert!(map.get(&1).await.is_none());
    }

    #[tokio::test]
    async fn failed_final_cleanup_blocks_replacement_until_drain_finishes() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        map.insert::<StringError>(1, Arc::new(7)).await.unwrap();
        let (finish_drain_tx, finish_drain_rx) = oneshot::channel();

        let (released, result) = map
            .release_with_cleanup_and_drain(
                1,
                |last| async move {
                    assert!(last);
                    Err::<(), _>(StringError::from("cleanup failed"))
                },
                async move {
                    let _ = finish_drain_rx.await;
                },
            )
            .await;
        assert!(released);
        assert!(result.is_err());
        assert!(map.contains_key(&1));
        assert!(map.has_resource_or_pending(&1));

        let creators = Arc::new(AtomicUsize::new(0));
        let create_map = map.clone();
        let create_count = creators.clone();
        let mut create_task = tokio::spawn(async move {
            create_map
                .get_or_create(1, async {
                    create_count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, StringError>(Arc::new(8))
                })
                .await
                .unwrap()
        });

        assert!(timeout(Duration::from_millis(50), &mut create_task)
            .await
            .is_err());
        assert_eq!(creators.load(Ordering::SeqCst), 0);

        finish_drain_tx.send(()).unwrap();
        assert_eq!(
            *timeout(Duration::from_secs(1), create_task)
                .await
                .unwrap()
                .unwrap(),
            8
        );
        assert_eq!(creators.load(Ordering::SeqCst), 1);
        assert_eq!(*map.get(&1).await.unwrap(), 8);
    }

    #[tokio::test]
    async fn insert_waits_for_create_and_preserves_existing_resource() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        let (create_started_tx, create_started_rx) = oneshot::channel();
        let (finish_create_tx, finish_create_rx) = oneshot::channel();

        let create_map = map.clone();
        let create_task = tokio::spawn(async move {
            create_map
                .get_or_create(1, async {
                    create_started_tx.send(()).unwrap();
                    finish_create_rx.await.unwrap();
                    Ok::<_, StringError>(Arc::new(1))
                })
                .await
                .unwrap()
        });

        create_started_rx.await.unwrap();

        let insert_map = map.clone();
        let insert_task =
            tokio::spawn(async move { insert_map.insert::<StringError>(1, Arc::new(2)).await });

        tokio::task::yield_now().await;
        finish_create_tx.send(()).unwrap();

        assert_eq!(*create_task.await.unwrap(), 1);
        assert!(timeout(Duration::from_secs(1), insert_task)
            .await
            .unwrap()
            .unwrap()
            .is_err());
        assert_eq!(*map.get(&1).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn with_resource_serializes_with_release() {
        let map = Arc::new(AsyncSharedMap::<u64, u64>::new());
        map.insert::<StringError>(1, Arc::new(1)).await.unwrap();

        let (with_started_tx, with_started_rx) = oneshot::channel();
        let (finish_with_tx, finish_with_rx) = oneshot::channel();
        let with_map = map.clone();
        let with_task = tokio::spawn(async move {
            with_map
                .with_resource(&1, |resource| async move {
                    assert_eq!(*resource, 1);
                    with_started_tx.send(()).unwrap();
                    finish_with_rx.await.unwrap();
                    Ok::<_, StringError>(())
                })
                .await
                .unwrap()
        });

        with_started_rx.await.unwrap();

        let cleanup_calls = Arc::new(AtomicUsize::new(0));
        let release_map = map.clone();
        let release_count = cleanup_calls.clone();
        let release_task = tokio::spawn(async move {
            let (released, cleanup_res) = release_map
                .release(1, async {
                    release_count.fetch_add(1, Ordering::SeqCst);
                    Ok::<_, StringError>(())
                })
                .await;
            cleanup_res.unwrap();
            released
        });

        tokio::task::yield_now().await;
        assert_eq!(cleanup_calls.load(Ordering::SeqCst), 0);

        finish_with_tx.send(()).unwrap();
        assert!(with_task.await.unwrap());
        assert!(release_task.await.unwrap());
        assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
    }
}
