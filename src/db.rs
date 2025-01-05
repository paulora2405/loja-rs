use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, RwLock},
    time::Duration,
};

use bytes::Bytes;
use tokio::{sync::Notify, time::Instant};
use tracing::debug;

#[derive(Debug)]
/// A single database entry.
struct Entry {
    /// Stored data
    data: Bytes,
    /// Instant at which the data expires and should be removed from the database
    expires_at: Option<Instant>,
}

#[derive(Debug)]
/// The internal state of the database.
struct DbState {
    /// The actual Key/Value data.
    entries: HashMap<String, Entry>,
    /// Keys TTLs tracking.
    ///
    /// A `BTreeSet` is used to maintain expirations sorted by when they will expire.
    /// This allows the background task to iterate this set to find the next expiring value.
    expirations: BTreeSet<(Instant, String)>,
    /// When the Db instance is shutting down, this is `true`.
    ///
    /// This happens when all `Db` values drop.
    /// Also, setting this to `true` signals the background task to exit.
    shutdown: bool,
}

#[derive(Debug)]
/// Shared state for the database.
struct DbSharedState {
    /// The actual database state is guarded by a `std::sync::rwlock::RwLock`.
    ///
    /// The is no need for `tokio::sync::RwLock` here, as there are no async operations
    /// performed while the write lock is held.
    /// Additionally, the critical sections are very small.
    state: RwLock<DbState>,
    /// Notifies the background task handling expiration events.
    ///
    /// The background task waits on this to be notified,
    /// then checks for expired values or the shutdown signal.
    background_task: Notify,
}

#[derive(Debug, Clone)]
/// Server state shared across all connections.
///
/// `Db` contains a `HashMap` storing the key/value data and all
/// `broadcast::Sender` values for active pub/sub channels.
///
/// A `Db` instance is a handle to shared state. Cloning `Db` is shallow and
/// only incurs an atomic ref count increment.
///
/// When a `Db` value is created, a background task is spawned. This task is
/// used to expire values after the requested duration has elapsed. The task
/// runs until all instances of `Db` are dropped, at which point the task
/// terminates.
pub(crate) struct Db {
    /// Handle to the shared state.
    ///
    /// The background task will also have an `Arc<DbSharedState>`.
    shared: Arc<DbSharedState>,
}

#[derive(Debug)]
/// A wrapper around `Db` instance.
///
/// This exists to allow orderly cleanup of the `Db` by signalling the background purge task
/// to shutdown when this struct is dropped.
pub(crate) struct DbDropGuard {
    /// The `Db` instance that will be shutdown when this `DbDropGuard` is dropped.
    db: Db,
}

impl DbDropGuard {
    /// Create a new `DbDropGuard`, wrapping a new `Db` instance.
    ///
    /// When this is dropped, the `Db`'s purge task will be shutdown.
    pub(crate) fn new() -> Self {
        DbDropGuard { db: Db::new() }
    }

    /// Get the shared database.
    ///
    /// Internally this is an `Arc`, so a clone only increments the ref count.
    pub(crate) fn db(&self) -> Db {
        self.db.clone()
    }
}

impl Drop for DbDropGuard {
    /// This `drop` signals the `Db` instance to shutdown the task that purges expired values.
    fn drop(&mut self) {
        self.db.shutdown_purge_task();
    }
}

impl Db {
    /// Create a new empty `Db` instance.
    ///
    /// Allocates the shared state and spawns a background task
    /// to manage key expiration.
    pub(crate) fn new() -> Self {
        let shared = Arc::new(DbSharedState {
            state: RwLock::new(DbState {
                entries: HashMap::new(),
                expirations: BTreeSet::new(),
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // Start the background task.
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Self { shared }
    }

    /// Get the value associated with a key.
    ///
    /// Returns `None` if there is no value associated with the key.
    /// This may be because no value was assigned to this key,
    /// or because a previously assigned value has expired.
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        // Acquire a read lock, get the entry and clone the value.
        // Because we use `Bytes` to store the data,
        // cloning is a shallow clone, the data itself is not copied.
        let state = self.shared.state.read().unwrap();
        state.entries.get(key).map(|e| e.data.clone())
    }

    /// Set the value associated with a key along with an optional TTL.
    ///
    /// if a value is already associated with the key, it will be replaced.
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.write().unwrap();

        // If this `set` becomes the key that expires **next**, the background
        // task needs to be notified so it can update its state.
        //
        // Whether or not the task needs to be notified is computed during the
        // `set` routine.
        let mut notify = false;

        let expires_at = expire.map(|duration| {
            // `Instant` at which the key expires
            let when = Instant::now() + duration;
            // Only notify the worker task if the newly inserted expiration is
            // the **next** key to evict. In this case, the worker needs to be
            // woken up to update its state.
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);
            when
        });

        // Insert the value into the database, and get the previous value if it existed.
        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expires_at,
            },
        );

        // If there was a value previously associated with the key,
        // **and** it had an expiration date, the associated entry in the `expirations`
        // set must be removed to avoid leaking data.
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                state.expirations.remove(&(when, key.clone()));
            }
        }

        // Track the expiration. If we insert before the remove that will cause
        // on the remote case when the current `(when, key)` is equal to the previous.
        if let Some(when) = expires_at {
            state.expirations.insert((when, key));
        }

        // Release the lock before notifying the background task.
        // This helps reduce contention by avoiding the background task waking up
        // only to be unable to acquire the lock due to this function still holding it,
        // and thus blocking.
        drop(state);

        // Finally, only notify the background task if it needs to update
        // its state to reflect a new expiration.
        if notify {
            self.shared.background_task.notify_one();
        }
    }

    /// Signals the purge background task to shutdown.
    ///
    /// This is called by the `DbDropGuard`'s `Drop` implementation.
    fn shutdown_purge_task(&self) {
        // The background task must be signaled to shutdown. This is done by
        // setting `DbState::shutdown` to `true` and signalling the task.
        let mut state = self.shared.state.write().unwrap();
        state.shutdown = true;
        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl DbSharedState {
    /// Returns `true` if the database is shutting down
    ///
    /// The `shutdown` flag is set when all `Db` values have dropped, indicating
    /// that the shared state can no longer be accessed.
    fn is_shutdown(&self) -> bool {
        self.state.read().unwrap().shutdown
    }

    /// Purge all expired keys and return the `Instant` at which the **next** key will expire.
    ///
    /// The background task will sleep until this instant.
    #[tracing::instrument(skip_all)]
    fn purge_expired_keys(&self) -> Option<Instant> {
        debug!("starting purge of expired keys");
        let mut state = self.state.write().unwrap();

        if state.shutdown {
            // The database is shutting down. All handles to the shared state
            // have been dropped. The background task should exit.
            return None;
        }

        // This is needed to make the borrow checker happy. In short, `write()`
        // returns a `RwLockWriteGuard` and not a `&mut DbState`. The borrow checker is
        // not able to see "through" the lock guard and determine that it is
        // safe to access both `state.expirations` and `state.entries` mutably,
        // so we get a "real" mutable reference to `DbState` outside of the loop.
        let state = &mut *state;

        // Find all keys scheduled to expire **before** now.
        let now = Instant::now();

        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                debug!("next expiration is in the future, done purging");
                // Done purging, `when` is the instant at which the next key expires.
                // The works task will wait until this instant.
                return Some(when);
            }

            // The key has expired, remove it.
            debug!("removing expired {key:?}");
            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }

        debug!("no keys to purge");
        None
    }
}

impl DbState {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

/// Routine executed by the background task.
///
/// Wait to be notified. On notification, purge any expired keys from the shared
/// state handle. If `shutdown` is set, terminate the task.
#[tracing::instrument(skip_all)]
async fn purge_expired_tasks(shared: Arc<DbSharedState>) {
    // If the shutdown flag is set, then the task should exit.
    while !shared.is_shutdown() {
        // Purge all keys that are expired. The function returns the instant at
        // which the **next** key will expire. The worker should wait until the
        // instant has passed then purge again.
        if let Some(when) = shared.purge_expired_keys() {
            // Wait until the next key expires **or** until the background task
            // is notified. If the task is notified, then it must reload its
            // state as new keys have been set to expire early. This is done by
            // looping.
            debug!("there are future expirations, sleeping or waiting for notification, whichever comes first");
            tokio::select! {
                _ = tokio::time::sleep_until(when) => {
                    debug!("background task woke up from sleep");
                }
                _ = shared.background_task.notified() => {
                    debug!("background task notified");
                }
            }
        } else {
            // There are no keys expiring in the future.
            // Wait until the task is notified.
            debug!("no future expirations, waiting for notification");
            shared.background_task.notified().await;
            debug!("background task notified");
        }
    }

    debug!("purge background task shutdown");
}
