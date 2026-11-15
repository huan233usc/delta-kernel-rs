//! Background L2 writes of path-based latest renewals (SPEC v12.1).
//!
//! A latest re-listing that finds no new commit renews the cached instance. Its L2 writes
//! (re-confirm the `snap` entry, advance the `latest` pointer) are best effort and must not hold up
//! the request, so they go through this queue to one writer task:
//!
//! - **Coalescing per key** `(table_key, tail_id)`: a pending renewal is replaced only by one that
//!   wins under [`latest_replaces`] (higher version, or the same version and tail confirmed later),
//!   so the queue never moves a pointer backwards; the store applies the same rule again.
//! - **Bounded**: at most `capacity` keys pending; a renewal of a new key beyond that is dropped
//!   (and counted). Dropping is safe: the next renewal or build writes the pointer.
//! - **Failures** are logged at WARN by the writer and never reach a request.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::Notify;

use crate::store::{LatestRecord, latest_replaces};

/// Pending renewal keys at most (`Core`'s queue).
pub const DEFAULT_RENEWAL_QUEUE_CAPACITY: usize = 4096;

/// One renewal's L2 writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Renewal {
    /// L2 `table_key`.
    pub table: String,
    /// The `latest` pointer to write (monotonic).
    pub rec: LatestRecord,
    /// Re-confirmation time of `rec.seg_id`'s `snap` entry.
    pub confirm_at_ms: i64,
    /// Request staleness bound (the Redis `latest` TTL).
    pub staleness: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Enqueued {
    /// A new pending key.
    Queued,
    /// Merged into the pending renewal of the same key.
    Coalesced,
    /// The queue is full; not written.
    Dropped,
}

pub struct RenewalQueue {
    pending: Mutex<HashMap<(String, String), Renewal>>,
    capacity: usize,
    /// Wakes the writer task.
    pub notify: Notify,
    /// Held while a batch is written, so a flush waits for the writer's in-flight batch.
    pub writing: tokio::sync::Mutex<()>,
}

impl RenewalQueue {
    pub fn new(capacity: usize) -> Self {
        Self {
            pending: Mutex::default(),
            capacity: capacity.max(1),
            notify: Notify::new(),
            writing: tokio::sync::Mutex::new(()),
        }
    }

    pub fn push(&self, renewal: Renewal) -> Enqueued {
        let outcome = {
            let mut pending = self.pending.lock().unwrap();
            let key = (renewal.table.clone(), renewal.rec.tail_id.clone());
            let full = pending.len() >= self.capacity;
            match pending.get_mut(&key) {
                Some(cur) => {
                    if latest_replaces(&cur.rec, &renewal.rec) {
                        *cur = renewal;
                    } else if cur.rec.seg_id == renewal.rec.seg_id {
                        cur.confirm_at_ms = cur.confirm_at_ms.max(renewal.confirm_at_ms);
                    }
                    Enqueued::Coalesced
                }
                None if full => Enqueued::Dropped,
                None => {
                    pending.insert(key, renewal);
                    Enqueued::Queued
                }
            }
        };
        if outcome != Enqueued::Dropped {
            self.notify.notify_one();
        }
        outcome
    }

    /// Takes every pending renewal.
    pub fn take(&self) -> Vec<Renewal> {
        self.pending
            .lock()
            .unwrap()
            .drain()
            .map(|(_, r)| r)
            .collect()
    }

    pub fn len(&self) -> usize {
        self.pending.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn renewal(table: &str, seg: &str, version: i64, at: i64) -> Renewal {
        Renewal {
            table: table.into(),
            rec: LatestRecord {
                seg_id: seg.into(),
                version,
                confirmed_at_ms: at,
                tail_id: String::new(),
            },
            confirm_at_ms: at,
            staleness: Duration::ZERO,
        }
    }

    #[test]
    fn coalesces_per_key_and_never_moves_backwards() {
        let q = RenewalQueue::new(16);
        assert_eq!(q.push(renewal("t", "s2", 2, 100)), Enqueued::Queued);
        // Later confirmation of the same version replaces.
        assert_eq!(q.push(renewal("t", "s2", 2, 150)), Enqueued::Coalesced);
        // An older version confirmed later does not.
        assert_eq!(q.push(renewal("t", "s1", 1, 999)), Enqueued::Coalesced);
        // An earlier confirmation of the same version does not either.
        assert_eq!(q.push(renewal("t", "s2", 2, 120)), Enqueued::Coalesced);
        assert_eq!(q.push(renewal("u", "x", 7, 1)), Enqueued::Queued);
        let mut got = q.take();
        got.sort_by(|a, b| a.table.cmp(&b.table));
        assert_eq!(
            got,
            vec![renewal("t", "s2", 2, 150), renewal("u", "x", 7, 1)]
        );
        assert!(q.is_empty());
        // A higher version wins whatever its confirmation time.
        q.push(renewal("t", "s2", 2, 500));
        q.push(renewal("t", "s3", 3, 10));
        assert_eq!(q.take(), vec![renewal("t", "s3", 3, 10)]);
    }

    #[test]
    fn bounded_new_keys_are_dropped_existing_still_coalesce() {
        let q = RenewalQueue::new(2);
        assert_eq!(q.push(renewal("a", "s", 1, 1)), Enqueued::Queued);
        assert_eq!(q.push(renewal("b", "s", 1, 1)), Enqueued::Queued);
        assert_eq!(q.push(renewal("c", "s", 1, 1)), Enqueued::Dropped);
        assert_eq!(q.push(renewal("a", "s", 1, 5)), Enqueued::Coalesced);
        assert_eq!(q.len(), 2);
        let tables: Vec<String> = {
            let mut t: Vec<String> = q.take().into_iter().map(|r| r.table).collect();
            t.sort();
            t
        };
        assert_eq!(tables, vec!["a", "b"]);
        assert_eq!(
            q.push(renewal("c", "s", 1, 1)),
            Enqueued::Queued,
            "room again"
        );
    }
}
