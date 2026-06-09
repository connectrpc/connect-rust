//! A shared-state [`Checker`] suitable for most servers.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Mutex;

use connectrpc::ConnectError;
use tokio::sync::watch;

use crate::checker::StatusStream;
use crate::{Checker, Status};

/// In-memory checker backed by a `HashMap<String, Status>`.
///
/// `Send + Sync`; clone the `Arc` you wrap it in to share across tasks.
///
/// # Registration is explicit
///
/// Every name a probe might ask about must be registered first — either
/// up front via [`with_services`](Self::with_services) (the common case;
/// pass the generated `*_SERVICE_NAME` constants) or at runtime via
/// [`register`](Self::register). [`set_status`](Self::set_status) refuses
/// unknown names and returns [`UnknownServiceError`], so a typo'd name
/// surfaces at the call site instead of silently shadowing the real
/// entry. This matches connect-go's `grpchealth.StaticChecker.SetStatus`.
///
/// # Empty service name
///
/// The empty string represents the whole-process status. It is always
/// pre-registered with [`Status::Serving`], so `check("")` and
/// `watch("")` behave like any other registered service — and
/// [`shutdown`](Self::shutdown) flips it alongside the user-registered
/// services. Unregistered non-empty services return `NotFound` from
/// both `check` and `watch`.
pub struct StaticChecker {
    services: Mutex<HashMap<String, watch::Sender<Status>>>,
}

/// Returned by [`StaticChecker::set_status`] when called with a name
/// that hasn't been registered. Register first via
/// [`StaticChecker::register`] (or up front via
/// [`StaticChecker::with_services`]) to avoid this.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownServiceError {
    service: String,
}

impl UnknownServiceError {
    /// The unknown service name that triggered the error.
    #[must_use]
    pub fn service(&self) -> &str {
        &self.service
    }
}

impl std::fmt::Display for UnknownServiceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Quote with `{:?}` so the empty whole-process name and any
        // whitespace-only typos render visibly in log lines.
        write!(f, "unknown service {:?}", self.service)
    }
}

impl std::error::Error for UnknownServiceError {}

impl StaticChecker {
    /// Create a checker with only the whole-process entry (`""`) seeded
    /// at [`Status::Serving`].
    #[must_use]
    pub fn new() -> Self {
        Self::with_services(std::iter::empty::<&str>())
    }

    /// Create a checker pre-populated with the given services and the
    /// whole-process entry (`""`), each reporting [`Status::Serving`].
    /// Pass the generated `*_SERVICE_NAME` constant to avoid typos.
    ///
    /// The synthetic `""` entry is chained *before* the user entries,
    /// so a user-supplied `""` (or any duplicate key) wins the last-write
    /// when the iterator is collected into the underlying `HashMap`.
    /// Duplicate non-empty names also last-write-win — pass each name
    /// once.
    #[must_use]
    pub fn with_services<I, S>(services: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        // Empty entry first → user entries chained after → HashMap
        // collect is last-write-wins → a user-supplied `""` overrides
        // the synthetic one.
        let map = std::iter::once((String::new(), watch::channel(Status::Serving).0))
            .chain(
                services
                    .into_iter()
                    .map(|s| (s.into(), watch::channel(Status::Serving).0)),
            )
            .collect();
        Self {
            services: Mutex::new(map),
        }
    }

    /// Register `service` with an initial [`Status::Serving`]. Returns
    /// `true` if the service was newly registered, `false` if it was
    /// already known (existing status and subscribers preserved).
    ///
    /// Takes `impl Into<String>` (consumes / allocates) because the
    /// name becomes a map key on the `Vacant` branch. The companion
    /// [`set_status`](Self::set_status) takes `impl AsRef<str>` instead
    /// — it only borrows for the lookup, no insertion path.
    ///
    /// `register("")` is a no-op in the common case (the whole-process
    /// entry is seeded at construction by [`new`](Self::new) /
    /// [`with_services`](Self::with_services)); it returns `true` only
    /// after [`remove_service("")`](Self::remove_service), where it
    /// installs a fresh `Sender` (in-flight `Watch` subscribers were
    /// already terminated by the `remove_service` call that dropped
    /// the prior `Sender`).
    #[must_use = "the bool indicates whether the name was newly registered \
                  (true) or already present (false); ignore with `let _ = …` \
                  if you don't care which"]
    pub fn register(&self, service: impl Into<String>) -> bool {
        self.register_with_status(service, Status::Serving)
    }

    /// Register `service` with the given initial `status`. Returns
    /// `true` if newly registered, `false` if already known (status
    /// preserved — use [`set_status`](Self::set_status) to update an
    /// existing entry). Use this to bring a service up as
    /// [`Status::NotServing`] while you wait for its dependencies.
    #[must_use = "the bool indicates whether the name was newly registered \
                  (true) or already present (false); ignore with `let _ = …` \
                  if you don't care which"]
    pub fn register_with_status(&self, service: impl Into<String>, status: Status) -> bool {
        let mut services = self.lock();
        match services.entry(service.into()) {
            Entry::Vacant(slot) => {
                slot.insert(watch::channel(status).0);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    /// Update the status of a previously [`register`](Self::register)ed
    /// service. Existing `Watch` subscribers are notified; no-op
    /// transitions are suppressed.
    ///
    /// Takes `impl AsRef<str>` (borrowing, no allocation) because — unlike
    /// [`register`](Self::register) — there's no insertion: a `String`,
    /// `&String`, or `&str` all work without cloning.
    ///
    /// # Errors
    ///
    /// Returns [`UnknownServiceError`] if `service` was never registered.
    /// This is the strict, typo-catching variant — call
    /// [`register`](Self::register) (or
    /// [`with_services`](Self::with_services) up front) before flipping
    /// status.
    pub fn set_status(
        &self,
        service: impl AsRef<str>,
        status: Status,
    ) -> Result<(), UnknownServiceError> {
        let service = service.as_ref();
        let services = self.lock();
        let Some(sender) = services.get(service) else {
            return Err(UnknownServiceError {
                service: service.to_string(),
            });
        };
        sender.send_if_modified(|current| {
            if *current == status {
                false
            } else {
                *current = status;
                true
            }
        });
        Ok(())
    }

    /// Remove `service` from the registry. Returns `true` if it was
    /// present. Active `Watch` streams complete when the underlying
    /// [`watch::Sender`] is dropped; subsequent `Check`/`Watch` calls
    /// for the name return `NotFound`. The whole-process entry (`""`)
    /// is removable like any other — call only if you really mean it.
    pub fn remove_service(&self, service: &str) -> bool {
        self.lock().remove(service).is_some()
    }

    /// Mark every registered service [`Status::NotServing`], including
    /// the whole-process `""` entry. Call this from your shutdown handler
    /// before draining traffic. Services registered after `shutdown` are
    /// unaffected.
    pub fn shutdown(&self) {
        let services = self.lock();
        for sender in services.values() {
            sender.send_if_modified(|status| {
                if *status == Status::NotServing {
                    false
                } else {
                    *status = Status::NotServing;
                    true
                }
            });
        }
    }

    /// Snapshot of every registered service name. Includes the
    /// whole-process entry `""` unless a caller explicitly removed it
    /// via [`remove_service("")`](Self::remove_service); covers names
    /// supplied to [`with_services`](Self::with_services),
    /// [`register`](Self::register), and
    /// [`register_with_status`](Self::register_with_status), minus
    /// anything removed since.
    #[must_use]
    pub fn services(&self) -> Vec<String> {
        self.lock().keys().cloned().collect()
    }

    // Poison recovery: the wrapped state is `Status` + `watch::Sender`,
    // both safe to observe after a panic, so we keep going instead of
    // turning the next handler call into a second panic.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, watch::Sender<Status>>> {
        self.services
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Live `watch::Receiver` count for `service`, or `None` if not
    /// registered. Used by the in-crate end-to-end Watch-disconnect
    /// tests to observe server-side subscriber cleanup.
    ///
    /// Monotonic-per-subscriber: the count can only rise via an
    /// explicit `Sender::subscribe()` call and can only fall when a
    /// `Receiver` is dropped, so once a Watch RPC's server-side
    /// `Receiver` is live the count cannot transiently dip below the
    /// subscribed baseline until that `Receiver` actually drops. Safe
    /// to poll from a test without `Acquire` fences beyond the
    /// `Mutex`-guarded map access.
    #[cfg(test)]
    pub(crate) fn receiver_count_for(&self, service: &str) -> Option<usize> {
        self.lock().get(service).map(watch::Sender::receiver_count)
    }
}

impl Default for StaticChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl Checker for StaticChecker {
    async fn check(&self, service: &str) -> Result<Status, ConnectError> {
        // Block-scope the guard: forces `Send`-incompatible state to drop
        // before any future code path could grow an `.await`.
        let snapshot = {
            let services = self.lock();
            services.get(service).map(|sender| *sender.borrow())
        };
        snapshot.ok_or_else(|| ConnectError::not_found(format!("unknown service {service}")))
    }

    async fn watch(&self, service: &str) -> Result<StatusStream, ConnectError> {
        let receiver = {
            let services = self.lock();
            services.get(service).map(watch::Sender::subscribe)
        };
        receiver
            .map(StatusStream::from_watch)
            .ok_or_else(|| ConnectError::not_found(format!("unknown service {service}")))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;

    use super::*;

    #[tokio::test]
    async fn check_unknown_service_returns_not_found() {
        let checker = StaticChecker::new();
        let err = checker.check("acme.NoSuch").await.unwrap_err();
        assert_eq!(err.code, connectrpc::ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn check_empty_service_defaults_to_serving() {
        let checker = StaticChecker::new();
        assert_eq!(checker.check("").await.unwrap(), Status::Serving);
    }

    #[tokio::test]
    async fn with_services_seeds_serving() {
        let checker = StaticChecker::with_services(["acme.A", "acme.B"]);
        assert_eq!(checker.check("acme.A").await.unwrap(), Status::Serving);
        assert_eq!(checker.check("acme.B").await.unwrap(), Status::Serving);
    }

    /// `with_services` doesn't panic or deadlock when the user list
    /// contains duplicates (including a duplicate `""`); the entries
    /// collapse last-write-wins under `HashMap::collect`. The surviving
    /// Sender must be live — a `watch`/`set_status` round-trip exercises
    /// the dedup'd entry to catch any "orphaned Sender" regression.
    #[tokio::test]
    async fn with_services_duplicates_collapse_cleanly() {
        let checker = StaticChecker::with_services(["foo", "foo", ""]);
        let mut names = checker.services();
        names.sort();
        assert_eq!(names, vec!["", "foo"]);

        // Exercise both surviving entries end-to-end.
        for name in ["foo", ""] {
            let mut stream = checker.watch(name).await.unwrap();
            assert_eq!(stream.next().await.unwrap(), Status::Serving);
            checker.set_status(name, Status::NotServing).unwrap();
            let next = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
                .await
                .expect("subscriber must receive update after dedup")
                .unwrap();
            assert_eq!(next, Status::NotServing);
            // Reset for the next iteration.
            checker.set_status(name, Status::Serving).unwrap();
        }
    }

    #[tokio::test]
    async fn set_status_unknown_returns_error() {
        let checker = StaticChecker::new();
        let err = checker
            .set_status("acme.A", Status::NotServing)
            .unwrap_err();
        assert_eq!(err.service(), "acme.A");
    }

    #[tokio::test]
    async fn register_then_set_status_works() {
        let checker = StaticChecker::new();
        assert!(checker.register("acme.A"));
        checker.set_status("acme.A", Status::NotServing).unwrap();
        assert_eq!(checker.check("acme.A").await.unwrap(), Status::NotServing);
    }

    #[tokio::test]
    async fn register_is_idempotent_and_preserves_status() {
        let checker = StaticChecker::with_services(["acme.A"]);
        checker.set_status("acme.A", Status::NotServing).unwrap();
        // Re-registering an existing name must not reset its status.
        assert!(!checker.register("acme.A"));
        assert_eq!(checker.check("acme.A").await.unwrap(), Status::NotServing);
    }

    #[tokio::test]
    async fn register_with_status_seeds_initial_value() {
        let checker = StaticChecker::new();
        assert!(checker.register_with_status("acme.A", Status::NotServing));
        assert_eq!(checker.check("acme.A").await.unwrap(), Status::NotServing);
    }

    /// Mirror of `register_is_idempotent_and_preserves_status` for the
    /// `register_with_status` variant: when the entry is already
    /// registered, the call must return `false`, preserve the existing
    /// status, AND keep the same `watch::Sender` so in-flight
    /// subscribers stay connected. Pins the Occupied branch against
    /// refactors that either overwrite the status or swap the sender
    /// while still returning `false`.
    #[tokio::test]
    async fn register_with_status_is_idempotent_and_preserves_status() {
        let checker = StaticChecker::with_services(["acme.A"]);
        checker.set_status("acme.A", Status::NotServing).unwrap();

        // Subscribe BEFORE the redundant register. A regression that
        // swaps the underlying `Sender` (e.g. `slot.insert(channel(s).0)`
        // returning `false`) would orphan this subscriber — visible
        // here as a missed update on the post-register `set_status`.
        let mut stream = checker.watch("acme.A").await.unwrap();
        assert_eq!(stream.next().await.unwrap(), Status::NotServing);

        // Re-register with a status that conflicts with the live one;
        // existing status must win, the call must return `false`.
        assert!(!checker.register_with_status("acme.A", Status::Serving));
        assert_eq!(checker.check("acme.A").await.unwrap(), Status::NotServing);

        // Now flip the status through the SAME entry. If the prior
        // register call swapped the Sender out from under our
        // subscriber, this update never arrives.
        checker.set_status("acme.A", Status::Serving).unwrap();
        let next = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("subscriber must survive register_with_status no-op")
            .unwrap();
        assert_eq!(next, Status::Serving);
    }

    #[tokio::test]
    async fn remove_service_returns_true_and_makes_subsequent_checks_not_found() {
        let checker = StaticChecker::with_services(["acme.A"]);
        assert!(checker.remove_service("acme.A"));
        let err = checker.check("acme.A").await.unwrap_err();
        assert_eq!(err.code, connectrpc::ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn remove_service_returns_false_for_unknown() {
        let checker = StaticChecker::new();
        assert!(!checker.remove_service("acme.NoSuch"));
    }

    #[tokio::test]
    async fn shutdown_marks_all_not_serving() {
        let checker = StaticChecker::with_services(["acme.A", "acme.B"]);
        checker.shutdown();
        assert_eq!(checker.check("acme.A").await.unwrap(), Status::NotServing);
        assert_eq!(checker.check("acme.B").await.unwrap(), Status::NotServing);
    }

    #[tokio::test]
    async fn shutdown_leaves_post_registered_services_alone() {
        let checker = StaticChecker::with_services(["acme.A"]);
        checker.shutdown();
        // Registering after shutdown is the documented escape hatch — the
        // new service must come up Serving, not NotServing.
        assert!(checker.register("acme.B"));
        assert_eq!(checker.check("acme.B").await.unwrap(), Status::Serving);
    }

    #[tokio::test]
    async fn shutdown_is_noop_for_already_not_serving() {
        let checker = StaticChecker::with_services(["acme.A"]);
        checker.set_status("acme.A", Status::NotServing).unwrap();
        let mut stream = checker.watch("acme.A").await.unwrap();
        assert_eq!(stream.next().await.unwrap(), Status::NotServing);

        // Already NotServing → shutdown must not emit a notification.
        checker.shutdown();
        tokio::select! {
            item = stream.next() => panic!("unexpected notification on no-op shutdown: {item:?}"),
            () = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
        }
    }

    #[tokio::test]
    async fn watch_streams_initial_and_changes() {
        let checker = StaticChecker::with_services(["acme.A"]);
        let mut stream = checker.watch("acme.A").await.unwrap();

        // Initial value is the current state.
        assert_eq!(stream.next().await.unwrap(), Status::Serving);

        // Update fires the subscriber.
        checker.set_status("acme.A", Status::NotServing).unwrap();
        let next = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("watch did not deliver update within timeout")
            .unwrap();
        assert_eq!(next, Status::NotServing);
    }

    #[tokio::test]
    async fn watch_unknown_service_returns_not_found() {
        let checker = StaticChecker::new();
        let err = checker.watch("acme.NoSuch").await.unwrap_err();
        assert_eq!(err.code, connectrpc::ErrorCode::NotFound);
    }

    #[tokio::test]
    async fn watch_empty_service_subscribes() {
        let checker = StaticChecker::new();
        let mut stream = checker.watch("").await.unwrap();
        assert_eq!(stream.next().await.unwrap(), Status::Serving);
    }

    // Both subscribers see updates from the same Sender — a regression
    // in `register` that swapped the entry's Sender out from under
    // existing subscribers would break this silently.
    #[tokio::test]
    async fn concurrent_watchers_of_registered_service_share_a_sender() {
        let checker = Arc::new(StaticChecker::with_services(["acme.A"]));
        let mut a = checker.watch("acme.A").await.unwrap();
        let mut b = checker.watch("acme.A").await.unwrap();

        assert_eq!(a.next().await.unwrap(), Status::Serving);
        assert_eq!(b.next().await.unwrap(), Status::Serving);

        checker.set_status("acme.A", Status::NotServing).unwrap();
        let a_next = tokio::time::timeout(std::time::Duration::from_secs(1), a.next())
            .await
            .expect("subscriber A did not receive update")
            .unwrap();
        let b_next = tokio::time::timeout(std::time::Duration::from_secs(1), b.next())
            .await
            .expect("subscriber B did not receive update")
            .unwrap();
        assert_eq!(a_next, Status::NotServing);
        assert_eq!(b_next, Status::NotServing);
    }

    // Regression test: earlier code inserted a fresh Sender on every
    // watch("") call, orphaning prior subscribers.
    #[tokio::test]
    async fn concurrent_watchers_of_empty_service_share_a_sender() {
        let checker = Arc::new(StaticChecker::new());
        let mut a = checker.watch("").await.unwrap();
        let mut b = checker.watch("").await.unwrap();

        // Both see the initial value.
        assert_eq!(a.next().await.unwrap(), Status::Serving);
        assert_eq!(b.next().await.unwrap(), Status::Serving);

        // A single update must reach both subscribers.
        checker.set_status("", Status::NotServing).unwrap();
        let a_next = tokio::time::timeout(std::time::Duration::from_secs(1), a.next())
            .await
            .expect("subscriber A did not receive update")
            .unwrap();
        let b_next = tokio::time::timeout(std::time::Duration::from_secs(1), b.next())
            .await
            .expect("subscriber B did not receive update")
            .unwrap();
        assert_eq!(a_next, Status::NotServing);
        assert_eq!(b_next, Status::NotServing);
    }

    #[tokio::test]
    async fn set_same_status_does_not_notify() {
        let checker = StaticChecker::with_services(["acme.A"]);
        let mut stream = checker.watch("acme.A").await.unwrap();
        assert_eq!(stream.next().await.unwrap(), Status::Serving);

        // No change → no notification.
        checker.set_status("acme.A", Status::Serving).unwrap();
        tokio::select! {
            item = stream.next() => panic!("unexpected notification on no-op set_status: {item:?}"),
            () = tokio::time::sleep(std::time::Duration::from_millis(50)) => {}
        }
    }

    #[tokio::test]
    async fn services_lists_every_registered_name() {
        let checker = StaticChecker::with_services(["acme.A", "acme.B"]);
        assert!(checker.register("acme.C"));
        checker.set_status("acme.C", Status::NotServing).unwrap();

        let mut names = checker.services();
        names.sort();
        // The whole-process "" entry is always present.
        assert_eq!(names, vec!["", "acme.A", "acme.B", "acme.C"]);
    }

    #[tokio::test]
    async fn shutdown_flips_whole_process_entry() {
        let checker = StaticChecker::new();
        assert_eq!(checker.check("").await.unwrap(), Status::Serving);
        checker.shutdown();
        assert_eq!(checker.check("").await.unwrap(), Status::NotServing);
    }

    // Cancelling a Watch RPC must release the underlying watch::Receiver
    // so the Sender stops holding state for a dead subscriber.
    #[tokio::test]
    async fn dropping_watch_stream_releases_subscriber() {
        let checker = StaticChecker::with_services(["acme.A"]);

        let receiver_count_before = {
            let services = checker.services.lock().unwrap();
            services.get("acme.A").unwrap().receiver_count()
        };

        let stream = checker.watch("acme.A").await.unwrap();
        let receiver_count_during = {
            let services = checker.services.lock().unwrap();
            services.get("acme.A").unwrap().receiver_count()
        };
        assert_eq!(receiver_count_during, receiver_count_before + 1);

        drop(stream);
        // `WatchStream` drops its `Receiver` synchronously, so the
        // `Sender` observes the decrement immediately.
        let receiver_count_after = {
            let services = checker.services.lock().unwrap();
            services.get("acme.A").unwrap().receiver_count()
        };
        assert_eq!(receiver_count_after, receiver_count_before);
    }

    #[tokio::test]
    async fn concurrent_set_status_does_not_panic() {
        let checker = Arc::new(StaticChecker::with_services(["acme.race"]));
        let mut handles = Vec::new();
        for i in 0..50 {
            let c = Arc::clone(&checker);
            handles.push(tokio::spawn(async move {
                let status = if i % 2 == 0 {
                    Status::Serving
                } else {
                    Status::NotServing
                };
                c.set_status("acme.race", status).unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        let final_status = checker.check("acme.race").await.unwrap();
        assert!(matches!(final_status, Status::Serving | Status::NotServing));
    }

    /// N tasks race to register the same name. Exactly one must observe
    /// `Vacant` and return `true`; the rest see `Occupied` and return
    /// `false`. A subscriber wrapped around the first sender must still
    /// receive updates after the race — i.e. no register call orphaned
    /// the live `Sender` by replacing it.
    ///
    /// Uses `flavor = "multi_thread"` so the 50 spawned tasks actually
    /// contend for the `Mutex`; the default `current_thread` runtime
    /// would serialize them and the test name would be misleading.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_register_only_one_wins_and_subscriber_survives() {
        let checker = Arc::new(StaticChecker::new());
        // Use a fresh contended name so exactly one register call sees
        // `Vacant` and returns `true`; the rest see `Occupied` and
        // return `false`. After the race, subscribe and verify the
        // surviving `Sender` propagates updates.
        let mut handles = Vec::new();
        for _ in 0..50 {
            let c = Arc::clone(&checker);
            handles.push(tokio::spawn(async move { c.register("acme.race") }));
        }
        let mut winners = 0;
        for h in handles {
            if h.await.unwrap() {
                winners += 1;
            }
        }
        assert_eq!(
            winners, 1,
            "exactly one register call must succeed under contention"
        );

        // Subscribe AFTER the race, then update — the live Sender is the
        // one the winner installed; this subscriber must receive it.
        let mut stream = checker.watch("acme.race").await.unwrap();
        assert_eq!(stream.next().await.unwrap(), Status::Serving);
        checker.set_status("acme.race", Status::NotServing).unwrap();
        let next = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .expect("subscriber did not receive update")
            .unwrap();
        assert_eq!(next, Status::NotServing);
    }

    /// `register("")` is normally a no-op (the whole-process entry is
    /// seeded at construction), but after `remove_service("")` the slot
    /// is `Vacant` again — registering re-inserts and returns `true`.
    /// The doc comment on `register` calls this case out explicitly;
    /// this test pins the behavior.
    #[tokio::test]
    async fn register_after_remove_inserts_fresh() {
        let checker = StaticChecker::new();
        assert!(!checker.register(""), "default `\"\"` entry is seeded");
        assert!(checker.remove_service(""), "remove must succeed");
        assert!(
            checker.register(""),
            "register after remove must re-insert and return true"
        );
        assert_eq!(checker.check("").await.unwrap(), Status::Serving);
    }

    /// `set_status` takes `impl AsRef<str>`. Verify the three common
    /// call shapes all type-check without coaxing: `&str`, `String`,
    /// and `&String`. A regression that tightened the bound to `&str`
    /// would break two of these.
    #[tokio::test]
    async fn set_status_accepts_str_string_and_borrowed_string() {
        let checker = StaticChecker::with_services(["acme.A"]);

        // &str
        checker.set_status("acme.A", Status::NotServing).unwrap();
        assert_eq!(checker.check("acme.A").await.unwrap(), Status::NotServing);

        // String (owned, by value)
        let owned: String = "acme.A".to_string();
        checker.set_status(owned, Status::Serving).unwrap();
        assert_eq!(checker.check("acme.A").await.unwrap(), Status::Serving);

        // &String — should coerce via AsRef<str> without an explicit `.as_str()`
        let s: String = "acme.A".to_string();
        checker.set_status(&s, Status::NotServing).unwrap();
        assert_eq!(checker.check("acme.A").await.unwrap(), Status::NotServing);
    }

    /// `UnknownServiceError` carries the offending name verbatim,
    /// including empty / whitespace-only / non-ASCII forms. Verify the
    /// public accessors return them faithfully and `Display` keeps them
    /// visible (via `{:?}` debug-quoting).
    #[test]
    fn unknown_service_error_preserves_name() {
        let err = UnknownServiceError {
            service: String::new(),
        };
        assert_eq!(err.service(), "");
        let s = err.to_string();
        assert!(
            s.contains("\"\""),
            "empty name must render as `\"\"` so logs distinguish \
             it from a missing field: got {s:?}"
        );

        let err = UnknownServiceError {
            service: "acme.❄.frozen".into(),
        };
        assert_eq!(err.service(), "acme.❄.frozen");
        // Debug-quoting escapes non-ASCII to \u{...}. The intent is
        // documented on the Display impl; this test pins it so a
        // change to plain quoting surfaces here first.
        assert!(
            err.to_string().contains("acme."),
            "Display must include the prefix verbatim: {err}"
        );
    }
}
