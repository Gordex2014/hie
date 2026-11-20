//! Whether the FHIR server answers, as this engine process last saw it: from
//! the requests deliveries send it, and from a probe of `/metadata` while none
//! are sent. Kept in memory; `GET /api/monitoring/status` reports it.

use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use crate::db::now_ms;

/// The longest probe interval `destination.probe_seconds` accepts.
pub const MAX_PROBE_SECONDS: u64 = 86_400;
/// How long one probe may take.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a 502, 503 or 504 answered to a delivery outweighs a probe that
/// gets an answer: longer than the default longest delivery backoff (5 min),
/// so a source that keeps retrying is heard before the probe takes over.
const DELIVERY_AUTHORITY: Duration = Duration::from_secs(600);

/// Why a request did not reach the FHIR server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failure {
    /// No connection could be made.
    Connect,
    /// No answer in time.
    Timeout,
    /// The connection broke before an answer.
    Lost,
    /// The server, or a proxy in front of it, answered 502, 503 or 504.
    Status(u16),
}

impl Failure {
    /// A fixed sentence, safe to show anyone: never a URL, a host or an
    /// error text from a library.
    pub fn sentence(self) -> &'static str {
        match self {
            Failure::Connect => "could not connect to the FHIR server",
            Failure::Timeout => "the FHIR server did not answer in time",
            Failure::Lost => "the connection to the FHIR server was lost",
            Failure::Status(502) => "the FHIR server answered HTTP 502",
            Failure::Status(503) => "the FHIR server answered HTTP 503",
            Failure::Status(_) => "the FHIR server answered HTTP 504",
        }
    }
}

/// Who sent the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Via {
    /// A delivery's request under the base URL: a batch Bundle, a
    /// `$bulk-submit` kick-off or status request.
    Delivery,
    /// The idle probe of `/metadata`.
    Probe,
}

/// What the status reports as `destination`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// `None` until something was observed in this process.
    pub reachable: Option<bool>,
    /// When `reachable` took its current value.
    pub since_ms: Option<i64>,
    /// While unreachable, why, as `Failure::sentence` gives it.
    pub last_error: Option<&'static str>,
    /// Unreachable observations in a row, every source and the probe together.
    pub consecutive_failures: u32,
    /// The latest observation that counted.
    pub observed_ms: Option<i64>,
}

struct State {
    reachable: Option<bool>,
    since_ms: Option<i64>,
    failure: Option<Failure>,
    consecutive_failures: u32,
    observed_ms: Option<i64>,
    /// When the latest observation that counted started.
    latest_start: Option<Instant>,
    /// When a request last started or ended.
    last_activity: Option<Instant>,
    /// When a delivery's request last started.
    delivery_started: Option<Instant>,
    /// When a delivery last got 502 to 504, until a delivery gets an answer.
    delivery_outage: Option<Instant>,
    in_flight: usize,
    /// How long `delivery_outage` outweighs a probe's answer.
    authority: Duration,
}

/// The engine's view of its FHIR server. One per `FhirClient`, shared by every
/// source's deliveries and the probe.
pub struct Destination {
    origin: Option<url::Url>,
    state: Mutex<State>,
}

/// One request under way, from just before it is sent until its answer has
/// been read. `observe` records how it went; dropped, it only ends the request.
pub struct InFlight {
    destination: Arc<Destination>,
    started: Instant,
    via: Via,
}

impl Drop for InFlight {
    fn drop(&mut self) {
        let mut s = self.destination.lock();
        s.in_flight = s.in_flight.saturating_sub(1);
        s.last_activity = Some(Instant::now());
    }
}

impl InFlight {
    /// Records how the request went, now.
    pub fn observe(&self, outcome: Result<(), Failure>) {
        self.destination
            .record(self.started, self.via, outcome, Instant::now());
    }

    /// Records the result of sending the request. A request that could not be
    /// built, or that went round in redirects, says nothing about the server.
    pub fn observe_send(&self, result: &Result<reqwest::Response, reqwest::Error>) {
        if let Some(outcome) = classify(result) {
            if let Err(e) = result {
                tracing::debug!(
                    "a request to the FHIR server failed: {}",
                    crate::bulk::safe_reason(&e.to_string())
                );
            }
            self.observe(outcome);
        }
    }
}

/// What a request's result says about the server; `None` when it says nothing.
pub fn classify(result: &Result<reqwest::Response, reqwest::Error>) -> Option<Result<(), Failure>> {
    match result {
        Ok(response) => Some(match response.status().as_u16() {
            status @ 502..=504 => Err(Failure::Status(status)),
            _ => Ok(()),
        }),
        Err(e) if e.is_builder() || e.is_redirect() => None,
        // A connect timeout is both: it is a connection that could not be made.
        Err(e) if e.is_connect() => Some(Err(Failure::Connect)),
        Err(e) if e.is_timeout() => Some(Err(Failure::Timeout)),
        Err(_) => Some(Err(Failure::Lost)),
    }
}

impl Destination {
    /// The destination at `base`, `fhir_base_url` as configured.
    pub fn new(base: &str) -> Self {
        Self {
            origin: url::Url::parse(base).ok(),
            state: Mutex::new(State {
                reachable: None,
                since_ms: None,
                failure: None,
                consecutive_failures: 0,
                observed_ms: None,
                latest_start: None,
                last_activity: None,
                delivery_started: None,
                delivery_outage: None,
                in_flight: 0,
                authority: DELIVERY_AUTHORITY,
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A delivery's request starts now.
    pub fn start(self: &Arc<Self>) -> InFlight {
        self.begin(Via::Delivery, Instant::now())
    }

    fn begin(self: &Arc<Self>, via: Via, now: Instant) -> InFlight {
        let mut s = self.lock();
        self.begin_locked(&mut s, via, now)
    }

    fn begin_locked(self: &Arc<Self>, s: &mut State, via: Via, now: Instant) -> InFlight {
        s.in_flight += 1;
        s.last_activity = Some(now);
        if via == Via::Delivery {
            s.delivery_started = Some(now);
        }
        InFlight {
            destination: self.clone(),
            started: now,
            via,
        }
    }

    /// Starts a probe if the destination is idle: no request under way, and
    /// none started or ended within `every`. Checked and started under one
    /// lock, so a delivery either counts against the idleness or starts after
    /// the probe, and outweighs it.
    fn try_probe(self: &Arc<Self>, now: Instant, every: Duration) -> Option<InFlight> {
        let mut s = self.lock();
        let idle = s.in_flight == 0
            && s.last_activity
                .is_none_or(|at| now.saturating_duration_since(at) >= every);
        idle.then(|| self.begin_locked(&mut s, Via::Probe, now))
    }

    /// How long until the destination may be idle for a probe.
    fn probe_wait(&self, now: Instant, every: Duration) -> Duration {
        let s = self.lock();
        if s.in_flight > 0 {
            return every;
        }
        match s.last_activity {
            None => Duration::ZERO,
            Some(at) => every.saturating_sub(now.saturating_duration_since(at)),
        }
    }

    /// Records an outcome. Late news does not count: a request that started
    /// before the latest one recorded, or a probe that a delivery started
    /// after. Neither does a probe's answer while deliveries see the server
    /// answer 502 to 504, which `/metadata` may not show; once no delivery has
    /// had one for `authority`, the probe's answer counts again.
    fn record(&self, started: Instant, via: Via, outcome: Result<(), Failure>, now: Instant) {
        let mut s = self.lock();
        if s.latest_start.is_some_and(|latest| started < latest) {
            return;
        }
        if via == Via::Probe {
            if s.delivery_started.is_some_and(|d| d > started) {
                return;
            }
            if outcome.is_ok()
                && s.delivery_outage
                    .is_some_and(|at| now.saturating_duration_since(at) < s.authority)
            {
                return;
            }
        }
        if via == Via::Delivery {
            s.delivery_outage = match outcome {
                Err(Failure::Status(_)) => Some(now),
                Ok(()) => None,
                Err(_) => s.delivery_outage,
            };
        }
        let at = now_ms();
        match outcome {
            Ok(()) => {
                if s.reachable != Some(true) {
                    if s.reachable == Some(false) {
                        tracing::info!("the FHIR server answers again");
                    }
                    s.since_ms = Some(at);
                }
                s.reachable = Some(true);
                s.failure = None;
                s.consecutive_failures = 0;
            }
            Err(failure) => {
                if s.reachable != Some(false) {
                    tracing::warn!("the FHIR server cannot be reached: {}", failure.sentence());
                    s.since_ms = Some(at);
                }
                s.reachable = Some(false);
                s.failure = Some(failure);
                s.consecutive_failures = s.consecutive_failures.saturating_add(1);
            }
        }
        s.observed_ms = Some(at);
        s.latest_start = Some(started);
    }

    pub fn snapshot(&self) -> Snapshot {
        let s = self.lock();
        Snapshot {
            reachable: s.reachable,
            since_ms: s.since_ms,
            last_error: s.failure.map(Failure::sentence),
            consecutive_failures: s.consecutive_failures,
            observed_ms: s.observed_ms,
        }
    }

    /// `{base}/metadata` without the base's user information, query or
    /// fragment: the probe sends no credentials of any kind.
    fn probe_url(&self) -> Option<url::Url> {
        let mut url = self.origin.clone()?;
        let _ = url.set_username("");
        let _ = url.set_password(None);
        url.set_query(None);
        url.set_fragment(None);
        let path = format!("{}/metadata", url.path().trim_end_matches('/'));
        url.set_path(&path);
        Some(url)
    }
}

/// Starts the idle probe: `GET {base}/metadata` once nothing has reached the
/// server for `every`, anonymously, without following redirects. `None` when
/// `every` is zero (the probe is off) or the base is not a URL. The task ends
/// once the destination is dropped.
pub fn spawn_probe(
    destination: Weak<Destination>,
    every: Duration,
) -> Option<tokio::task::JoinHandle<()>> {
    if every.is_zero() {
        return None;
    }
    let url = destination.upgrade()?.probe_url()?;
    let http = reqwest::Client::builder()
        .timeout(PROBE_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .pool_max_idle_per_host(0)
        .build()
        .expect("HTTP client");
    Some(tokio::spawn(async move {
        loop {
            let probe = {
                let Some(d) = destination.upgrade() else {
                    return;
                };
                let now = Instant::now();
                match d.try_probe(now, every) {
                    Some(probe) => Ok(probe),
                    None => Err(d.probe_wait(now, every)),
                }
            };
            match probe {
                Ok(probe) => {
                    let result = http
                        .get(url.clone())
                        .header("Accept", "application/fhir+json")
                        .send()
                        .await;
                    probe.observe_send(&result);
                }
                // At least a millisecond, so a wait that rounds to nothing
                // does not spin.
                Err(wait) => tokio::time::sleep(wait.max(Duration::from_millis(1))).await,
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn destination() -> Arc<Destination> {
        Arc::new(Destination::new("http://hfs:8080/fhir"))
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[test]
    fn failures_have_fixed_sentences() {
        assert_eq!(
            Failure::Connect.sentence(),
            "could not connect to the FHIR server"
        );
        assert_eq!(
            Failure::Timeout.sentence(),
            "the FHIR server did not answer in time"
        );
        assert_eq!(
            Failure::Lost.sentence(),
            "the connection to the FHIR server was lost"
        );
        for status in [502, 503, 504] {
            assert_eq!(
                Failure::Status(status).sentence(),
                format!("the FHIR server answered HTTP {status}")
            );
        }
    }

    #[test]
    fn nothing_observed_is_unknown() {
        let d = destination();
        assert_eq!(
            d.snapshot(),
            Snapshot {
                reachable: None,
                since_ms: None,
                last_error: None,
                consecutive_failures: 0,
                observed_ms: None
            }
        );
    }

    #[test]
    fn failures_count_until_an_answer_and_since_marks_the_change() {
        let d = destination();
        let t0 = Instant::now();
        d.record(t0, Via::Delivery, Err(Failure::Connect), t0);
        let first = d.snapshot();
        assert_eq!(first.reachable, Some(false));
        assert_eq!(first.consecutive_failures, 1);
        std::thread::sleep(ms(3));
        d.record(t0 + ms(1), Via::Delivery, Err(Failure::Timeout), t0 + ms(2));
        let second = d.snapshot();
        assert_eq!(second.since_ms, first.since_ms, "same outage, same since");
        assert!(second.observed_ms > first.observed_ms);
        assert_eq!(second.consecutive_failures, 2);
        assert_eq!(
            second.last_error,
            Some("the FHIR server did not answer in time")
        );
        std::thread::sleep(ms(3));
        d.record(t0 + ms(3), Via::Delivery, Ok(()), t0 + ms(4));
        let back = d.snapshot();
        assert_eq!(back.reachable, Some(true));
        assert!(back.since_ms > second.since_ms);
        assert_eq!(back.consecutive_failures, 0);
        assert_eq!(back.last_error, None);
    }

    #[test]
    fn a_request_that_started_before_the_latest_one_recorded_does_not_count() {
        let d = destination();
        let t0 = Instant::now();
        // A slow request times out after a newer one got an answer.
        d.record(t0 + ms(10), Via::Delivery, Ok(()), t0 + ms(11));
        d.record(t0, Via::Delivery, Err(Failure::Timeout), t0 + ms(120));
        assert_eq!(d.snapshot().reachable, Some(true));
    }

    #[test]
    fn a_probe_gives_way_to_a_delivery_started_after_it_in_either_order() {
        let t0 = Instant::now();
        // The probe ends first.
        let d = destination();
        let probe = d.begin(Via::Probe, t0);
        let delivery = d.begin(Via::Delivery, t0 + ms(1));
        d.record(probe.started, Via::Probe, Ok(()), t0 + ms(2));
        assert_eq!(d.snapshot().reachable, None);
        d.record(
            delivery.started,
            Via::Delivery,
            Err(Failure::Status(503)),
            t0 + ms(3),
        );
        assert_eq!(d.snapshot().reachable, Some(false));
        // The delivery ends first.
        let d = destination();
        let probe = d.begin(Via::Probe, t0);
        let delivery = d.begin(Via::Delivery, t0 + ms(1));
        d.record(
            delivery.started,
            Via::Delivery,
            Err(Failure::Status(503)),
            t0 + ms(2),
        );
        d.record(probe.started, Via::Probe, Ok(()), t0 + ms(3));
        let s = d.snapshot();
        assert_eq!(s.reachable, Some(false));
        assert_eq!(s.consecutive_failures, 1);
    }

    #[test]
    fn a_delivery_outage_outweighs_the_probe_until_its_authority_runs_out() {
        let d = destination();
        d.lock().authority = ms(100);
        let t0 = Instant::now();
        d.record(t0, Via::Delivery, Err(Failure::Status(503)), t0);
        let since = d.snapshot().since_ms;
        // The probe answers, within the window: no change.
        d.record(t0 + ms(10), Via::Probe, Ok(()), t0 + ms(10));
        assert_eq!(d.snapshot().reachable, Some(false));
        // A probe that fails counts, and does not end the delivery's authority.
        d.record(t0 + ms(20), Via::Probe, Err(Failure::Connect), t0 + ms(20));
        d.record(t0 + ms(30), Via::Probe, Ok(()), t0 + ms(30));
        let s = d.snapshot();
        assert_eq!(s.reachable, Some(false));
        assert_eq!(s.since_ms, since);
        assert_eq!(s.consecutive_failures, 2);
        // No delivery for longer than the window: the probe's answer counts.
        d.record(t0 + ms(150), Via::Probe, Ok(()), t0 + ms(150));
        assert_eq!(d.snapshot().reachable, Some(true));
    }

    #[test]
    fn a_delivery_answer_ends_the_outage_at_once() {
        let d = destination();
        let t0 = Instant::now();
        d.record(t0, Via::Delivery, Err(Failure::Status(502)), t0);
        d.record(t0 + ms(1), Via::Delivery, Ok(()), t0 + ms(1));
        assert_eq!(d.snapshot().reachable, Some(true));
        // And the probe's answers count again.
        d.record(t0 + ms(2), Via::Probe, Err(Failure::Connect), t0 + ms(2));
        d.record(t0 + ms(3), Via::Probe, Ok(()), t0 + ms(3));
        assert_eq!(d.snapshot().reachable, Some(true));
    }

    #[test]
    fn the_probe_waits_for_idle() {
        let every = Duration::from_secs(60);
        let d = destination();
        let t0 = Instant::now();
        assert_eq!(d.probe_wait(t0, every), Duration::ZERO, "never observed");
        let delivery = d.begin(Via::Delivery, t0);
        assert!(d.try_probe(t0 + ms(1), every).is_none(), "one in flight");
        assert_eq!(d.probe_wait(t0 + ms(1), every), every);
        drop(delivery);
        let ended = d.lock().last_activity.unwrap();
        assert!(
            d.try_probe(ended + Duration::from_secs(10), every)
                .is_none()
        );
        assert_eq!(
            d.probe_wait(ended + Duration::from_secs(10), every),
            Duration::from_secs(50)
        );
        let probe = d.try_probe(ended + every, every);
        assert!(probe.is_some());
        assert_eq!(d.lock().in_flight, 1);
        drop(probe);
        assert_eq!(d.lock().in_flight, 0);
    }

    #[test]
    fn the_probe_url_carries_no_credentials() {
        let d = Destination::new("http://user:secret@hfs:8080/fhir/?token=x#f");
        assert_eq!(
            d.probe_url().unwrap().as_str(),
            "http://hfs:8080/fhir/metadata"
        );
        let d = Destination::new("http://hfs:8080");
        assert_eq!(d.probe_url().unwrap().as_str(), "http://hfs:8080/metadata");
        assert!(Destination::new("not a url").probe_url().is_none());
    }

    #[tokio::test]
    async fn a_probe_interval_of_zero_starts_nothing() {
        let d = destination();
        assert!(spawn_probe(Arc::downgrade(&d), Duration::ZERO).is_none());
    }
}
