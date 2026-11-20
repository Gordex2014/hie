//! Calendar days and times of day in an IANA time zone, UTC by default.
//!
//! A local time that does not exist (skipped by a change of offset, such as
//! daylight saving time) stands for the first valid instant after it, which is
//! the change itself; one that happens twice stands for its first occurrence. A
//! day is `[local midnight, next local midnight)` by that rule, so it lasts as
//! long as it did on the clock (23 or 25 hours across most DST changes), and a
//! date a zone skipped entirely is an empty day.
//!
//! Zone rules come from the copy of the IANA database built into the engine
//! (`jiff-tzdb`), never from the host: every instance counts the same days.

use std::sync::LazyLock;

use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, Timelike, Utc};
use jiff::tz::{AmbiguousOffset, TimeZone, TimeZoneDatabase};

const DAY_MS: i64 = 86_400_000;

static TZDB: LazyLock<TimeZoneDatabase> = LazyLock::new(TimeZoneDatabase::bundled);

/// Why a zone name was refused. A fixed sentence: it never repeats what was sent.
pub const INVALID_ZONE: &str = "tz must be an IANA time zone name, such as Europe/Madrid or UTC";

/// An IANA time zone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Zone {
    name: String,
    tz: TimeZone,
}

impl Default for Zone {
    fn default() -> Self {
        Zone::utc()
    }
}

impl Zone {
    pub fn utc() -> Zone {
        Zone {
            name: "UTC".into(),
            tz: TimeZone::UTC,
        }
    }

    /// An IANA name (`Europe/Madrid`, `America/New_York`, `UTC`), in any ASCII
    /// case; refused with [`INVALID_ZONE`].
    pub fn parse(name: &str) -> Result<Zone, &'static str> {
        if name.is_empty() || name.len() > 128 || name.trim() != name {
            return Err(INVALID_ZONE);
        }
        let tz = TZDB.get(name).map_err(|_| INVALID_ZONE)?;
        // `Etc/Unknown` has no name and `Factory` means "local time unknown".
        let name = tz
            .iana_name()
            .filter(|n| !n.eq_ignore_ascii_case("Factory"))
            .ok_or(INVALID_ZONE)?
            .to_string();
        Ok(Zone { name, tz })
    }

    /// The zone's name as the database spells it (`europe/madrid` is `Europe/Madrid`).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The instant a local date and time stand for: a time skipped by a DST
    /// change is the change itself, a repeated one its first occurrence.
    pub fn instant(&self, date: NaiveDate, time: NaiveTime) -> DateTime<Utc> {
        let civil = jiff::civil::DateTime::new(
            date.year() as i16,
            date.month() as i8,
            date.day() as i8,
            time.hour() as i8,
            time.minute() as i8,
            time.second() as i8,
            0,
        )
        .expect("a civil date and time jiff can hold");
        let ambiguous = self.tz.to_ambiguous_timestamp(civil);
        let at = match ambiguous.offset() {
            AmbiguousOffset::Unambiguous { .. } | AmbiguousOffset::Fold { .. } => {
                ambiguous.earlier().expect("an instant in range")
            }
            AmbiguousOffset::Gap { .. } => {
                // Read with the offset before the gap, the time lands at or after
                // the change; the change is the last transition up to there.
                let past = ambiguous.later().expect("an instant in range");
                let just_after = past
                    .checked_add(jiff::SignedDuration::from_nanos(1))
                    .expect("an instant in range");
                self.tz
                    .preceding(just_after)
                    .next()
                    .map(|t| t.timestamp())
                    .unwrap_or(past)
            }
        };
        DateTime::from_timestamp_millis(at.as_millisecond()).expect("an instant in range")
    }

    /// When a local day starts.
    pub fn day_start(&self, date: NaiveDate) -> DateTime<Utc> {
        self.instant(date, NaiveTime::MIN)
    }

    /// The local day `now` falls in, by the day starts above. The wall-clock
    /// date is only a first guess: a change that sets clocks back across
    /// midnight shows the previous date for a while after the day began.
    pub fn today(&self, now: DateTime<Utc>) -> NaiveDate {
        let at =
            jiff::Timestamp::from_millisecond(now.timestamp_millis()).expect("an instant in range");
        let wall = self.tz.to_datetime(at).date();
        let mut day =
            NaiveDate::from_ymd_opt(wall.year().into(), wall.month() as u32, wall.day() as u32)
                .expect("a valid date");
        while self.day_start(day) > now {
            day = day.pred_opt().expect("a previous day");
        }
        while let Some(next) = day.succ_opt() {
            if self.day_start(next) > now {
                break;
            }
            day = next;
        }
        day
    }

    /// `[start, end)` of the local day `now` falls in, in epoch milliseconds.
    pub fn today_bounds(&self, now: DateTime<Utc>) -> (NaiveDate, i64, i64) {
        let today = self.today(now);
        let next = today.succ_opt().expect("a next day");
        (
            today,
            self.day_start(today).timestamp_millis(),
            self.day_start(next).timestamp_millis(),
        )
    }
}

/// Consecutive local days, as epoch-millisecond starts.
#[derive(Debug, Clone)]
pub struct Days {
    pub first: NaiveDate,
    /// `len + 1` starts: day `i` is `[starts[i], starts[i + 1])`.
    starts: Vec<i64>,
}

impl Days {
    /// The `n` (at least one) local days that end with today, today last.
    pub fn ending_today(zone: &Zone, now: DateTime<Utc>, n: u32) -> Days {
        let n = n.max(1);
        let today = zone.today(now);
        let first = today - chrono::Days::new(u64::from(n - 1));
        let starts = (0..=n)
            .map(|i| {
                zone.day_start(first + chrono::Days::new(u64::from(i)))
                    .timestamp_millis()
            })
            .collect();
        Days { first, starts }
    }

    pub fn len(&self) -> usize {
        self.starts.len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// `[start, end)` of day `index`, in epoch milliseconds.
    pub fn bounds(&self, index: usize) -> (i64, i64) {
        (self.starts[index], self.starts[index + 1])
    }

    /// From the start of the first day to the end of the last.
    pub fn span(&self) -> (i64, i64) {
        (self.starts[0], self.starts[self.len()])
    }

    /// The date of day `index`.
    pub fn date(&self, index: i64) -> Option<NaiveDate> {
        usize::try_from(index)
            .ok()
            .filter(|i| *i < self.len())
            .map(|i| self.first + chrono::Days::new(i as u64))
    }

    /// The day index (`0..len`) of an epoch-milliseconds column, as SQL; NULL
    /// outside the days. A run of 24-hour days is one arithmetic branch; a day
    /// of another length (23 or 25 hours, or empty) is a branch of its own.
    /// Only integers the engine computed are written into the SQL.
    pub fn sql_index(&self, col: &str) -> String {
        let mut sql = format!("CASE WHEN {col} < {} THEN NULL", self.starts[0]);
        let mut i = 0;
        while i < self.len() {
            let start = self.starts[i];
            if self.starts[i + 1] - start == DAY_MS {
                let mut end = i + 1;
                while end < self.len() && self.starts[end + 1] - self.starts[end] == DAY_MS {
                    end += 1;
                }
                sql.push_str(&format!(
                    " WHEN {col} < {} THEN {i} + ({col} - {start}) / {DAY_MS}",
                    self.starts[end]
                ));
                i = end;
            } else {
                sql.push_str(&format!(" WHEN {col} < {} THEN {i}", self.starts[i + 1]));
                i += 1;
            }
        }
        sql.push_str(" ELSE NULL END");
        sql
    }
}

#[cfg(test)]
pub(crate) fn utc(text: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(text)
        .expect("an RFC 3339 instant")
        .with_timezone(&Utc)
}

#[cfg(test)]
pub(crate) fn date(text: &str) -> NaiveDate {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").expect("a date")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zone(name: &str) -> Zone {
        Zone::parse(name).unwrap()
    }

    fn time(text: &str) -> NaiveTime {
        NaiveTime::parse_from_str(text, "%H:%M").unwrap()
    }

    /// How many minutes a local day lasts.
    fn minutes(z: &Zone, day: &str) -> i64 {
        let d = date(day);
        (z.day_start(d.succ_opt().unwrap()) - z.day_start(d)).num_minutes()
    }

    #[test]
    fn names_are_iana_names_spelled_as_the_database_spells_them() {
        assert_eq!(zone("Europe/Madrid").name(), "Europe/Madrid");
        assert_eq!(zone("europe/madrid").name(), "Europe/Madrid");
        assert_eq!(zone("US/Eastern").name(), "US/Eastern");
        assert_eq!(zone("utc").name(), "UTC");
        assert_eq!(Zone::default().name(), "UTC");
        for bad in [
            "Mars/Base",
            "",
            " UTC",
            "Europe/Madrid ",
            "+02:00",
            "../etc/passwd",
            "Etc/Unknown",
            "Factory",
            "Bad\nZone",
            &"x".repeat(500),
        ] {
            assert_eq!(Zone::parse(bad).unwrap_err(), INVALID_ZONE, "{bad:?}");
        }
    }

    #[test]
    fn spring_forward_and_fall_back_days_keep_their_length() {
        let ny = zone("America/New_York");
        assert_eq!(
            ny.day_start(date("2026-03-08")),
            utc("2026-03-08T05:00:00Z")
        );
        assert_eq!(minutes(&ny, "2026-03-08"), 23 * 60);
        assert_eq!(
            ny.day_start(date("2026-03-09")),
            utc("2026-03-09T04:00:00Z")
        );
        assert_eq!(minutes(&ny, "2026-11-01"), 25 * 60);
        assert_eq!(
            ny.day_start(date("2026-11-02")),
            utc("2026-11-02T05:00:00Z")
        );
        assert_eq!(minutes(&ny, "2026-06-15"), 24 * 60);
    }

    #[test]
    fn a_skipped_time_is_the_change_and_a_repeated_one_its_first_occurrence() {
        let ny = zone("America/New_York");
        // 02:30 does not exist on 2026-03-08: clocks go from 02:00 EST to 03:00 EDT.
        assert_eq!(
            ny.instant(date("2026-03-08"), time("02:30")),
            utc("2026-03-08T07:00:00Z")
        );
        // The first skipped time is the change too.
        assert_eq!(
            ny.instant(date("2026-03-08"), time("02:00")),
            utc("2026-03-08T07:00:00Z")
        );
        assert_eq!(
            ny.instant(date("2026-03-08"), time("03:00")),
            utc("2026-03-08T07:00:00Z")
        );
        // 01:30 happens twice on 2026-11-01: the first is EDT.
        assert_eq!(
            ny.instant(date("2026-11-01"), time("01:30")),
            utc("2026-11-01T05:30:00Z")
        );
    }

    #[test]
    fn a_skipped_midnight_starts_the_day_at_the_change() {
        // Santiago starts DST at 24:00: 2025-09-07 begins at 01:00 (-03).
        let scl = zone("America/Santiago");
        assert_eq!(
            scl.day_start(date("2025-09-07")),
            utc("2025-09-07T04:00:00Z")
        );
        assert_eq!(minutes(&scl, "2025-09-06"), 24 * 60);
        assert_eq!(minutes(&scl, "2025-09-07"), 23 * 60);
    }

    #[test]
    fn a_repeated_midnight_starts_the_day_at_its_first_occurrence() {
        // Havana fell back at 01:00 to 00:00 on 2025-11-02.
        let hav = zone("America/Havana");
        assert_eq!(
            hav.day_start(date("2025-11-02")),
            utc("2025-11-02T04:00:00Z")
        );
        assert_eq!(minutes(&hav, "2025-11-02"), 25 * 60);
        // During the second 00:xx the day is still 2025-11-02.
        assert_eq!(hav.today(utc("2025-11-02T05:10:00Z")), date("2025-11-02"));
    }

    #[test]
    fn a_change_back_at_midnight_lengthens_the_day_before() {
        // Santiago, 2025-04-06 03:00Z: Saturday 24:00 (-03) back to 23:00 (-04),
        // so Sunday's midnight is 04:00Z and Saturday lasts 25 hours.
        let scl = zone("America/Santiago");
        assert_eq!(
            scl.day_start(date("2025-04-06")),
            utc("2025-04-06T04:00:00Z")
        );
        assert_eq!(scl.today(utc("2025-04-06T03:30:00Z")), date("2025-04-05"));
        assert_eq!(minutes(&scl, "2025-04-05"), 25 * 60);
        assert_eq!(minutes(&scl, "2025-04-06"), 24 * 60);
    }

    #[test]
    fn a_day_can_begin_while_the_clock_still_shows_the_day_before() {
        // St. John's fell back from 00:01 to 23:01 the day before on 2009-11-01.
        let sj = zone("America/St_Johns");
        assert_eq!(
            sj.day_start(date("2009-11-01")),
            utc("2009-11-01T02:30:00Z")
        );
        assert_eq!(sj.today(utc("2009-11-01T02:45:00Z")), date("2009-11-01"));
        assert_eq!(sj.today(utc("2009-11-01T02:29:59Z")), date("2009-10-31"));
    }

    #[test]
    fn a_gap_resolves_to_the_change_to_the_second() {
        // Monrovia skipped from 23:59:59.x to 00:44:30 on 1972-01-07.
        let mon = zone("Africa/Monrovia");
        assert_eq!(
            mon.day_start(date("1972-01-07")),
            utc("1972-01-07T00:44:30Z")
        );
    }

    #[test]
    fn a_skipped_date_is_an_empty_day() {
        let apia = zone("Pacific/Apia");
        assert_eq!(minutes(&apia, "2011-12-30"), 0);
        assert_eq!(
            apia.today(apia.day_start(date("2011-12-31"))),
            date("2011-12-31")
        );
    }

    #[test]
    fn the_built_in_database_is_current() {
        // British Columbia stays on UTC-07 from 2026-11-01 (tzdb 2026a and later).
        let van = zone("America/Vancouver");
        assert_eq!(
            van.day_start(date("2026-11-02")),
            utc("2026-11-02T07:00:00Z")
        );
        assert_eq!(
            van.instant(date("2026-11-02"), time("06:00")),
            utc("2026-11-02T13:00:00Z")
        );
    }

    #[test]
    fn zones_without_dst_and_utc() {
        let kol = zone("Asia/Kolkata");
        assert_eq!(
            kol.day_start(date("2026-11-20")),
            utc("2026-11-19T18:30:00Z")
        );
        assert_eq!(
            kol.today(utc("2026-11-19T18:29:59.999Z")),
            date("2026-11-19")
        );
        assert_eq!(kol.today(utc("2026-11-19T18:30:00Z")), date("2026-11-20"));
        let ktm = zone("Asia/Kathmandu");
        assert_eq!(
            ktm.day_start(date("2026-11-20")),
            utc("2026-11-19T18:15:00Z")
        );
        let z = Zone::utc();
        assert_eq!(z.day_start(date("2026-11-20")), utc("2026-11-20T00:00:00Z"));
        assert_eq!(
            z.today_bounds(utc("2026-11-20T23:59:59.999Z")),
            (
                date("2026-11-20"),
                utc("2026-11-20T00:00:00Z").timestamp_millis(),
                utc("2026-11-21T00:00:00Z").timestamp_millis()
            )
        );
    }

    #[test]
    fn days_end_today() {
        let ny = zone("America/New_York");
        let days = Days::ending_today(&ny, utc("2026-11-03T12:00:00Z"), 5);
        assert_eq!(days.first, date("2026-10-30"));
        assert_eq!(days.len(), 5);
        assert_eq!(days.date(4), Some(date("2026-11-03")));
        assert_eq!(days.date(5), None);
        assert_eq!(days.date(-1), None);
        assert_eq!(
            days.bounds(4),
            (
                utc("2026-11-03T05:00:00Z").timestamp_millis(),
                utc("2026-11-04T05:00:00Z").timestamp_millis()
            )
        );
        assert_eq!(days.bounds(2).1 - days.bounds(2).0, 25 * 3_600_000);
    }

    /// Every boundary of every day, through the store's own SQL (SQLite, or
    /// PostgreSQL under `HIE_TEST_DATABASE_URL`), with no other filter.
    #[test]
    fn the_sql_day_index_puts_each_instant_in_its_day() {
        let db = crate::db::Db::in_memory().unwrap();
        for (name, now) in [
            ("America/New_York", "2026-11-03T12:00:00Z"),
            ("America/Havana", "2025-11-05T12:00:00Z"),
            ("America/Santiago", "2025-09-10T12:00:00Z"),
            ("America/St_Johns", "2009-11-03T12:00:00Z"),
            ("Asia/Kathmandu", "2026-11-03T12:00:00Z"),
            ("UTC", "2026-11-03T12:00:00Z"),
            ("Pacific/Apia", "2012-01-02T12:00:00Z"),
        ] {
            let days = Days::ending_today(&zone(name), utc(now), 250);
            let index = |at: i64| -> Option<i64> {
                let rows = db
                    .query_json(
                        &format!(
                            "SELECT {} AS i FROM (SELECT CAST($1 AS BIGINT) AS at) t",
                            days.sql_index("at")
                        ),
                        crate::db::params![at],
                    )
                    .unwrap();
                rows[0]["i"].as_i64()
            };
            let (first, last) = days.span();
            assert_eq!(index(first - 1), None, "{name}: before the days");
            assert_eq!(index(last), None, "{name}: after the days");
            assert_eq!(index(first), Some(0), "{name}");
            for i in 0..days.len() {
                let (start, end) = days.bounds(i);
                if start == end {
                    continue; // an empty day holds no instant
                }
                for at in [start, start + 1, (start + end) / 2, end - 1] {
                    assert_eq!(index(at), Some(i as i64), "{name}: day {i} at {at}");
                }
            }
        }
        // Apia skipped 2011-12-30: the shared boundary is 12-31's.
        let apia = Days::ending_today(&zone("Pacific/Apia"), utc("2012-01-01T00:00:00Z"), 5);
        let skipped = (0..apia.len())
            .find(|i| apia.date(*i as i64) == Some(date("2011-12-30")))
            .unwrap();
        let (start, end) = apia.bounds(skipped);
        assert_eq!(start, end);
        assert_eq!(apia.bounds(skipped + 1).0, start);
    }
}
