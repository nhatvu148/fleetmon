//! Durable history in SQLite: every sample for a couple of hours, and one
//! averaged row per host per minute for a month.
//!
//! Only the scalar part of a sample is stored (see [`Sample::slim`]) — those are
//! what the charts plot. Lists such as processes and disks are only ever shown
//! for the newest sample, which lives in memory.
//!
//! Writes go through one thread fed by a channel, so recording a sample never
//! blocks the async runtime. Reads use their own connection (WAL mode lets them
//! run alongside the writer) and are meant to be called from `spawn_blocking`.

use std::{
    path::Path,
    sync::{Mutex, mpsc},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use fleetmon_proto::Sample;
use rusqlite::{Connection, OpenFlags, Row, params};

/// Every sample is kept this long — enough for the 1 h range with margin.
const RAW_KEEP: Duration = Duration::from_secs(2 * 3600);
/// Minute averages are kept this long — the 30 d range with margin.
const MINUTE_KEEP: Duration = Duration::from_secs(31 * 86400);
/// Points a range query returns at most: about one per two pixels of a chart.
pub const POINTS: i64 = 360;
/// Samples waiting for the writer, at most. A few machines produce a few per
/// second, so this is minutes of slack if the disk stalls — and a hard bound
/// on memory if an agent floods, instead of growing until the hub is killed.
const QUEUE: usize = 20_000;

/// The columns stored per sample, in one place so every query agrees.
const COLS: &str = "ts, cpu_pct, mem_used, swap_used, swap_total, net_rx_bps, net_tx_bps, \
    uptime_s, cpu_freq_mhz, load1, load5, load15, mem_available, disk_read_bps, \
    disk_write_bps, temp_max_c, proc_count";

/// How each column is combined when rows are averaged into one.
fn averaged(ts: &str) -> String {
    format!(
        "{ts} AS ts, AVG(cpu_pct), AVG(mem_used), AVG(swap_used), MAX(swap_total), \
         AVG(net_rx_bps), AVG(net_tx_bps), MAX(uptime_s), AVG(cpu_freq_mhz), \
         AVG(load1), AVG(load5), AVG(load15), AVG(mem_available), AVG(disk_read_bps), \
         AVG(disk_write_bps), AVG(temp_max_c), AVG(proc_count)"
    )
}

const SCHEMA: &str = "
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    CREATE TABLE IF NOT EXISTS raw (
        host TEXT NOT NULL, ts INTEGER NOT NULL,
        cpu_pct REAL, mem_used REAL, swap_used REAL, swap_total REAL,
        net_rx_bps REAL, net_tx_bps REAL, uptime_s REAL, cpu_freq_mhz REAL,
        load1 REAL, load5 REAL, load15 REAL, mem_available REAL,
        disk_read_bps REAL, disk_write_bps REAL, temp_max_c REAL, proc_count REAL
    );
    CREATE INDEX IF NOT EXISTS raw_host_ts ON raw (host, ts);
    CREATE TABLE IF NOT EXISTS minute (
        host TEXT NOT NULL, ts INTEGER NOT NULL,
        cpu_pct REAL, mem_used REAL, swap_used REAL, swap_total REAL,
        net_rx_bps REAL, net_tx_bps REAL, uptime_s REAL, cpu_freq_mhz REAL,
        load1 REAL, load5 REAL, load15 REAL, mem_available REAL,
        disk_read_bps REAL, disk_write_bps REAL, temp_max_c REAL, proc_count REAL,
        PRIMARY KEY (host, ts)
    );
    -- The newest minute already averaged into `minute`, so each is done once.
    CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
";

/// The time windows a chart can show.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Range {
    Hour,
    Day,
    Week,
    Month,
}

impl Range {
    pub fn parse(s: &str) -> Option<Range> {
        match s {
            "1h" => Some(Range::Hour),
            "24h" => Some(Range::Day),
            "7d" => Some(Range::Week),
            "30d" => Some(Range::Month),
            _ => None,
        }
    }

    fn span(self) -> Duration {
        Duration::from_secs(match self {
            Range::Hour => 3600,
            Range::Day => 86400,
            Range::Week => 7 * 86400,
            Range::Month => 30 * 86400,
        })
    }

    /// The hour is drawn from every sample; longer ranges from minute averages.
    fn table(self) -> &'static str {
        match self {
            Range::Hour => "raw",
            _ => "minute",
        }
    }
}

pub struct Store {
    writes: mpsc::SyncSender<(String, Sample)>,
    reader: Mutex<Connection>,
    dropped: std::sync::atomic::AtomicU64,
}

impl Store {
    pub fn open(path: &Path) -> Result<Store> {
        let writer =
            Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        writer
            .execute_batch(SCHEMA)
            .context("creating the schema")?;
        let reader = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        let (tx, rx) = mpsc::sync_channel(QUEUE);
        std::thread::Builder::new()
            .name("fleetmon-store".into())
            .spawn(move || write_loop(writer, rx))?;
        Ok(Store {
            writes: tx,
            reader: Mutex::new(reader),
            dropped: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Queues a sample for writing. Never blocks: when the queue is full the
    /// sample is dropped from history (it is still shown live) and counted.
    pub fn record(&self, host: &str, sample: &Sample) {
        use std::sync::atomic::Ordering;
        if self
            .writes
            .try_send((host.to_string(), sample.slim()))
            .is_err()
        {
            let n = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            // First drop, then one line per thousand: enough to notice.
            if n == 1 || n.is_multiple_of(1000) {
                tracing::warn!(dropped = n, "history queue full; samples not stored");
            }
        }
    }

    /// The newest `n` samples for a host, oldest first. Blocking.
    pub fn recent(&self, host: &str, n: usize) -> Result<Vec<Sample>> {
        let db = self.reader.lock().unwrap();
        let mut stmt = db.prepare(&format!(
            "SELECT {COLS} FROM raw WHERE host = ?1 ORDER BY ts DESC LIMIT ?2"
        ))?;
        let mut rows = stmt
            .query_map(params![host, n as i64], row_to_sample)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        rows.reverse();
        Ok(rows)
    }

    /// A host's history over `range`, averaged down to at most [`POINTS`]
    /// evenly spaced points, oldest first. Blocking.
    pub fn history(&self, host: &str, range: Range, now_ms: i64) -> Result<Vec<Sample>> {
        let span = range.span().as_millis() as i64;
        let bucket = (span / POINTS).max(1);
        let db = self.reader.lock().unwrap();
        let mut stmt = db.prepare(&format!(
            "SELECT {} FROM {} WHERE host = ?1 AND ts >= ?2 \
             GROUP BY ts / ?3 ORDER BY ts",
            averaged("(ts / ?3) * ?3 + ?3 / 2"),
            range.table()
        ))?;
        let rows = stmt
            .query_map(params![host, now_ms - span, bucket], row_to_sample)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }
}

fn row_to_sample(r: &Row) -> rusqlite::Result<Sample> {
    let f = |i: usize| -> rusqlite::Result<Option<f64>> { r.get(i) };
    let u =
        |i: usize| -> rusqlite::Result<u64> { Ok(f(i)?.unwrap_or(0.0).max(0.0).round() as u64) };
    let load = match (f(9)?, f(10)?, f(11)?) {
        (Some(a), Some(b), Some(c)) => Some([a, b, c]),
        _ => None,
    };
    Ok(Sample {
        ts_ms: r.get::<_, i64>(0)? as u64,
        cpu_pct: f(1)?.unwrap_or(0.0) as f32,
        mem_used: u(2)?,
        swap_used: u(3)?,
        swap_total: u(4)?,
        net_rx_bps: u(5)?,
        net_tx_bps: u(6)?,
        uptime_s: u(7)?,
        cpu_freq_mhz: u(8)?,
        load_avg: load,
        mem_available: u(12)?,
        disk_read_bps: u(13)?,
        disk_write_bps: u(14)?,
        temp_max_c: f(15)?.map(|v| v as f32),
        proc_count: u(16)? as u32,
        ..Default::default()
    })
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn write_loop(mut db: Connection, rx: mpsc::Receiver<(String, Sample)>) {
    let mut last_prune = 0i64;
    loop {
        // Batch whatever arrived in the last second into one transaction.
        let mut batch = Vec::new();
        match rx.recv_timeout(Duration::from_secs(1)) {
            Ok(first) => {
                batch.push(first);
                batch.extend(rx.try_iter());
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        }
        let now = now_ms();
        let prune = now - last_prune >= 3_600_000;
        if let Err(e) = write(&mut db, &batch, now, prune) {
            tracing::warn!("history write failed: {e:#}");
        } else if prune {
            last_prune = now;
        }
    }
}

fn write(db: &mut Connection, batch: &[(String, Sample)], now: i64, prune: bool) -> Result<()> {
    let tx = db.transaction()?;
    {
        let mut ins = tx.prepare_cached(&format!(
            "INSERT INTO raw (host, {COLS}) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, \
             ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)"
        ))?;
        for (host, s) in batch {
            let load = s.load_avg;
            ins.execute(params![
                host,
                s.ts_ms as i64,
                s.cpu_pct as f64,
                s.mem_used as f64,
                s.swap_used as f64,
                s.swap_total as f64,
                s.net_rx_bps as f64,
                s.net_tx_bps as f64,
                s.uptime_s as f64,
                s.cpu_freq_mhz as f64,
                load.map(|l| l[0]),
                load.map(|l| l[1]),
                load.map(|l| l[2]),
                s.mem_available as f64,
                s.disk_read_bps as f64,
                s.disk_write_bps as f64,
                s.temp_max_c.map(|t| t as f64),
                s.proc_count as f64,
            ])?;
        }
    }
    roll_up(&tx, now)?;
    if prune {
        tx.execute(
            "DELETE FROM raw WHERE ts < ?1",
            params![now - RAW_KEEP.as_millis() as i64],
        )?;
        tx.execute(
            "DELETE FROM minute WHERE ts < ?1",
            params![now - MINUTE_KEEP.as_millis() as i64],
        )?;
    }
    tx.commit()?;
    Ok(())
}

/// Averages every finished minute not yet in `minute` into it. A minute is
/// finished once the clock has moved past it; samples that arrive later for an
/// already rolled-up minute (a reconnecting agent) only reach `raw`.
fn roll_up(tx: &rusqlite::Transaction, now: i64) -> Result<()> {
    let current = now / 60_000 * 60_000;
    let done: i64 = tx
        .query_row(
            "SELECT value FROM meta WHERE key = 'rolled_up_to'",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0);
    if done >= current {
        return Ok(());
    }
    let from = done.max(current - RAW_KEEP.as_millis() as i64);
    tx.execute(
        &format!(
            "INSERT OR REPLACE INTO minute (host, {COLS}) \
             SELECT host, {} FROM raw WHERE ts >= ?1 AND ts < ?2 GROUP BY host, ts / 60000",
            averaged("ts / 60000 * 60000")
        ),
        params![from, current],
    )?;
    tx.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('rolled_up_to', ?1)",
        params![current],
    )?;
    Ok(())
}

/// Opens the store if a path was configured. Kept separate so `main` can
/// report a bad path before the server starts.
pub fn open(path: Option<&Path>) -> Result<Option<Store>> {
    match path {
        None => Ok(None),
        Some(p) if p.as_os_str().is_empty() => bail!("--db is empty"),
        Some(p) => Store::open(p).map(Some),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db(name: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "fleetmon-{name}-{}-{}.db",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_file(&p);
        p
    }

    fn at(ts_ms: i64, cpu: f32) -> Sample {
        Sample {
            ts_ms: ts_ms as u64,
            cpu_pct: cpu,
            mem_used: 1000,
            load_avg: Some([1.0, 2.0, 3.0]),
            temp_max_c: None,
            ..Default::default()
        }
    }

    #[test]
    fn samples_round_trip_and_average_into_buckets() {
        let path = temp_db("rt");
        let store = Store::open(&path).unwrap();
        let mut db = Connection::open(&path).unwrap();
        let now = 10 * 3_600_000;
        // Two samples in one 10 s bucket of the hour range, one in another.
        let batch = vec![
            ("a".to_string(), at(now - 30_000, 10.0)),
            ("a".to_string(), at(now - 29_000, 30.0)),
            ("a".to_string(), at(now - 5_000, 50.0)),
            ("b".to_string(), at(now - 5_000, 99.0)),
        ];
        write(&mut db, &batch, now, false).unwrap();

        let recent = store.recent("a", 10).unwrap();
        assert_eq!(recent.len(), 3);
        assert!(
            recent.windows(2).all(|w| w[0].ts_ms < w[1].ts_ms),
            "oldest first"
        );
        assert_eq!(recent[0].load_avg, Some([1.0, 2.0, 3.0]));
        assert_eq!(recent[0].temp_max_c, None, "absent stays absent");

        let h = store.history("a", Range::Hour, now).unwrap();
        let cpus: Vec<f32> = h.iter().map(|s| s.cpu_pct).collect();
        assert_eq!(
            cpus,
            [20.0, 50.0],
            "averaged per bucket, other hosts excluded"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn finished_minutes_roll_up_once_and_old_rows_are_pruned() {
        let path = temp_db("roll");
        let store = Store::open(&path).unwrap();
        let mut db = Connection::open(&path).unwrap();
        let t0 = 100 * 60_000; // a minute boundary
        let batch: Vec<_> = (0..60)
            .map(|i| ("a".to_string(), at(t0 + i * 1000, i as f32)))
            .collect();
        // Still inside the minute: nothing rolled up yet.
        write(&mut db, &batch, t0 + 59_500, false).unwrap();
        assert!(
            store
                .history("a", Range::Day, t0 + 59_500)
                .unwrap()
                .is_empty()
        );
        // The clock moves on: the minute is averaged exactly once.
        write(&mut db, &[], t0 + 61_000, false).unwrap();
        write(&mut db, &[], t0 + 62_000, false).unwrap();
        let day = store.history("a", Range::Day, t0 + 62_000).unwrap();
        assert_eq!(day.len(), 1);
        assert!((day[0].cpu_pct - 29.5).abs() < 1e-3, "mean of 0..59");

        // Far in the future, pruning empties raw but keeps the minute row.
        let later = t0 + 3 * 3_600_000;
        write(&mut db, &[], later, true).unwrap();
        assert!(store.recent("a", 100).unwrap().is_empty());
        assert_eq!(store.history("a", Range::Day, later).unwrap().len(), 1);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn ranges_parse() {
        assert_eq!(Range::parse("1h"), Some(Range::Hour));
        assert_eq!(Range::parse("30d"), Some(Range::Month));
        assert_eq!(
            Range::parse("5m"),
            None,
            "the live window is served from memory"
        );
    }
}
