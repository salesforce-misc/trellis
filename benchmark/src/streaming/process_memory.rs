//! The benchmark process's own memory, sampled in-process (issue #620 A3).
//!
//! Every streaming scenario runs the engine inside this process (a
//! [`trellis::Client`]), so `VmRSS` here is the engine's memory plus the
//! harness's (a few counters and fixed-size histograms). #617 sampled it with
//! a side script; [`RssSampler`] does the same from a plain OS thread, so a
//! runtime starved by the engine's own work can't stretch the sampling
//! interval, and reports:
//!
//! - the peak sampled `VmRSS`, over the whole run and per phase (the caller
//!   closes a phase with [`RssSampler::end_phase`]);
//! - `VmHWM` at the end, the kernel's own high-water mark, which also
//!   catches a spike shorter than the interval;
//! - the enclosing cgroup's `memory.peak` (cgroup v2) **since the sampler
//!   started**, when there is one. Under `systemd-run --scope -p
//!   MemoryMax=...` that is the usage the cap is enforced against. It counts
//!   the cluster's postgres processes and the page cache they fault in (on
//!   tmpfs, the cluster's files themselves), so it runs well above the
//!   process's RSS. The cgroup's own lifetime peak would also count whatever
//!   ran in the scope first, and `bench` runs `cargo build` there, so the
//!   sampler resets a private watermark on its own `memory.peak` descriptor
//!   (Linux 6.12+) and reads that; `null` where the kernel can't. Only
//!   meaningful when the benchmark has the cgroup to itself, i.e. under its
//!   own scope.
//!
//! Optionally it prints one progress line to stderr per `progress` interval
//! (elapsed, RSS, peak), so a long run's log shows memory over time without
//! a sampler script next to it.

use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// The default sampling interval: fine enough to see a drain batch's step
/// (#617 saw 2.5-3 GB per batch over seconds), cheap enough to be free.
pub const DEFAULT_INTERVAL: Duration = Duration::from_millis(250);

/// One `key: <n> kB` field of a `/proc/<pid>/status` text, in bytes.
fn status_kb(status: &str, key: &str) -> Option<u64> {
    status.lines().find_map(|line| {
        let rest = line.strip_prefix(key)?.strip_prefix(':')?;
        let kb = rest.trim().strip_suffix("kB")?.trim().parse::<u64>().ok()?;
        Some(kb * 1024)
    })
}

fn self_status() -> String {
    std::fs::read_to_string("/proc/self/status").unwrap_or_default()
}

/// This process's resident set now, in bytes (`None` off Linux).
pub fn rss_bytes() -> Option<u64> {
    status_kb(&self_status(), "VmRSS")
}

/// The unified-hierarchy cgroup path in a `/proc/self/cgroup` text
/// (the `0::<path>` line).
fn cgroup_v2_path(cgroup: &str) -> Option<&str> {
    cgroup.lines().find_map(|line| line.strip_prefix("0::"))
}

/// The enclosing cgroup's `memory.peak`, opened read-write with its
/// watermark reset for this descriptor only: reading it back gives the
/// cgroup's peak usage since now (cgroup v2 on Linux 6.12+; `None` without
/// the file or where the kernel refuses the write). Other readers of the file,
/// and the cap, are unaffected.
fn cgroup_memory_peak_since_now() -> Option<File> {
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    let path = cgroup_v2_path(&cgroup)?;
    let mut file = File::options()
        .read(true)
        .write(true)
        .open(format!("/sys/fs/cgroup{path}/memory.peak"))
        .ok()?;
    file.write_all(b"reset\n").ok()?;
    Some(file)
}

/// The peak, in bytes, that a [`cgroup_memory_peak_since_now`] descriptor
/// has seen.
fn read_peak(file: &mut File) -> Option<u64> {
    let mut text = String::new();
    file.seek(SeekFrom::Start(0)).ok()?;
    file.read_to_string(&mut text).ok()?;
    text.trim().parse().ok()
}

/// Peak sampled RSS per phase, in the order the phases ended.
#[derive(Debug, Clone, Default)]
pub struct RssSummary {
    /// The highest sampled `VmRSS` over the run.
    pub peak_bytes: u64,
    /// Seconds from [`RssSampler::start`] to that sample.
    pub peak_at_secs: f64,
    /// `VmHWM` when the sampler stopped (`None` off Linux).
    pub hwm_bytes: Option<u64>,
    /// The cgroup's peak usage from [`RssSampler::start`] to
    /// [`RssSampler::finish`] (`None` without cgroup v2's `memory.peak`, or
    /// on a kernel that can't reset it per descriptor).
    pub cgroup_peak_bytes: Option<u64>,
    /// `(phase, peak sampled VmRSS in it)` for every [`RssSampler::end_phase`].
    pub phases: Vec<(&'static str, u64)>,
    pub samples: u64,
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / 1_000_000.0
}

fn json_opt_mb(v: Option<u64>) -> String {
    v.map(|b| format!("{:.1}", mb(b)))
        .unwrap_or_else(|| "null".into())
}

impl RssSummary {
    /// The JSON fields, comma-separated with no surrounding comma: the
    /// overall peak, `VmHWM`, the cgroup peak, and one `peak_rss_<phase>_mb`
    /// per phase.
    pub fn json_fields(&self) -> String {
        let mut out = format!(
            "\"peak_rss_mb\":{:.1},\"peak_rss_at_secs\":{:.1},\"rss_hwm_mb\":{},\
             \"cgroup_mem_peak_mb\":{},\"rss_samples\":{}",
            mb(self.peak_bytes),
            self.peak_at_secs,
            json_opt_mb(self.hwm_bytes),
            json_opt_mb(self.cgroup_peak_bytes),
            self.samples,
        );
        for (phase, peak) in &self.phases {
            out.push_str(&format!(",\"peak_rss_{phase}_mb\":{:.1}", mb(*peak)));
        }
        out
    }

    pub fn human(&self) -> String {
        let phases = self
            .phases
            .iter()
            .map(|(p, b)| format!("{p} {:.0} MB", mb(*b)))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "peak RSS {:.0} MB at +{:.0}s ({phases}), VmHWM {} MB, cgroup peak {} MB",
            mb(self.peak_bytes),
            self.peak_at_secs,
            json_opt_mb(self.hwm_bytes),
            json_opt_mb(self.cgroup_peak_bytes),
        )
    }
}

struct Shared {
    stop: AtomicBool,
    peak: AtomicU64,
    phase_peak: AtomicU64,
    peak_at_ms: AtomicU64,
    samples: AtomicU64,
    phases: Mutex<Vec<(&'static str, u64)>>,
}

/// Samples this process's `VmRSS` on its own thread until [`finish`](Self::finish).
pub struct RssSampler {
    shared: Arc<Shared>,
    thread: Option<JoinHandle<()>>,
    cgroup_peak: Option<File>,
}

impl RssSampler {
    /// Starts sampling every `interval`; with `progress`, also prints
    /// `<label>: +<s>s rss <MB> MB (peak <MB> MB)` to stderr that often.
    pub fn start(interval: Duration, progress: Option<(Duration, &'static str)>) -> Self {
        let cgroup_peak = cgroup_memory_peak_since_now();
        let shared = Arc::new(Shared {
            stop: AtomicBool::new(false),
            peak: AtomicU64::new(0),
            phase_peak: AtomicU64::new(0),
            peak_at_ms: AtomicU64::new(0),
            samples: AtomicU64::new(0),
            phases: Mutex::new(Vec::new()),
        });
        let thread_shared = shared.clone();
        let thread = std::thread::Builder::new()
            .name("rss-sampler".into())
            .spawn(move || {
                let s = thread_shared;
                let start = Instant::now();
                let mut next_progress = progress.map(|(every, _)| start + every);
                while !s.stop.load(Ordering::Relaxed) {
                    if let Some(rss) = rss_bytes() {
                        s.samples.fetch_add(1, Ordering::Relaxed);
                        s.phase_peak.fetch_max(rss, Ordering::Relaxed);
                        if s.peak.fetch_max(rss, Ordering::Relaxed) < rss {
                            s.peak_at_ms
                                .store(start.elapsed().as_millis() as u64, Ordering::Relaxed);
                        }
                        if let (Some(at), Some((every, label))) = (next_progress, progress)
                            && Instant::now() >= at
                        {
                            eprintln!(
                                "{label}: +{:.0}s rss {:.0} MB (peak {:.0} MB)",
                                start.elapsed().as_secs_f64(),
                                mb(rss),
                                mb(s.peak.load(Ordering::Relaxed)),
                            );
                            next_progress = Some(at + every);
                        }
                    }
                    std::thread::sleep(interval);
                }
            })
            .expect("spawn the RSS sampler thread");
        RssSampler {
            shared,
            thread: Some(thread),
            cgroup_peak,
        }
    }

    /// Closes the current phase: records its peak sampled RSS under `name`
    /// (topped up with a fresh reading) and starts the next phase's peak from
    /// the current RSS.
    pub fn end_phase(&self, name: &'static str) {
        let now = rss_bytes().unwrap_or(0);
        let peak = self.shared.phase_peak.swap(now, Ordering::Relaxed).max(now);
        self.shared.peak.fetch_max(now, Ordering::Relaxed);
        self.shared
            .phases
            .lock()
            .expect("phase list")
            .push((name, peak));
    }

    /// Stops the thread and reports; `VmHWM` and the cgroup peak are read now.
    pub fn finish(mut self) -> RssSummary {
        self.shared.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.thread.take() {
            t.join().expect("RSS sampler thread");
        }
        let s = &self.shared;
        RssSummary {
            peak_bytes: s.peak.load(Ordering::Relaxed),
            peak_at_secs: s.peak_at_ms.load(Ordering::Relaxed) as f64 / 1000.0,
            hwm_bytes: status_kb(&self_status(), "VmHWM"),
            cgroup_peak_bytes: self.cgroup_peak.as_mut().and_then(read_peak),
            phases: s.phases.lock().expect("phase list").clone(),
            samples: s.samples.load(Ordering::Relaxed),
        }
    }
}

impl Drop for RssSampler {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATUS: &str = "Name:\tbenchmark\nVmPeak:\t 9000 kB\nVmHWM:\t    2048 kB\n\
                          VmRSS:\t    1024 kB\nThreads:\t12\n";

    #[test]
    fn status_fields_parse_to_bytes() {
        assert_eq!(status_kb(STATUS, "VmRSS"), Some(1024 * 1024));
        assert_eq!(status_kb(STATUS, "VmHWM"), Some(2048 * 1024));
        // A prefix of another key is not that key.
        assert_eq!(status_kb(STATUS, "Vm"), None);
        assert_eq!(status_kb(STATUS, "Threads"), None);
    }

    #[test]
    fn the_unified_cgroup_line_is_found() {
        let cgroup = "12:cpu:/x\n0::/user.slice/user-1000.slice/run-r1.scope\n";
        assert_eq!(
            cgroup_v2_path(cgroup),
            Some("/user.slice/user-1000.slice/run-r1.scope")
        );
        assert_eq!(cgroup_v2_path("12:cpu:/x\n"), None);
    }

    #[test]
    fn a_sampler_reports_this_process_and_its_phases() {
        let sampler = RssSampler::start(Duration::from_millis(5), None);
        // 32 MB, every page touched, inside the sampled window.
        let ballast = std::hint::black_box(vec![1u8; 32 << 20]);
        std::thread::sleep(Duration::from_millis(30));
        drop(ballast);
        sampler.end_phase("first");
        sampler.end_phase("second");
        let s = sampler.finish();
        if rss_bytes().is_none() {
            return; // not Linux: nothing to read
        }
        assert!(s.samples > 0 && s.peak_bytes > 0, "{s:?}");
        assert_eq!(
            s.phases.iter().map(|(p, _)| *p).collect::<Vec<_>>(),
            ["first", "second"]
        );
        assert!(s.phases.iter().all(|&(_, b)| b > 0 && b <= s.peak_bytes));
        // Both saw the ballast. Not `hwm >= peak`: the kernel's high-water
        // mark comes from approximate per-CPU RSS counters, and after a large
        // free it can read a little under a sampled (exact) `VmRSS`.
        assert!(s.peak_bytes >= 32 << 20, "{s:?}");
        assert!(s.hwm_bytes.unwrap() >= 32 << 20, "{s:?}");
        // The cgroup's peak since start includes the ballast (where this
        // kernel lets the sampler reset its own watermark at all).
        if let Some(cgroup_peak) = s.cgroup_peak_bytes {
            assert!(cgroup_peak >= 32 << 20, "{s:?}");
        }
        let json = s.json_fields();
        assert!(json.contains("\"peak_rss_first_mb\":"), "{json}");
        assert!(json.contains("\"peak_rss_second_mb\":"), "{json}");
    }
}
