//! The canonical capture clock.
//!
//! Every timestamp Kaseta records is a reading of `CLOCK_BOOTTIME`.
//!
//! The wall clock is unusable here: NTP steps and daylight-saving changes would
//! reorder or duplicate timestamps mid-recording. `CLOCK_MONOTONIC` is closer
//! but stops advancing while the system is suspended, so closing a laptop lid
//! during a meeting would collapse the gap and misalign every track afterwards.
//! `CLOCK_BOOTTIME` is monotonic *and* counts suspended time, which is exactly
//! the property track alignment needs.

/// Reads the canonical clock, in nanoseconds since boot.
#[cfg(target_os = "linux")]
pub fn boottime_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid, properly aligned `timespec` for the duration of
    // the call, and `CLOCK_BOOTTIME` is available on every supported kernel.
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut ts) };
    debug_assert_eq!(rc, 0, "clock_gettime(CLOCK_BOOTTIME) failed");
    (ts.tv_sec as u64) * 1_000_000_000 + (ts.tv_nsec as u64)
}

/// Non-Linux builds exist only so the contracts and storage layers stay testable
/// off the target platform. Capture itself is Linux-only.
#[cfg(not(target_os = "linux"))]
pub fn boottime_ns() -> u64 {
    use std::sync::OnceLock;
    use std::time::Instant;
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    ORIGIN.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

/// Converts a sample count at a given rate into nanoseconds.
///
/// Uses 128-bit intermediate arithmetic: a long recording at 48 kHz overflows
/// `u64` when multiplied by 1e9 after roughly ten hours, which is well within
/// the range of a recording someone might actually leave running.
pub fn samples_to_ns(samples: u64, sample_rate_hz: u32) -> u64 {
    if sample_rate_hz == 0 {
        return 0;
    }
    ((samples as u128 * 1_000_000_000u128) / sample_rate_hz as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn the_clock_advances_and_never_goes_backwards() {
        let a = boottime_ns();
        std::thread::sleep(Duration::from_millis(5));
        let b = boottime_ns();
        assert!(b > a, "canonical clock must be strictly monotonic");
        assert!(
            b - a >= 4_000_000,
            "expected at least ~5ms to have elapsed, saw {}ns",
            b - a
        );
    }

    #[test]
    fn converts_samples_to_nanoseconds() {
        assert_eq!(samples_to_ns(48_000, 48_000), 1_000_000_000);
        assert_eq!(samples_to_ns(24_000, 48_000), 500_000_000);
        assert_eq!(samples_to_ns(0, 48_000), 0);
    }

    #[test]
    fn long_recordings_do_not_overflow() {
        // Twelve hours at 48 kHz. This overflows a naive u64 multiplication.
        let samples = 48_000u64 * 3_600 * 12;
        let ns = samples_to_ns(samples, 48_000);
        assert_eq!(ns, 12 * 3_600 * 1_000_000_000);
    }

    #[test]
    fn a_zero_sample_rate_does_not_divide_by_zero() {
        assert_eq!(samples_to_ns(1_000, 0), 0);
    }
}
