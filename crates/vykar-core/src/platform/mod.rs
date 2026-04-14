pub mod fs;
pub use vykar_common::paths;
pub mod shell;

/// Strip the domain suffix from a hostname (everything after the first `.`).
pub fn strip_hostname_domain(hostname: &str) -> String {
    match hostname.find('.') {
        Some(pos) => hostname[..pos].to_string(),
        None => hostname.to_string(),
    }
}

/// Return the short hostname (domain stripped).
/// On macOS, gethostname() returns a network-dependent FQDN; truncating
/// at the first dot yields the stable local hostname. On Linux/Windows
/// this is typically a no-op.
pub fn short_hostname() -> String {
    strip_hostname_domain(&hostname())
}

/// Return the system hostname, or `"unknown"` if it cannot be determined.
pub fn hostname() -> String {
    #[cfg(unix)]
    {
        nix::unistd::gethostname()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".into())
    }

    #[cfg(windows)]
    {
        std::env::var("COMPUTERNAME").unwrap_or_else(|_| "unknown".into())
    }
}

/// Check whether a process with the given PID is alive on the local machine.
///
/// On Unix, uses `kill(pid, 0)`: returns `true` if the process exists (even if
/// owned by another user — `EPERM` still means alive). On non-Unix platforms,
/// conservatively returns `true` to avoid false-positive stale detection.
pub fn is_pid_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let Some(pid) = i32::try_from(pid).ok().filter(|&p| p > 0) else {
            return true; // out-of-range PID — conservatively assume alive
        };
        // SAFETY: kill(pid, 0) performs no actual signal delivery; it only
        // checks whether the process exists.
        let ret = unsafe { libc::kill(pid, 0) };
        if ret == 0 {
            return true;
        }
        // EPERM means the process exists but we lack permission to signal it.
        std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }

    #[cfg(not(unix))]
    {
        let _ = pid;
        true // conservative: assume alive
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_hostname_domain_removes_suffix() {
        assert_eq!(strip_hostname_domain("MiniBeard.local"), "MiniBeard");
        assert_eq!(strip_hostname_domain("MiniBeard.fritz.box"), "MiniBeard");
    }

    #[test]
    fn strip_hostname_domain_noop_without_dot() {
        assert_eq!(strip_hostname_domain("myhost"), "myhost");
        assert_eq!(strip_hostname_domain(""), "");
    }

    #[test]
    fn short_hostname_returns_nonempty() {
        let h = short_hostname();
        assert!(!h.is_empty() || hostname() == "unknown");
    }
}
