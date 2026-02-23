use std::io::{self, IsTerminal, Stderr, Write};
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tracing_subscriber::fmt::MakeWriter;
use vger_core::commands;

use crate::format::format_bytes;

const PROGRESS_REDRAW_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_PROGRESS_COLUMNS: usize = 120;

// ---------------------------------------------------------------------------
// Shared state between the progress renderer and the tracing writer
// ---------------------------------------------------------------------------

/// True while a backup progress line is being displayed on stderr.
static PROGRESS_ACTIVE: AtomicBool = AtomicBool::new(false);

/// Serializes all stderr writes between the progress renderer and tracing.
static STDERR_LOCK: Mutex<()> = Mutex::new(());

fn acquire_stderr_lock() -> MutexGuard<'static, ()> {
    STDERR_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// ---------------------------------------------------------------------------
// Progress-aware tracing writer
// ---------------------------------------------------------------------------

/// A [`MakeWriter`] that clears the progress line before each tracing event,
/// preventing log messages from corrupting the `\r`-based progress display.
pub(crate) struct ProgressAwareStderr;

/// Holds the `STDERR_LOCK` guard for the entire lifetime of a single tracing
/// write, so the lock spans from the line-clear through the full log message.
pub(crate) struct ProgressWriter {
    _guard: MutexGuard<'static, ()>,
    inner: Stderr,
}

impl Write for ProgressWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<'a> MakeWriter<'a> for ProgressAwareStderr {
    type Writer = ProgressWriter;

    fn make_writer(&'a self) -> Self::Writer {
        let guard = acquire_stderr_lock();
        let mut stderr = io::stderr();

        if PROGRESS_ACTIVE.load(Relaxed) && stderr.is_terminal() {
            // Clear the current progress line so the log message starts clean.
            let _ = stderr.write_all(b"\r\x1b[2K");
        }

        ProgressWriter {
            _guard: guard,
            inner: stderr,
        }
    }
}

// ---------------------------------------------------------------------------
// Backup progress renderer
// ---------------------------------------------------------------------------

pub(crate) struct BackupProgressRenderer {
    current_file: Option<String>,
    nfiles: u64,
    original_size: u64,
    compressed_size: u64,
    deduplicated_size: u64,
    last_draw: Instant,
    last_line_len: usize,
    rendered_any: bool,
}

impl BackupProgressRenderer {
    pub(crate) fn new() -> Self {
        PROGRESS_ACTIVE.store(true, Relaxed);
        Self {
            current_file: None,
            nfiles: 0,
            original_size: 0,
            compressed_size: 0,
            deduplicated_size: 0,
            last_draw: Instant::now(),
            last_line_len: 0,
            rendered_any: false,
        }
    }

    pub(crate) fn on_event(&mut self, event: commands::backup::BackupProgressEvent) {
        let should_render = match event {
            commands::backup::BackupProgressEvent::FileStarted { path } => {
                self.current_file = Some(path);
                true
            }
            commands::backup::BackupProgressEvent::StatsUpdated {
                nfiles,
                original_size,
                compressed_size,
                deduplicated_size,
                current_file,
            } => {
                self.nfiles = nfiles;
                self.original_size = original_size;
                self.compressed_size = compressed_size;
                self.deduplicated_size = deduplicated_size;
                if let Some(path) = current_file {
                    self.current_file = Some(path);
                }
                true
            }
            commands::backup::BackupProgressEvent::SourceStarted { .. }
            | commands::backup::BackupProgressEvent::SourceFinished { .. } => false,
        };

        if should_render {
            self.render(false);
        }
    }

    pub(crate) fn finish(&mut self) {
        if !self.rendered_any {
            PROGRESS_ACTIVE.store(false, Relaxed);
            return;
        }
        self.render(true);
        // Final newline under the lock so it doesn't race with tracing.
        {
            let _guard = acquire_stderr_lock();
            eprintln!();
        }
        PROGRESS_ACTIVE.store(false, Relaxed);
        self.rendered_any = false;
        self.last_line_len = 0;
    }

    fn render(&mut self, force: bool) {
        if !force && self.rendered_any && self.last_draw.elapsed() < PROGRESS_REDRAW_INTERVAL {
            return;
        }
        self.last_draw = Instant::now();

        let file = self.current_file.as_deref().unwrap_or("-");
        let prefix = format!(
            "Files: {}, Original: {}, Compressed: {}, Deduplicated: {}, Current: ",
            self.nfiles,
            format_bytes(self.original_size),
            format_bytes(self.compressed_size),
            format_bytes(self.deduplicated_size),
        );

        let columns = terminal_columns();
        let available = columns.saturating_sub(prefix.chars().count());
        let current = truncate_middle(file, available);
        let line = format!("{prefix}{current}");
        let line_len = line.chars().count();
        let pad_len = self.last_line_len.saturating_sub(line_len);

        {
            let _guard = acquire_stderr_lock();
            eprint!("\r{line}{}", " ".repeat(pad_len));
            let _ = io::stderr().flush();
        }

        self.last_line_len = line_len;
        self.rendered_any = true;
    }
}

fn terminal_columns() -> usize {
    std::env::var("COLUMNS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_PROGRESS_COLUMNS)
}

// ---------------------------------------------------------------------------
// Middle-truncation for file paths
// ---------------------------------------------------------------------------

/// Truncate a string to `max_cols` characters, showing both the beginning and
/// end with `...` in the middle (e.g. `/very/l...file.txt`).
fn truncate_middle(input: &str, max_cols: usize) -> String {
    if max_cols == 0 {
        return String::new();
    }

    let input_len = input.chars().count();
    if input_len <= max_cols {
        return input.to_string();
    }

    if max_cols <= 3 {
        return ".".repeat(max_cols);
    }

    let keep = max_cols - 3;
    let head = keep / 2;
    let tail = keep - head;
    let head_str: String = input.chars().take(head).collect();
    let tail_str: String = input.chars().skip(input_len - tail).collect();
    format!("{head_str}...{tail_str}")
}

#[cfg(test)]
mod tests {
    use super::truncate_middle;

    #[test]
    fn truncate_middle_shows_head_and_tail() {
        let input = "/very/long/path/to/a/file.txt";
        let out = truncate_middle(input, 16);
        // keep = 13, head = 6, tail = 7
        assert_eq!(out, "/very/...ile.txt");
        assert_eq!(out.chars().count(), 16);
    }

    #[test]
    fn truncate_middle_returns_original_when_short() {
        let input = "short.txt";
        assert_eq!(truncate_middle(input, 32), input);
    }

    #[test]
    fn truncate_middle_handles_tiny_widths() {
        assert_eq!(truncate_middle("abcdef", 0), "");
        assert_eq!(truncate_middle("abcdef", 1), ".");
        assert_eq!(truncate_middle("abcdef", 2), "..");
        assert_eq!(truncate_middle("abcdef", 3), "...");
    }

    #[test]
    fn truncate_middle_exact_fit() {
        let input = "exactly10!";
        assert_eq!(truncate_middle(input, 10), input);
    }

    #[test]
    fn truncate_middle_one_over() {
        // 11 chars, max 10 → keep=7, head=3, tail=4
        let input = "abcdefghijk";
        let out = truncate_middle(input, 10);
        assert_eq!(out, "abc...hijk");
        assert_eq!(out.chars().count(), 10);
    }

    #[test]
    fn truncate_middle_unicode() {
        let input = "aaaa\u{00e9}\u{00e9}\u{00e9}\u{00e9}bbbb"; // 12 chars
        let out = truncate_middle(input, 10);
        // keep=7, head=3, tail=4
        assert_eq!(out, "aaa...bbbb");
        assert_eq!(out.chars().count(), 10);
    }
}
