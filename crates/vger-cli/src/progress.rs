use std::io::Write;
use std::time::{Duration, Instant};

use vger_core::commands;

use crate::format::format_bytes;

const PROGRESS_REDRAW_INTERVAL: Duration = Duration::from_millis(100);
const DEFAULT_PROGRESS_COLUMNS: usize = 120;

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
            return;
        }
        self.render(true);
        eprintln!();
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
        let current = truncate_with_ellipsis(file, available);
        let line = format!("{prefix}{current}");
        let line_len = line.chars().count();
        let pad_len = self.last_line_len.saturating_sub(line_len);

        eprint!("\r{line}{}", " ".repeat(pad_len));
        let _ = std::io::stderr().flush();

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

fn truncate_with_ellipsis(input: &str, max_cols: usize) -> String {
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
    let tail: String = input
        .chars()
        .rev()
        .take(keep)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("...{tail}")
}

#[cfg(test)]
mod tests {
    use super::truncate_with_ellipsis;

    #[test]
    fn truncate_with_ellipsis_keeps_tail_when_needed() {
        let input = "/very/long/path/to/a/file.txt";
        let out = truncate_with_ellipsis(input, 16);
        assert_eq!(out, "...to/a/file.txt");
        assert_eq!(out.chars().count(), 16);
    }

    #[test]
    fn truncate_with_ellipsis_returns_original_when_short() {
        let input = "short.txt";
        assert_eq!(truncate_with_ellipsis(input, 32), input);
    }

    #[test]
    fn truncate_with_ellipsis_handles_tiny_widths() {
        assert_eq!(truncate_with_ellipsis("abcdef", 0), "");
        assert_eq!(truncate_with_ellipsis("abcdef", 1), ".");
        assert_eq!(truncate_with_ellipsis("abcdef", 2), "..");
        assert_eq!(truncate_with_ellipsis("abcdef", 3), "...");
    }
}
