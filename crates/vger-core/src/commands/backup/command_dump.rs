use chrono::Utc;
use tracing::{info, warn};

use crate::chunker;
use crate::compress::Compression;
use crate::config::{ChunkerConfig, CommandDump};
use crate::crypto::chunk_id::ChunkId;
use crate::error::{Result, VgerError};
use crate::platform::shell;
use crate::repo::pack::PackType;
use crate::repo::Repository;
use crate::snapshot::item::{ChunkRef, Item, ItemType};
use crate::snapshot::SnapshotStats;

use super::{append_item_to_stream, emit_stats_progress, BackupProgressEvent};

/// Default timeout for command_dump execution (1 hour).
pub(super) const COMMAND_DUMP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3600);

/// Execute a shell command and capture its stdout.
pub(super) fn execute_dump_command(dump: &CommandDump) -> Result<Vec<u8>> {
    let output =
        shell::run_script_with_timeout(&dump.command, COMMAND_DUMP_TIMEOUT).map_err(|e| {
            VgerError::Other(format!(
                "failed to execute command_dump '{}': {}",
                dump.name, e
            ))
        })?;

    if !output.status.success() {
        let code = output
            .status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(VgerError::Other(format!(
            "command_dump '{}' failed (exit code {code}): {stderr}",
            dump.name
        )));
    }

    if output.stdout.is_empty() {
        warn!(name = %dump.name, "command_dump produced empty output");
    }

    Ok(output.stdout)
}

#[allow(clippy::too_many_arguments)]
pub(super) fn process_command_dumps(
    repo: &mut Repository,
    command_dumps: &[CommandDump],
    compression: Compression,
    items_config: &ChunkerConfig,
    item_stream: &mut Vec<u8>,
    item_ptrs: &mut Vec<ChunkId>,
    stats: &mut SnapshotStats,
    progress: &mut Option<&mut dyn FnMut(BackupProgressEvent)>,
    time_start: chrono::DateTime<Utc>,
) -> Result<()> {
    if command_dumps.is_empty() {
        return Ok(());
    }

    let dumps_dir_item = Item {
        path: ".vger-dumps".to_string(),
        entry_type: ItemType::Directory,
        mode: 0o755,
        uid: 0,
        gid: 0,
        user: None,
        group: None,
        mtime: 0,
        atime: None,
        ctime: None,
        size: 0,
        chunks: Vec::new(),
        link_target: None,
        xattrs: None,
    };
    append_item_to_stream(
        repo,
        item_stream,
        item_ptrs,
        &dumps_dir_item,
        items_config,
        compression,
    )?;

    for dump in command_dumps {
        info!(
            name = %dump.name,
            command = %dump.command,
            "executing command dump"
        );
        let data = execute_dump_command(dump)?;
        let data_len = data.len() as u64;

        let chunk_ranges = chunker::chunk_data(&data, &repo.config.chunker_params);
        let chunk_id_key = *repo.crypto.chunk_id_key();

        let mut chunk_refs = Vec::new();
        for (offset, length) in chunk_ranges {
            let chunk_data = &data[offset..offset + length];
            let chunk_id = ChunkId::compute(&chunk_id_key, chunk_data);
            let size = length as u32;

            if let Some(csize) = repo.bump_ref_if_exists(&chunk_id) {
                stats.original_size += size as u64;
                stats.compressed_size += csize as u64;
                chunk_refs.push(ChunkRef {
                    id: chunk_id,
                    size,
                    csize,
                });
            } else {
                let (chunk_id, csize, _is_new) =
                    repo.store_chunk(chunk_data, compression, PackType::Data)?;
                stats.original_size += size as u64;
                stats.compressed_size += csize as u64;
                stats.deduplicated_size += csize as u64;
                chunk_refs.push(ChunkRef {
                    id: chunk_id,
                    size,
                    csize,
                });
            }
        }

        stats.nfiles += 1;

        let dump_item = Item {
            path: format!(".vger-dumps/{}", dump.name),
            entry_type: ItemType::RegularFile,
            mode: 0o644,
            uid: 0,
            gid: 0,
            user: None,
            group: None,
            mtime: time_start.timestamp_nanos_opt().unwrap_or(0),
            atime: None,
            ctime: None,
            size: data_len,
            chunks: chunk_refs,
            link_target: None,
            xattrs: None,
        };
        append_item_to_stream(
            repo,
            item_stream,
            item_ptrs,
            &dump_item,
            items_config,
            compression,
        )?;

        emit_stats_progress(progress, stats, Some(format!(".vger-dumps/{}", dump.name)));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CommandDump;

    #[cfg(windows)]
    fn shell_echo_hello() -> &'static str {
        "Write-Output hello"
    }

    #[cfg(not(windows))]
    fn shell_echo_hello() -> &'static str {
        "echo hello"
    }

    #[cfg(windows)]
    fn shell_fail() -> &'static str {
        "exit 1"
    }

    #[cfg(not(windows))]
    fn shell_fail() -> &'static str {
        "false"
    }

    #[cfg(windows)]
    fn shell_success_no_output() -> &'static str {
        "$null = 1"
    }

    #[cfg(not(windows))]
    fn shell_success_no_output() -> &'static str {
        "true"
    }

    #[test]
    fn execute_dump_command_captures_stdout() {
        let dump = CommandDump {
            name: "test.txt".to_string(),
            command: shell_echo_hello().to_string(),
        };
        let result = execute_dump_command(&dump).unwrap();
        let text = String::from_utf8(result).unwrap();
        assert_eq!(text.trim_end(), "hello");
    }

    #[test]
    fn execute_dump_command_fails_on_nonzero_exit() {
        let dump = CommandDump {
            name: "fail.txt".to_string(),
            command: shell_fail().to_string(),
        };
        let result = execute_dump_command(&dump);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("command_dump 'fail.txt' failed"));
    }

    #[test]
    fn execute_dump_command_empty_stdout_succeeds() {
        let dump = CommandDump {
            name: "empty.txt".to_string(),
            command: shell_success_no_output().to_string(),
        };
        let result = execute_dump_command(&dump).unwrap();
        assert!(result.is_empty());
    }
}
