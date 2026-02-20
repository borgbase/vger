# REST Restore Performance Findings (High-Confidence, High-Impact)

Top high-confidence, high-impact options for restore:

1. Preallocate REST range buffers to exact `length` before reading.
- Change: in `crates/vger-core/src/storage/rest_backend.rs:244`, replace `Vec::new()` + `read_to_end` with `Vec::with_capacity(length as usize)` (and ideally bounded read logic).
- Why high confidence: the biggest peak comes from `read_to_end`-driven `Vec` growth/realloc in `get_range` call path (`92.41M` inside `default_read_to_end` from `rest_backend::get_range`) in `20260219T144235Z/restore/heaptrack.analysis.txt:2399` and `20260219T144235Z/restore/heaptrack.analysis.txt:2414`.
- Expected impact: large memory drop (tens to >100MB peak churn reduction), measurable CPU reduction from fewer realloc/copies.

2. Replace restore write path `seek + write` with positional writes (`pwrite`/`write_at`).
- Change: in `crates/vger-core/src/commands/extract.rs:681` and `crates/vger-core/src/commands/extract.rs:682`, switch to `std::os::unix::fs::FileExt::write_all_at` (platform-gated), keep current path on non-Unix.
- Why high confidence: `perf stat` shows unusually high sys time (`25.44s`) and heavy context switches in restore (`20260219T144235Z/restore/perf.stat.txt:6`, `20260219T144235Z/restore/perf.stat.txt:21`). Current code does two syscalls per chunk target (`seek` then `write`).
- Expected impact: significant CPU/system-time reduction in restore workers.

3. Use one-shot zstd decode for restore chunks when expected size is known.
- Change: in `crates/vger-core/src/compress/mod.rs:131`, for `TAG_ZSTD` + `expected_size.is_some()`, prefer `zstd::bulk::decompress(payload, expected)` path; keep stream decoder fallback.
- Why high confidence: zstd decode allocates heavily (`7.47M` over `47k` calls) in restore path `decompress_with_hint -> process_read_group` (`20260219T144235Z/restore/heaptrack.analysis.txt:3394`, `20260219T144235Z/restore/heaptrack.analysis.txt:3425`).
- Expected impact: moderate-to-large CPU improvement and lower allocator pressure.

4. Reduce path-planning allocation pressure by reserving and reusing buffers in `stream_and_plan`.
- Change: in `crates/vger-core/src/commands/extract.rs:331`, add first-pass counting to `reserve` `planned_files`/maps; avoid repeated `Path::join` temp growth by reusing `PathBuf` scratch where possible.
- Why high confidence: `Path::join` path-building accounts for ~`23.23M` peak allocation across `135298` calls (`20260219T144235Z/restore/heaptrack.analysis.txt:2959`, `20260219T144235Z/restore/heaptrack.analysis.txt:2990`) and appears in temporary allocation leaders (`20260219T144235Z/restore/heaptrack.analysis.txt:4251`).
- Expected impact: notable memory reduction and some CPU improvement in planning phase.

5. Increase per-worker open-file cache (or make it adaptive) to cut reopen churn.
- Change: tune `MAX_OPEN_FILES_PER_GROUP` from `16` in `crates/vger-core/src/commands/extract.rs:43`, and replace random eviction with deterministic LRU.
- Why high confidence: restore writes fan out across many targets; current random eviction can force extra open/close cycles during hot write loops.
- Expected impact: moderate CPU/syscall reduction, especially on large snapshots.
