use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{DateTime, Local};
use crossbeam_channel::{Receiver, Sender};
use notify::{Config as NotifyConfig, RecommendedWatcher, RecursiveMode, Watcher};
use tray_icon::menu::{Menu, MenuEvent, MenuId, MenuItem};
use tray_icon::{Icon, TrayIconBuilder};
use vger_core::app::{self, operations, passphrase};
use vger_core::config::{self, ResolvedRepo, ScheduleConfig};
use vger_core::error::VgerError;

slint::slint! {
    import { VerticalBox, HorizontalBox, Button, LineEdit, ScrollView, TabWidget, ComboBox } from "std-widgets.slint";

    export component MainWindow inherits Window {
        in-out property <string> config_path;
        in-out property <string> schedule_text;
        in-out property <string> status_text;
        in-out property <string> snapshot_id;
        in-out property <string> extract_dest;
        in-out property <string> extract_pattern;
        in-out property <string> snapshots_text;
        in-out property <string> snapshot_items_text;
        in-out property <string> log_text;
        in-out property <string> repo_info_text;
        in-out property <string> sources_text;
        in-out property <[string]> repo_names;
        in-out property <[string]> source_names;
        in-out property <string> repos_combo_value;
        in-out property <string> sources_combo_value;
        in-out property <string> snapshots_repo_combo_value;

        callback backup_all_clicked();
        callback refresh_snapshots_clicked();
        callback show_snapshot_items_clicked();
        callback extract_clicked();
        callback reload_config_clicked();
        callback pause_schedule_clicked();
        callback quit_clicked();
        callback fetch_repo_info_clicked();
        callback backup_repo_clicked();
        callback backup_source_clicked();

        title: "V'Ger";
        width: 1100px;
        height: 760px;

        VerticalBox {
            padding: 12px;
            spacing: 8px;

            // ── Header ──

            Text {
                text: "V'Ger Desktop";
                font-size: 26px;
            }

            HorizontalBox {
                spacing: 10px;
                Text { text: "Config:"; }
                Text { text: root.config_path; wrap: word-wrap; }
            }

            HorizontalBox {
                spacing: 10px;
                Text { text: "Schedule:"; }
                Text { text: root.schedule_text; wrap: word-wrap; }
            }

            HorizontalBox {
                spacing: 10px;
                Text { text: "Status:"; }
                Text { text: root.status_text; wrap: word-wrap; }
            }

            Rectangle {
                height: 1px;
                background: #d5d5d5;
            }

            HorizontalBox {
                spacing: 8px;
                Button {
                    text: "Backup All";
                    clicked => { root.backup_all_clicked(); }
                }
                Button {
                    text: "Reload Config";
                    clicked => { root.reload_config_clicked(); }
                }
                Button {
                    text: "Pause/Resume Schedule";
                    clicked => { root.pause_schedule_clicked(); }
                }
                Button {
                    text: "Quit";
                    clicked => { root.quit_clicked(); }
                }
            }

            Rectangle {
                height: 1px;
                background: #d5d5d5;
            }

            // ── Tabs ──

            TabWidget {
                Tab {
                    title: "Repositories";
                    VerticalBox {
                        spacing: 8px;
                        padding: 8px;

                        HorizontalBox {
                            spacing: 8px;
                            Text { text: "Repository:"; vertical-alignment: center; }
                            ComboBox {
                                model: root.repo_names;
                                current-value <=> root.repos_combo_value;
                            }
                            Button {
                                text: "Fetch Info";
                                clicked => { root.fetch_repo_info_clicked(); }
                            }
                            Button {
                                text: "Backup This Repo";
                                clicked => { root.backup_repo_clicked(); }
                            }
                        }

                        Text {
                            text: "Repository Info";
                            font-size: 16px;
                        }
                        ScrollView {
                            vertical-stretch: 1;
                            Rectangle {
                                background: #f4f4f4;
                                border-color: #dcdcdc;
                                border-width: 1px;
                                Text {
                                    text: root.repo_info_text;
                                    wrap: word-wrap;
                                }
                            }
                        }
                    }
                }

                Tab {
                    title: "Sources";
                    VerticalBox {
                        spacing: 8px;
                        padding: 8px;

                        HorizontalBox {
                            spacing: 8px;
                            Text { text: "Source:"; vertical-alignment: center; }
                            ComboBox {
                                model: root.source_names;
                                current-value <=> root.sources_combo_value;
                            }
                            Button {
                                text: "Backup Source";
                                clicked => { root.backup_source_clicked(); }
                            }
                        }

                        Text {
                            text: "Configured Sources";
                            font-size: 16px;
                        }
                        ScrollView {
                            vertical-stretch: 1;
                            Rectangle {
                                background: #f4f4f4;
                                border-color: #dcdcdc;
                                border-width: 1px;
                                Text {
                                    text: root.sources_text;
                                    wrap: word-wrap;
                                }
                            }
                        }
                    }
                }

                Tab {
                    title: "Snapshots";
                    VerticalBox {
                        spacing: 8px;
                        padding: 8px;

                        HorizontalBox {
                            spacing: 8px;
                            Text { text: "Repository:"; vertical-alignment: center; }
                            ComboBox {
                                model: root.repo_names;
                                current-value <=> root.snapshots_repo_combo_value;
                            }
                            Button {
                                text: "Refresh Snapshots";
                                clicked => { root.refresh_snapshots_clicked(); }
                            }
                        }

                        HorizontalBox {
                            spacing: 8px;
                            Text { text: "Snapshot ID:"; }
                            LineEdit { text <=> root.snapshot_id; }
                            Button {
                                text: "Show Contents";
                                clicked => { root.show_snapshot_items_clicked(); }
                            }
                        }

                        HorizontalBox {
                            spacing: 8px;
                            Text { text: "Extract Destination:"; }
                            LineEdit { text <=> root.extract_dest; }
                        }

                        HorizontalBox {
                            spacing: 8px;
                            Text { text: "Extract Pattern (optional):"; }
                            LineEdit { text <=> root.extract_pattern; }
                            Button {
                                text: "Extract";
                                clicked => { root.extract_clicked(); }
                            }
                        }

                        HorizontalBox {
                            spacing: 12px;

                            VerticalBox {
                                spacing: 6px;
                                Text {
                                    text: "Snapshots";
                                    font-size: 16px;
                                }
                                ScrollView {
                                    height: 180px;
                                    Rectangle {
                                        background: #f4f4f4;
                                        border-color: #dcdcdc;
                                        border-width: 1px;
                                        Text {
                                            text: root.snapshots_text;
                                            wrap: word-wrap;
                                        }
                                    }
                                }
                            }

                            VerticalBox {
                                spacing: 6px;
                                Text {
                                    text: "Snapshot Contents";
                                    font-size: 16px;
                                }
                                ScrollView {
                                    height: 180px;
                                    Rectangle {
                                        background: #f4f4f4;
                                        border-color: #dcdcdc;
                                        border-width: 1px;
                                        Text {
                                            text: root.snapshot_items_text;
                                            wrap: word-wrap;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // ── Footer ──

            Text {
                text: "Activity Log";
                font-size: 16px;
            }
            ScrollView {
                vertical-stretch: 1;
                Rectangle {
                    background: #f4f4f4;
                    border-color: #dcdcdc;
                    border-width: 1px;
                    Text {
                        text: root.log_text;
                        wrap: word-wrap;
                    }
                }
            }
        }
    }
}

#[derive(Debug)]
enum AppCommand {
    RunBackupAll {
        scheduled: bool,
    },
    RunBackupRepo {
        repo_selector: String,
    },
    RunBackupSource {
        source_selector: String,
    },
    FetchRepoInfo {
        repo_selector: String,
    },
    RefreshSnapshots {
        repo_selector: String,
    },
    ShowSnapshotItems {
        repo_selector: String,
        snapshot: String,
    },
    Extract {
        repo_selector: String,
        snapshot: String,
        dest: String,
        pattern: Option<String>,
    },
    ReloadConfig,
    ToggleSchedulePause,
    ShowWindow,
    Quit,
}

#[derive(Debug, Clone)]
enum UiEvent {
    Status(String),
    Log(String),
    ConfigInfo { path: String, schedule: String },
    SnapshotsText(String),
    SnapshotItemsText(String),
    RepoInfoText(String),
    SourcesText(String),
    RepoNames(Vec<String>),
    SourceNames(Vec<String>),
    Quit,
    ShowWindow,
}

#[derive(Debug)]
struct SchedulerState {
    enabled: bool,
    paused: bool,
    every: Duration,
    jitter_seconds: u64,
    next_run: Option<Instant>,
}

impl Default for SchedulerState {
    fn default() -> Self {
        Self {
            enabled: false,
            paused: false,
            every: Duration::from_secs(24 * 60 * 60),
            jitter_seconds: 0,
            next_run: None,
        }
    }
}

fn schedule_description(schedule: &ScheduleConfig, paused: bool) -> String {
    format!(
        "enabled={}, every={}, on_startup={}, jitter_seconds={}, paused={}",
        schedule.enabled, schedule.every, schedule.on_startup, schedule.jitter_seconds, paused,
    )
}

fn build_tray_icon() -> Result<(tray_icon::TrayIcon, MenuId, MenuId, MenuId, MenuId), String> {
    let menu = Menu::new();

    let open_item = MenuItem::new("Open V'Ger", true, None);
    let run_now_item = MenuItem::new("Run Backup Now", true, None);
    let pause_item = MenuItem::new("Pause/Resume Scheduled Backups", true, None);
    let quit_item = MenuItem::new("Quit", true, None);

    menu.append(&open_item)
        .map_err(|e| format!("tray menu append failed: {e}"))?;
    menu.append(&run_now_item)
        .map_err(|e| format!("tray menu append failed: {e}"))?;
    menu.append(&pause_item)
        .map_err(|e| format!("tray menu append failed: {e}"))?;
    menu.append(&quit_item)
        .map_err(|e| format!("tray menu append failed: {e}"))?;

    let mut rgba = Vec::with_capacity(16 * 16 * 4);
    for _ in 0..(16 * 16) {
        rgba.extend_from_slice(&[0x10, 0x8e, 0xf1, 0xff]);
    }
    let icon = Icon::from_rgba(rgba, 16, 16).map_err(|e| format!("tray icon error: {e}"))?;

    let tray = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_tooltip("V'Ger")
        .with_icon(icon)
        .build()
        .map_err(|e| format!("tray icon build failed: {e}"))?;

    Ok((
        tray,
        open_item.id().clone(),
        run_now_item.id().clone(),
        pause_item.id().clone(),
        quit_item.id().clone(),
    ))
}

fn spawn_config_watcher(path: PathBuf, app_tx: Sender<AppCommand>) {
    thread::spawn(move || {
        let (notify_tx, notify_rx) = std::sync::mpsc::channel();
        let mut watcher = match RecommendedWatcher::new(
            move |res| {
                let _ = notify_tx.send(res);
            },
            NotifyConfig::default(),
        ) {
            Ok(w) => w,
            Err(_) => return,
        };

        if watcher.watch(&path, RecursiveMode::NonRecursive).is_err() {
            return;
        }

        let mut last_sent = Instant::now() - Duration::from_secs(10);

        while let Ok(event) = notify_rx.recv() {
            if event.is_ok() && last_sent.elapsed() >= Duration::from_millis(500) {
                if app_tx.send(AppCommand::ReloadConfig).is_err() {
                    break;
                }
                last_sent = Instant::now();
            }
        }
    });
}

fn spawn_scheduler(
    app_tx: Sender<AppCommand>,
    scheduler: Arc<Mutex<SchedulerState>>,
    backup_running: Arc<AtomicBool>,
) {
    thread::spawn(move || loop {
        thread::sleep(Duration::from_secs(1));

        let mut should_run = false;

        {
            let mut state = match scheduler.lock() {
                Ok(s) => s,
                Err(_) => break,
            };

            if !state.enabled || state.paused {
                continue;
            }

            if state.next_run.is_none() {
                let jitter = vger_core::app::scheduler::random_jitter(state.jitter_seconds);
                state.next_run = Some(Instant::now() + state.every + jitter);
            }

            if let Some(next) = state.next_run {
                if Instant::now() >= next && !backup_running.load(Ordering::SeqCst) {
                    should_run = true;
                    let jitter = vger_core::app::scheduler::random_jitter(state.jitter_seconds);
                    state.next_run = Some(Instant::now() + state.every + jitter);
                }
            }
        }

        if should_run
            && app_tx
                .send(AppCommand::RunBackupAll { scheduled: true })
                .is_err()
        {
            break;
        }
    });
}

fn format_repo_name(repo: &ResolvedRepo) -> String {
    repo.label
        .clone()
        .unwrap_or_else(|| repo.config.repository.url.clone())
}

fn format_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = KB * 1024.0;
    const GB: f64 = MB * 1024.0;

    let b = bytes as f64;
    if b >= GB {
        format!("{:.2} GiB", b / GB)
    } else if b >= MB {
        format!("{:.2} MiB", b / MB)
    } else if b >= KB {
        format!("{:.2} KiB", b / KB)
    } else {
        format!("{bytes} B")
    }
}

fn format_info_stats(repo_name: &str, stats: &vger_core::commands::info::InfoStats) -> String {
    let encryption = format!("{:?}", stats.encryption);
    let last_snapshot = stats
        .last_snapshot_time
        .map(|t| {
            let local: DateTime<Local> = t.with_timezone(&Local);
            local.format("%Y-%m-%d %H:%M:%S").to_string()
        })
        .unwrap_or_else(|| "N/A".to_string());
    let created: DateTime<Local> = stats.repo_created.with_timezone(&Local);

    format!(
        "Repository: {repo_name}\n\
         Created: {}\n\
         Encryption: {encryption}\n\
         Snapshots: {}\n\
         Last snapshot: {last_snapshot}\n\
         \n\
         Raw size: {}\n\
         Compressed size: {}\n\
         Deduplicated size: {}\n\
         \n\
         Unique chunks: {}\n\
         Unique stored size: {}\n\
         Referenced stored size: {}",
        created.format("%Y-%m-%d %H:%M:%S"),
        stats.snapshot_count,
        format_bytes(stats.raw_size),
        format_bytes(stats.compressed_size),
        format_bytes(stats.deduplicated_size),
        stats.unique_chunks,
        format_bytes(stats.unique_stored_size),
        format_bytes(stats.referenced_stored_size),
    )
}

fn format_sources_text(repos: &[ResolvedRepo]) -> String {
    let mut seen_labels = std::collections::HashSet::new();
    let mut lines = Vec::new();

    for repo in repos {
        for source in &repo.sources {
            if !seen_labels.insert(&source.label) {
                continue;
            }
            lines.push(format!("Source: {}", source.label));
            lines.push(format!("  Paths: {}", source.paths.join(", ")));
            if !source.exclude.is_empty() {
                lines.push(format!("  Exclude: {}", source.exclude.join(", ")));
            }
            if !source.repos.is_empty() {
                lines.push(format!("  Target repos: {}", source.repos.join(", ")));
            } else {
                lines.push("  Target repos: (all)".to_string());
            }
            lines.push(String::new());
        }
    }

    if lines.is_empty() {
        "No sources configured.".to_string()
    } else {
        lines.join("\n")
    }
}

fn collect_repo_names(repos: &[ResolvedRepo]) -> Vec<String> {
    repos.iter().map(|r| format_repo_name(r)).collect()
}

fn collect_source_names(repos: &[ResolvedRepo]) -> Vec<String> {
    let mut seen = std::collections::HashSet::new();
    let mut names = Vec::new();
    for repo in repos {
        for source in &repo.sources {
            if seen.insert(source.label.clone()) {
                names.push(source.label.clone());
            }
        }
    }
    names
}

fn resolve_passphrase_for_repo(repo: &ResolvedRepo) -> Result<Option<String>, VgerError> {
    let repo_name = format_repo_name(repo);
    passphrase::resolve_passphrase(&repo.config, repo.label.as_deref(), |prompt| {
        let title = format!("V'Ger Passphrase ({repo_name})");
        let message = format!(
            "Enter passphrase for {}\nRepository: {}",
            prompt
                .repository_label
                .as_deref()
                .unwrap_or(prompt.repository_url.as_str()),
            prompt.repository_url,
        );
        let value = tinyfiledialogs::password_box(&title, &message);
        Ok(value.filter(|v| !v.is_empty()))
    })
}

fn get_or_resolve_passphrase(
    repo: &ResolvedRepo,
    cache: &mut HashMap<String, String>,
) -> Result<Option<String>, VgerError> {
    let key = &repo.config.repository.url;
    if let Some(existing) = cache.get(key) {
        return Ok(Some(existing.clone()));
    }
    let pass = resolve_passphrase_for_repo(repo)?;
    if let Some(ref p) = pass {
        cache.insert(key.clone(), p.clone());
    }
    Ok(pass)
}

fn snapshot_rows_for_repo(
    repo: &ResolvedRepo,
    passphrase: Option<&str>,
) -> Result<Vec<String>, VgerError> {
    let mut snapshots = operations::list_snapshots(&repo.config, passphrase)?;
    snapshots.sort_by_key(|s| s.time);

    let repo_name = format_repo_name(repo);
    Ok(snapshots
        .into_iter()
        .map(|s| {
            let ts: DateTime<Local> = s.time.with_timezone(&Local);
            let label = if s.label.is_empty() {
                "-"
            } else {
                s.label.as_str()
            };
            let source = if s.source_label.is_empty() {
                "-"
            } else {
                s.source_label.as_str()
            };
            format!(
                "[{repo_name}] {}  source={} label={} time={}",
                s.name,
                source,
                label,
                ts.format("%Y-%m-%d %H:%M:%S")
            )
        })
        .collect())
}

fn select_repos<'a>(
    repos: &'a [ResolvedRepo],
    selector: &str,
) -> Result<Vec<&'a ResolvedRepo>, VgerError> {
    let selector = selector.trim();
    if selector.is_empty() {
        return Ok(repos.iter().collect());
    }

    let repo = config::select_repo(repos, selector)
        .ok_or_else(|| VgerError::Config(format!("no repository matching '{selector}'")))?;
    Ok(vec![repo])
}

fn find_repo_for_snapshot<'a>(
    repos: &'a [ResolvedRepo],
    selector: &str,
    snapshot: &str,
    passphrases: &mut HashMap<String, String>,
) -> Result<(&'a ResolvedRepo, Option<String>), VgerError> {
    for repo in select_repos(repos, selector)? {
        let key = repo.config.repository.url.clone();
        let pass = if let Some(cached) = passphrases.get(&key) {
            Some(cached.clone())
        } else {
            let p = resolve_passphrase_for_repo(repo)?;
            if let Some(ref v) = p {
                passphrases.insert(key.clone(), v.clone());
            }
            p
        };

        match operations::list_snapshot_items(&repo.config, pass.as_deref(), snapshot) {
            Ok(_) => return Ok((repo, pass)),
            Err(VgerError::SnapshotNotFound(_)) => continue,
            Err(e) => return Err(e),
        }
    }

    Err(VgerError::SnapshotNotFound(snapshot.to_string()))
}

fn send_log(ui_tx: &Sender<UiEvent>, message: impl Into<String>) {
    let _ = ui_tx.send(UiEvent::Log(message.into()));
}

fn send_combo_data(ui_tx: &Sender<UiEvent>, repos: &[ResolvedRepo]) {
    let _ = ui_tx.send(UiEvent::RepoNames(collect_repo_names(repos)));
    let _ = ui_tx.send(UiEvent::SourceNames(collect_source_names(repos)));
    let _ = ui_tx.send(UiEvent::SourcesText(format_sources_text(repos)));
}

fn log_backup_report(
    ui_tx: &Sender<UiEvent>,
    repo_name: &str,
    report: &operations::BackupRunReport,
) {
    if report.created.is_empty() {
        send_log(ui_tx, format!("[{repo_name}] no snapshots created"));
        return;
    }
    for created in &report.created {
        send_log(
            ui_tx,
            format!(
                "[{repo_name}] snapshot {} source={} files={} original={} compressed={} deduplicated={}",
                created.snapshot_name,
                created.source_label,
                created.stats.nfiles,
                format_bytes(created.stats.original_size),
                format_bytes(created.stats.compressed_size),
                format_bytes(created.stats.deduplicated_size),
            ),
        );
    }
}

fn run_worker(
    app_tx: Sender<AppCommand>,
    cmd_rx: Receiver<AppCommand>,
    ui_tx: Sender<UiEvent>,
    scheduler: Arc<Mutex<SchedulerState>>,
    backup_running: Arc<AtomicBool>,
    mut runtime: app::RuntimeConfig,
) {
    let mut passphrases: HashMap<String, String> = HashMap::new();

    let schedule = runtime.schedule();
    let mut schedule_paused = false;
    let schedule_interval = vger_core::app::scheduler::schedule_interval(&schedule)
        .unwrap_or_else(|_| Duration::from_secs(24 * 60 * 60));

    if let Ok(mut state) = scheduler.lock() {
        state.enabled = schedule.enabled;
        state.paused = false;
        state.every = schedule_interval;
        state.jitter_seconds = schedule.jitter_seconds;
        state.next_run = Some(Instant::now() + schedule_interval);
    }

    let _ = ui_tx.send(UiEvent::ConfigInfo {
        path: runtime.source.path().display().to_string(),
        schedule: schedule_description(&schedule, schedule_paused),
    });

    send_combo_data(&ui_tx, &runtime.repos);

    if schedule.enabled && schedule.on_startup {
        let _ = ui_tx.send(UiEvent::Log(
            "Scheduled on-startup backup requested by configuration.".to_string(),
        ));
        let _ = app_tx.send(AppCommand::RunBackupAll { scheduled: true });
    }

    while let Ok(cmd) = cmd_rx.recv() {
        match cmd {
            AppCommand::RunBackupAll { scheduled } => {
                backup_running.store(true, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::Status(if scheduled {
                    "Running scheduled backup...".to_string()
                } else {
                    "Running backup...".to_string()
                }));

                for repo in &runtime.repos {
                    let repo_name = format_repo_name(repo);

                    let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                        Ok(pass) => pass,
                        Err(e) => {
                            send_log(
                                &ui_tx,
                                format!("[{repo_name}] failed to resolve passphrase: {e}"),
                            );
                            continue;
                        }
                    };

                    if repo.config.encryption.mode != vger_core::config::EncryptionModeConfig::None
                        && passphrase.is_none()
                    {
                        send_log(
                            &ui_tx,
                            format!(
                                "[{repo_name}] passphrase prompt canceled; skipping this repository"
                            ),
                        );
                        continue;
                    }

                    match operations::run_backup_for_repo(
                        &repo.config,
                        &repo.sources,
                        passphrase.as_deref(),
                        None,
                    ) {
                        Ok(report) => log_backup_report(&ui_tx, &repo_name, &report),
                        Err(e) => {
                            send_log(&ui_tx, format!("[{repo_name}] backup failed: {e}"));
                        }
                    }
                }

                backup_running.store(false, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::RunBackupRepo { repo_selector } => {
                let repo_selector = repo_selector.trim().to_string();
                if repo_selector.is_empty() {
                    send_log(&ui_tx, "Select a repository first.");
                    continue;
                }

                let repo = match config::select_repo(&runtime.repos, &repo_selector) {
                    Some(r) => r,
                    None => {
                        send_log(&ui_tx, format!("No repository matching '{repo_selector}'."));
                        continue;
                    }
                };

                backup_running.store(true, Ordering::SeqCst);
                let repo_name = format_repo_name(repo);
                let _ = ui_tx.send(UiEvent::Status(format!(
                    "Running backup for [{repo_name}]..."
                )));

                let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                    Ok(p) => p,
                    Err(e) => {
                        send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                        backup_running.store(false, Ordering::SeqCst);
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                if repo.config.encryption.mode != vger_core::config::EncryptionModeConfig::None
                    && passphrase.is_none()
                {
                    send_log(
                        &ui_tx,
                        format!("[{repo_name}] passphrase prompt canceled; skipping."),
                    );
                    backup_running.store(false, Ordering::SeqCst);
                    let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                    continue;
                }

                match operations::run_backup_for_repo(
                    &repo.config,
                    &repo.sources,
                    passphrase.as_deref(),
                    None,
                ) {
                    Ok(report) => log_backup_report(&ui_tx, &repo_name, &report),
                    Err(e) => send_log(&ui_tx, format!("[{repo_name}] backup failed: {e}")),
                }

                backup_running.store(false, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::RunBackupSource { source_selector } => {
                let source_selector = source_selector.trim().to_string();
                if source_selector.is_empty() {
                    send_log(&ui_tx, "Select a source first.");
                    continue;
                }

                backup_running.store(true, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::Status(format!(
                    "Running backup for source '{source_selector}'..."
                )));

                let mut any_backed_up = false;
                for repo in &runtime.repos {
                    let matching_sources: Vec<config::SourceEntry> = repo
                        .sources
                        .iter()
                        .filter(|s| s.label == source_selector)
                        .cloned()
                        .collect();

                    if matching_sources.is_empty() {
                        continue;
                    }

                    let repo_name = format_repo_name(repo);
                    let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                        Ok(p) => p,
                        Err(e) => {
                            send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                            continue;
                        }
                    };

                    if repo.config.encryption.mode != vger_core::config::EncryptionModeConfig::None
                        && passphrase.is_none()
                    {
                        send_log(
                            &ui_tx,
                            format!("[{repo_name}] passphrase prompt canceled; skipping."),
                        );
                        continue;
                    }

                    match operations::run_backup_for_repo(
                        &repo.config,
                        &matching_sources,
                        passphrase.as_deref(),
                        None,
                    ) {
                        Ok(report) => {
                            any_backed_up = true;
                            log_backup_report(&ui_tx, &repo_name, &report);
                        }
                        Err(e) => {
                            send_log(&ui_tx, format!("[{repo_name}] backup failed: {e}"));
                        }
                    }
                }

                if !any_backed_up {
                    send_log(
                        &ui_tx,
                        format!("No repositories found with source '{source_selector}'."),
                    );
                }

                backup_running.store(false, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::FetchRepoInfo { repo_selector } => {
                let repo_selector = repo_selector.trim().to_string();
                if repo_selector.is_empty() {
                    send_log(&ui_tx, "Select a repository first.");
                    continue;
                }

                let repo = match config::select_repo(&runtime.repos, &repo_selector) {
                    Some(r) => r,
                    None => {
                        send_log(&ui_tx, format!("No repository matching '{repo_selector}'."));
                        continue;
                    }
                };

                let repo_name = format_repo_name(repo);
                let _ = ui_tx.send(UiEvent::Status(format!(
                    "Fetching info for [{repo_name}]..."
                )));

                let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                    Ok(p) => p,
                    Err(e) => {
                        send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                match vger_core::commands::info::run(&repo.config, passphrase.as_deref()) {
                    Ok(stats) => {
                        let text = format_info_stats(&repo_name, &stats);
                        let _ = ui_tx.send(UiEvent::RepoInfoText(text));
                        send_log(&ui_tx, format!("Fetched info for [{repo_name}]."));
                    }
                    Err(e) => {
                        send_log(&ui_tx, format!("[{repo_name}] info failed: {e}"));
                    }
                }

                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::RefreshSnapshots { repo_selector } => {
                let _ = ui_tx.send(UiEvent::Status("Loading snapshots...".to_string()));
                let mut rows = Vec::new();

                let repos_to_scan = match select_repos(&runtime.repos, &repo_selector) {
                    Ok(repos) => repos,
                    Err(e) => {
                        send_log(&ui_tx, format!("Failed to select repository: {e}"));
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                for repo in repos_to_scan {
                    let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                        Ok(pass) => pass,
                        Err(e) => {
                            send_log(
                                &ui_tx,
                                format!("[{}] passphrase error: {e}", format_repo_name(repo)),
                            );
                            continue;
                        }
                    };

                    match snapshot_rows_for_repo(repo, passphrase.as_deref()) {
                        Ok(mut repo_rows) => rows.append(&mut repo_rows),
                        Err(e) => {
                            send_log(
                                &ui_tx,
                                format!(
                                    "[{}] snapshot listing failed: {e}",
                                    format_repo_name(repo)
                                ),
                            );
                        }
                    }
                }

                if rows.is_empty() {
                    rows.push("No snapshots found.".to_string());
                }

                let _ = ui_tx.send(UiEvent::SnapshotsText(rows.join("\n")));
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::ShowSnapshotItems {
                repo_selector,
                snapshot,
            } => {
                let snapshot = snapshot.trim().to_string();
                if snapshot.is_empty() {
                    send_log(&ui_tx, "Snapshot ID is required to list contents.");
                    continue;
                }

                let _ = ui_tx.send(UiEvent::Status("Loading snapshot contents...".to_string()));

                match find_repo_for_snapshot(
                    &runtime.repos,
                    &repo_selector,
                    &snapshot,
                    &mut passphrases,
                ) {
                    Ok((repo, passphrase)) => match operations::list_snapshot_items(
                        &repo.config,
                        passphrase.as_deref(),
                        &snapshot,
                    ) {
                        Ok(items) => {
                            let mut lines = Vec::new();
                            for item in items {
                                let type_char = match item.entry_type {
                                    vger_core::snapshot::item::ItemType::Directory => 'd',
                                    vger_core::snapshot::item::ItemType::RegularFile => '-',
                                    vger_core::snapshot::item::ItemType::Symlink => 'l',
                                };
                                lines.push(format!(
                                    "{}{:o} {:>10} {}",
                                    type_char,
                                    item.mode & 0o7777,
                                    item.size,
                                    item.path
                                ));
                            }

                            if lines.is_empty() {
                                lines.push("Snapshot has no entries.".to_string());
                            }

                            let _ = ui_tx.send(UiEvent::SnapshotItemsText(lines.join("\n")));
                            send_log(
                                &ui_tx,
                                format!(
                                    "Loaded {} item(s) from snapshot {} in [{}]",
                                    lines.len(),
                                    snapshot,
                                    format_repo_name(repo)
                                ),
                            );
                        }
                        Err(e) => send_log(&ui_tx, format!("Failed to load snapshot items: {e}")),
                    },
                    Err(e) => send_log(&ui_tx, format!("Failed to resolve snapshot: {e}")),
                }

                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::Extract {
                repo_selector,
                snapshot,
                dest,
                pattern,
            } => {
                if snapshot.trim().is_empty() {
                    send_log(&ui_tx, "Snapshot ID is required for extract.");
                    continue;
                }

                if dest.trim().is_empty() {
                    send_log(&ui_tx, "Destination path is required for extract.");
                    continue;
                }

                let _ = ui_tx.send(UiEvent::Status("Extracting snapshot...".to_string()));

                match find_repo_for_snapshot(
                    &runtime.repos,
                    &repo_selector,
                    &snapshot,
                    &mut passphrases,
                ) {
                    Ok((repo, passphrase)) => {
                        let req = operations::ExtractRequest {
                            snapshot_name: snapshot.clone(),
                            destination: dest.clone(),
                            pattern: pattern.clone(),
                        };
                        match operations::extract_snapshot(&repo.config, passphrase.as_deref(), &req)
                        {
                            Ok(stats) => send_log(
                                &ui_tx,
                                format!(
                                    "Extracted snapshot {} -> {} (files={}, dirs={}, symlinks={}, bytes={})",
                                    snapshot,
                                    dest,
                                    stats.files,
                                    stats.dirs,
                                    stats.symlinks,
                                    format_bytes(stats.total_bytes),
                                ),
                            ),
                            Err(e) => send_log(&ui_tx, format!("Extract failed: {e}")),
                        }
                    }
                    Err(e) => send_log(&ui_tx, format!("Failed to resolve snapshot: {e}")),
                }

                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::ReloadConfig => {
                let config_path = runtime.source.path().to_path_buf();
                match app::load_runtime_config_from_path(&config_path) {
                    Ok(repos) => {
                        if repos.is_empty() {
                            send_log(&ui_tx, "Reloaded config is empty; keeping previous state.");
                            continue;
                        }

                        let schedule = repos[0].config.schedule.clone();
                        let interval = match vger_core::app::scheduler::schedule_interval(&schedule)
                        {
                            Ok(v) => v,
                            Err(e) => {
                                send_log(
                                    &ui_tx,
                                    format!(
                                        "Config reload rejected due to invalid schedule.every: {e}. Keeping previous config."
                                    ),
                                );
                                continue;
                            }
                        };

                        runtime.repos = repos;
                        passphrases.clear();

                        if let Ok(mut state) = scheduler.lock() {
                            state.enabled = schedule.enabled;
                            state.paused = schedule_paused;
                            state.every = interval;
                            state.jitter_seconds = schedule.jitter_seconds;
                            state.next_run = Some(Instant::now() + interval);
                        }

                        let _ = ui_tx.send(UiEvent::ConfigInfo {
                            path: config_path.display().to_string(),
                            schedule: schedule_description(&schedule, schedule_paused),
                        });
                        send_combo_data(&ui_tx, &runtime.repos);
                        send_log(&ui_tx, "Configuration reloaded.");
                    }
                    Err(e) => {
                        send_log(
                            &ui_tx,
                            format!("Configuration reload failed; keeping previous config: {e}"),
                        );
                    }
                }
            }
            AppCommand::ToggleSchedulePause => {
                schedule_paused = !schedule_paused;
                if let Ok(mut state) = scheduler.lock() {
                    state.paused = schedule_paused;
                }
                let schedule = runtime.schedule();
                let _ = ui_tx.send(UiEvent::ConfigInfo {
                    path: runtime.source.path().display().to_string(),
                    schedule: schedule_description(&schedule, schedule_paused),
                });
                send_log(
                    &ui_tx,
                    if schedule_paused {
                        "Scheduled backups paused.".to_string()
                    } else {
                        "Scheduled backups resumed.".to_string()
                    },
                );
            }
            AppCommand::ShowWindow => {
                let _ = ui_tx.send(UiEvent::ShowWindow);
            }
            AppCommand::Quit => {
                let _ = ui_tx.send(UiEvent::Quit);
                break;
            }
        }
    }
}

fn append_log(ui: &MainWindow, line: &str) {
    let current = ui.get_log_text();
    let mut next = current.to_string();
    if !next.is_empty() {
        next.push('\n');
    }
    next.push_str(line);
    ui.set_log_text(next.into());
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = app::load_runtime_config(None)?;
    let config_path = runtime.source.path().to_path_buf();

    let (app_tx, app_rx) = crossbeam_channel::unbounded::<AppCommand>();
    let (ui_tx, ui_rx) = crossbeam_channel::unbounded::<UiEvent>();

    let scheduler = Arc::new(Mutex::new(SchedulerState::default()));
    let backup_running = Arc::new(AtomicBool::new(false));

    spawn_config_watcher(config_path, app_tx.clone());
    spawn_scheduler(app_tx.clone(), scheduler.clone(), backup_running.clone());

    thread::spawn({
        let app_tx = app_tx.clone();
        let scheduler = scheduler.clone();
        let backup_running = backup_running.clone();
        move || run_worker(app_tx, app_rx, ui_tx, scheduler, backup_running, runtime)
    });

    let ui = MainWindow::new()?;
    ui.set_config_path("(loading...)".into());
    ui.set_schedule_text("(loading...)".into());
    ui.set_status_text("Idle".into());
    ui.set_extract_dest(".".into());

    let ui_weak_for_events = ui.as_weak();
    thread::spawn(move || {
        while let Ok(event) = ui_rx.recv() {
            let ui_weak = ui_weak_for_events.clone();
            let _ = slint::invoke_from_event_loop(move || {
                let Some(ui) = ui_weak.upgrade() else {
                    return;
                };

                match event {
                    UiEvent::Status(status) => ui.set_status_text(status.into()),
                    UiEvent::Log(line) => append_log(&ui, &line),
                    UiEvent::ConfigInfo { path, schedule } => {
                        ui.set_config_path(path.into());
                        ui.set_schedule_text(schedule.into());
                    }
                    UiEvent::SnapshotsText(text) => ui.set_snapshots_text(text.into()),
                    UiEvent::SnapshotItemsText(text) => ui.set_snapshot_items_text(text.into()),
                    UiEvent::RepoInfoText(text) => ui.set_repo_info_text(text.into()),
                    UiEvent::SourcesText(text) => ui.set_sources_text(text.into()),
                    UiEvent::RepoNames(names) => {
                        let shared: Vec<slint::SharedString> =
                            names.into_iter().map(|s| s.into()).collect();
                        ui.set_repo_names(slint::ModelRc::new(slint::VecModel::from(shared)));
                    }
                    UiEvent::SourceNames(names) => {
                        let shared: Vec<slint::SharedString> =
                            names.into_iter().map(|s| s.into()).collect();
                        ui.set_source_names(slint::ModelRc::new(slint::VecModel::from(shared)));
                    }
                    UiEvent::Quit => {
                        let _ = slint::quit_event_loop();
                    }
                    UiEvent::ShowWindow => {
                        let _ = ui.show();
                    }
                }
            });
        }
    });

    let tx = app_tx.clone();
    ui.on_backup_all_clicked(move || {
        let _ = tx.send(AppCommand::RunBackupAll { scheduled: false });
    });

    let tx = app_tx.clone();
    let ui_weak = ui.as_weak();
    ui.on_refresh_snapshots_clicked(move || {
        let Some(ui) = ui_weak.upgrade() else {
            return;
        };
        let _ = tx.send(AppCommand::RefreshSnapshots {
            repo_selector: ui.get_snapshots_repo_combo_value().to_string(),
        });
    });

    let tx = app_tx.clone();
    let ui_weak = ui.as_weak();
    ui.on_show_snapshot_items_clicked(move || {
        let Some(ui) = ui_weak.upgrade() else {
            return;
        };
        let _ = tx.send(AppCommand::ShowSnapshotItems {
            repo_selector: ui.get_snapshots_repo_combo_value().to_string(),
            snapshot: ui.get_snapshot_id().to_string(),
        });
    });

    let tx = app_tx.clone();
    let ui_weak = ui.as_weak();
    ui.on_extract_clicked(move || {
        let Some(ui) = ui_weak.upgrade() else {
            return;
        };

        let pattern = {
            let raw = ui.get_extract_pattern().to_string();
            if raw.trim().is_empty() {
                None
            } else {
                Some(raw)
            }
        };

        let _ = tx.send(AppCommand::Extract {
            repo_selector: ui.get_snapshots_repo_combo_value().to_string(),
            snapshot: ui.get_snapshot_id().to_string(),
            dest: ui.get_extract_dest().to_string(),
            pattern,
        });
    });

    let tx = app_tx.clone();
    ui.on_reload_config_clicked(move || {
        let _ = tx.send(AppCommand::ReloadConfig);
    });

    let tx = app_tx.clone();
    ui.on_pause_schedule_clicked(move || {
        let _ = tx.send(AppCommand::ToggleSchedulePause);
    });

    let tx = app_tx.clone();
    ui.on_quit_clicked(move || {
        let _ = tx.send(AppCommand::Quit);
    });

    let tx = app_tx.clone();
    let ui_weak = ui.as_weak();
    ui.on_fetch_repo_info_clicked(move || {
        let Some(ui) = ui_weak.upgrade() else {
            return;
        };
        let _ = tx.send(AppCommand::FetchRepoInfo {
            repo_selector: ui.get_repos_combo_value().to_string(),
        });
    });

    let tx = app_tx.clone();
    let ui_weak = ui.as_weak();
    ui.on_backup_repo_clicked(move || {
        let Some(ui) = ui_weak.upgrade() else {
            return;
        };
        let _ = tx.send(AppCommand::RunBackupRepo {
            repo_selector: ui.get_repos_combo_value().to_string(),
        });
    });

    let tx = app_tx.clone();
    let ui_weak = ui.as_weak();
    ui.on_backup_source_clicked(move || {
        let Some(ui) = ui_weak.upgrade() else {
            return;
        };
        let _ = tx.send(AppCommand::RunBackupSource {
            source_selector: ui.get_sources_combo_value().to_string(),
        });
    });

    // Close-to-tray behavior: hide window and keep background tasks running.
    ui.window().on_close_requested({
        let ui_weak = ui.as_weak();
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                let _ = ui.hide();
            }
            slint::CloseRequestResponse::HideWindow
        }
    });

    let (_tray_icon, open_item_id, run_now_item_id, pause_item_id, quit_item_id) =
        build_tray_icon().map_err(|e| format!("failed to initialize tray icon: {e}"))?;

    {
        let tx = app_tx.clone();
        thread::spawn(move || {
            let menu_rx = MenuEvent::receiver();
            while let Ok(event) = menu_rx.recv() {
                if event.id == open_item_id {
                    let _ = tx.send(AppCommand::ShowWindow);
                } else if event.id == run_now_item_id {
                    let _ = tx.send(AppCommand::RunBackupAll { scheduled: false });
                } else if event.id == pause_item_id {
                    let _ = tx.send(AppCommand::ToggleSchedulePause);
                } else if event.id == quit_item_id {
                    let _ = tx.send(AppCommand::Quit);
                    break;
                }
            }
        });
    }

    ui.run()?;
    Ok(())
}
