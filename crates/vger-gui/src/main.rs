use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{DateTime, Local};
use crossbeam_channel::{Receiver, Sender};
use notify::{Config as NotifyConfig, RecommendedWatcher, RecursiveMode, Watcher};
use slint::{ModelRc, SharedString, StandardListViewItem, VecModel};
use tray_icon::menu::{Menu, MenuEvent, MenuId, MenuItem};
use tray_icon::{Icon, TrayIconBuilder};
use vger_core::app::{self, operations, passphrase};
use vger_core::config::{self, ResolvedRepo, ScheduleConfig};
use vger_core::error::VgerError;

slint::slint! {
    import { VerticalBox, HorizontalBox, Button, LineEdit, ScrollView, TabWidget, ComboBox, StandardTableView, ListView } from "std-widgets.slint";

    struct RepoInfo {
        name: string,
        url: string,
        snapshots: string,
        last_snapshot: string,
    }

    export component RestoreWindow inherits Window {
        in-out property <string> snapshot_name;
        in-out property <string> repo_name;
        in-out property <string> extract_dest: ".";
        in-out property <string> extract_pattern;
        in-out property <string> status_text: "Ready";
        in-out property <[[StandardListViewItem]]> contents_rows: [];

        callback restore_all_clicked();
        callback restore_filtered_clicked();

        title: "Restore Snapshot";
        width: 900px;
        height: 600px;

        VerticalBox {
            padding: 12px;
            spacing: 8px;

            HorizontalBox {
                spacing: 8px;
                Text { text: "Snapshot:"; vertical-alignment: center; }
                Text { text: root.snapshot_name; vertical-alignment: center; }
                Text { text: "  Repository:"; vertical-alignment: center; }
                Text { text: root.repo_name; vertical-alignment: center; }
            }

            StandardTableView {
                vertical-stretch: 1;
                columns: [
                    { title: "Path", horizontal-stretch: 1 },
                    { title: "Type", min-width: 50px },
                    { title: "Permissions", min-width: 80px },
                    { title: "Size", min-width: 90px },
                ];
                rows: root.contents_rows;
            }

            Rectangle {
                height: 1px;
                background: #d5d5d5;
            }

            HorizontalBox {
                spacing: 8px;
                Text { text: "Destination:"; vertical-alignment: center; }
                LineEdit {
                    horizontal-stretch: 1;
                    text <=> root.extract_dest;
                }
            }

            HorizontalBox {
                spacing: 8px;
                Text { text: "Filter pattern (optional):"; vertical-alignment: center; }
                LineEdit {
                    horizontal-stretch: 1;
                    text <=> root.extract_pattern;
                }
            }

            HorizontalBox {
                spacing: 8px;
                Button {
                    text: "Restore All";
                    clicked => { root.restore_all_clicked(); }
                }
                Button {
                    text: "Restore Filtered";
                    clicked => { root.restore_filtered_clicked(); }
                }
                Text {
                    vertical-alignment: center;
                    text: root.status_text;
                }
            }
        }
    }

    export component MainWindow inherits Window {
        in-out property <string> config_path;
        in-out property <string> schedule_text;
        in-out property <string> status_text;
        in-out property <string> log_text;

        // Repo model (custom cards)
        in-out property <[RepoInfo]> repo_model: [];

        // Source table
        in-out property <[[StandardListViewItem]]> source_rows: [];

        // Snapshot table
        in-out property <[string]> repo_names: [];
        in-out property <string> snapshots_repo_combo_value;
        in-out property <[[StandardListViewItem]]> snapshot_rows: [];

        callback open_config_clicked();
        callback backup_all_clicked();
        callback reload_config_clicked();
        callback backup_repo_clicked(/* index */ int);
        callback check_repo_clicked(/* index */ int);
        callback backup_selected_source_clicked(/* row */ int);
        callback refresh_snapshots_clicked();
        callback restore_selected_snapshot_clicked(/* row */ int);
        callback delete_selected_snapshot_clicked(/* row */ int);
        callback snapshots_repo_changed(/* value */ string);

        title: "V'Ger";
        width: 1100px;
        height: 760px;

        VerticalBox {
            padding: 0px;
            spacing: 0px;

            // ── Header ──
            VerticalBox {
                padding-left: 12px;
                padding-right: 12px;
                padding-top: 10px;
                padding-bottom: 6px;
                spacing: 4px;

                HorizontalBox {
                    spacing: 4px;
                    Text { text: "Edit your backup configuration:"; vertical-alignment: center; }
                    TouchArea {
                        mouse-cursor: pointer;
                        clicked => { root.open_config_clicked(); }
                        Text { text: root.config_path; color: #4a90d9; vertical-alignment: center; }
                    }
                }
                HorizontalBox {
                    spacing: 16px;
                    Text { text: "Schedule:"; vertical-alignment: center; }
                    Text { text: root.schedule_text; vertical-alignment: center; }
                }
            }
            Rectangle { height: 1px; background: #d5d5d5; }

            // ── Tabs ──
            TabWidget {
                vertical-stretch: 1;

                Tab {
                    title: "Repositories";
                    VerticalBox {
                        spacing: 8px;
                        padding: 8px;
                        ListView {
                            vertical-stretch: 1;
                            for repo[idx] in root.repo_model: Rectangle {
                                height: 56px;
                                HorizontalLayout {
                                    padding: 8px;
                                    spacing: 16px;
                                    VerticalLayout {
                                        horizontal-stretch: 1;
                                        Text { text: repo.name; font-weight: 700; }
                                        Text { text: repo.url; color: #888888; font-size: 11px; }
                                    }
                                    VerticalLayout {
                                        horizontal-stretch: 1;
                                        Text { text: "Snapshots: " + repo.snapshots; }
                                        Text { text: "Latest: " + repo.last_snapshot; font-size: 11px; color: #888888; }
                                    }
                                    VerticalLayout {
                                        alignment: center;
                                        HorizontalLayout {
                                            spacing: 8px;
                                            Button { text: "Backup"; clicked => { root.backup_repo_clicked(idx); } }
                                            Button { text: "Check"; clicked => { root.check_repo_clicked(idx); } }
                                        }
                                    }
                                }
                                Rectangle { y: parent.height - 1px; height: 1px; background: #d5d5d5; width: 100%; }
                            }
                        }
                    }
                }

                Tab {
                    title: "Sources";
                    VerticalBox {
                        spacing: 8px;
                        padding: 8px;

                        source-table := StandardTableView {
                            vertical-stretch: 1;
                            columns: [
                                { title: "Label" },
                                { title: "Paths" },
                                { title: "Excludes" },
                                { title: "Target Repos" },
                            ];
                            rows: root.source_rows;
                        }

                        HorizontalBox {
                            spacing: 8px;
                            Button {
                                text: "Backup Selected Source";
                                clicked => {
                                    root.backup-selected-source-clicked(source-table.current-row);
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
                                selected(value) => {
                                    root.snapshots_repo_changed(value);
                                }
                            }
                            Button {
                                text: "Refresh";
                                clicked => { root.refresh_snapshots_clicked(); }
                            }
                        }

                        snapshot-table := StandardTableView {
                            vertical-stretch: 1;
                            columns: [
                                { title: "ID" },
                                { title: "Source" },
                                { title: "Label" },
                                { title: "Time" },
                            ];
                            rows: root.snapshot_rows;
                        }

                        HorizontalBox {
                            spacing: 8px;
                            Button {
                                text: "Restore Selected Snapshot";
                                clicked => {
                                    root.restore-selected-snapshot-clicked(snapshot-table.current-row);
                                }
                            }
                            Button {
                                text: "Delete Selected Snapshot";
                                clicked => {
                                    root.delete-selected-snapshot-clicked(snapshot-table.current-row);
                                }
                            }
                        }
                    }
                }

                Tab {
                    title: "Log";
                    VerticalBox {
                        padding: 8px;
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

            // ── Footer ──
            Rectangle {
                height: 1px;
                background: #d5d5d5;
            }

            HorizontalBox {
                padding-left: 12px;
                padding-right: 12px;
                padding-top: 6px;
                padding-bottom: 6px;
                spacing: 8px;

                Text { text: "Config:"; vertical-alignment: center; }
                Text { text: root.config_path; vertical-alignment: center; horizontal-stretch: 1; }
                Text { text: "Status:"; vertical-alignment: center; }
                Text { text: root.status_text; vertical-alignment: center; }

                Button {
                    text: "Backup All";
                    clicked => { root.backup_all_clicked(); }
                }
                Button {
                    text: "Reload";
                    clicked => { root.reload_config_clicked(); }
                }
            }
        }
    }
}

// ── Commands and Events ──

#[derive(Debug)]
enum AppCommand {
    RunBackupAll {
        scheduled: bool,
    },
    RunBackupRepo {
        repo_name: String,
    },
    RunBackupSource {
        source_label: String,
    },
    FetchAllRepoInfo,
    RefreshSnapshots {
        repo_selector: String,
    },
    FetchSnapshotContents {
        repo_name: String,
        snapshot_name: String,
    },
    Extract {
        repo_name: String,
        snapshot: String,
        dest: String,
        pattern: Option<String>,
    },
    CheckRepo {
        repo_name: String,
    },
    DeleteSnapshot {
        repo_name: String,
        snapshot_name: String,
    },
    OpenConfigFile,
    ReloadConfig,
    ToggleSchedulePause,
    ShowWindow,
    Quit,
}

#[derive(Debug, Clone)]
struct RepoInfoData {
    name: String,
    url: String,
    snapshots: String,
    last_snapshot: String,
}

#[derive(Debug, Clone)]
enum UiEvent {
    Status(String),
    Log(String),
    ConfigInfo {
        path: String,
        schedule: String,
    },
    RepoNames(Vec<String>),
    RepoModelData {
        items: Vec<RepoInfoData>,
        labels: Vec<String>,
    },
    SourceTableData {
        rows: Vec<Vec<String>>,
        source_labels: Vec<String>,
    },
    SnapshotTableData {
        rows: Vec<Vec<String>>,
        snapshot_ids: Vec<String>,
        repo_names: Vec<String>,
    },
    SnapshotContentsData {
        rows: Vec<Vec<String>>,
    },
    Quit,
    ShowWindow,
}

// ── Scheduler ──

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

// ── Tray icon ──

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

    let logo_bytes = include_bytes!("../../../docs/src/images/logo_simple.png");
    let logo_img = image::load_from_memory(logo_bytes)
        .map_err(|e| format!("failed to decode logo: {e}"))?
        .resize(44, 44, image::imageops::FilterType::Lanczos3)
        .into_rgba8();
    let (w, h) = logo_img.dimensions();
    let icon =
        Icon::from_rgba(logo_img.into_raw(), w, h).map_err(|e| format!("tray icon error: {e}"))?;

    let tray = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_tooltip("V'Ger")
        .with_icon(icon)
        .with_icon_as_template(true)
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

// ── Config watcher ──

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

// ── Scheduler thread ──

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

// ── Helpers ──

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

fn to_table_model(rows: Vec<Vec<String>>) -> ModelRc<ModelRc<StandardListViewItem>> {
    let outer: Vec<ModelRc<StandardListViewItem>> = rows
        .into_iter()
        .map(|row| {
            let items: Vec<StandardListViewItem> = row
                .into_iter()
                .map(|cell| StandardListViewItem::from(SharedString::from(cell)))
                .collect();
            ModelRc::new(VecModel::from(items))
        })
        .collect();
    ModelRc::new(VecModel::from(outer))
}

fn to_string_model(items: Vec<String>) -> ModelRc<SharedString> {
    let shared: Vec<SharedString> = items.into_iter().map(SharedString::from).collect();
    ModelRc::new(VecModel::from(shared))
}

fn collect_repo_names(repos: &[ResolvedRepo]) -> Vec<String> {
    repos.iter().map(|r| format_repo_name(r)).collect()
}

fn build_source_table_data(repos: &[ResolvedRepo]) -> (Vec<Vec<String>>, Vec<String>) {
    let mut seen = std::collections::HashSet::new();
    let mut rows = Vec::new();
    let mut labels = Vec::new();

    for repo in repos {
        for source in &repo.sources {
            if !seen.insert(source.label.clone()) {
                continue;
            }
            let target = if source.repos.is_empty() {
                "(all)".to_string()
            } else {
                source.repos.join(", ")
            };
            rows.push(vec![
                source.label.clone(),
                source.paths.join("\n"),
                source.exclude.join(", "),
                target,
            ]);
            labels.push(source.label.clone());
        }
    }

    (rows, labels)
}

fn send_structured_data(ui_tx: &Sender<UiEvent>, repos: &[ResolvedRepo]) {
    let _ = ui_tx.send(UiEvent::RepoNames(collect_repo_names(repos)));

    let (rows, labels) = build_source_table_data(repos);
    let _ = ui_tx.send(UiEvent::SourceTableData {
        rows,
        source_labels: labels,
    });
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

// ── Worker thread ──

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

    send_structured_data(&ui_tx, &runtime.repos);

    // Auto-fetch repo info at startup
    let _ = app_tx.send(AppCommand::FetchAllRepoInfo);

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
            AppCommand::RunBackupRepo { repo_name } => {
                let repo_name_sel = repo_name.trim().to_string();
                if repo_name_sel.is_empty() {
                    send_log(&ui_tx, "Select a repository first.");
                    continue;
                }

                let repo = match config::select_repo(&runtime.repos, &repo_name_sel) {
                    Some(r) => r,
                    None => {
                        send_log(&ui_tx, format!("No repository matching '{repo_name_sel}'."));
                        continue;
                    }
                };

                backup_running.store(true, Ordering::SeqCst);
                let rn = format_repo_name(repo);
                let _ = ui_tx.send(UiEvent::Status(format!("Running backup for [{rn}]...")));

                let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                    Ok(p) => p,
                    Err(e) => {
                        send_log(&ui_tx, format!("[{rn}] passphrase error: {e}"));
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
                        format!("[{rn}] passphrase prompt canceled; skipping."),
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
                    Ok(report) => log_backup_report(&ui_tx, &rn, &report),
                    Err(e) => send_log(&ui_tx, format!("[{rn}] backup failed: {e}")),
                }

                backup_running.store(false, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::RunBackupSource { source_label } => {
                let source_label = source_label.trim().to_string();
                if source_label.is_empty() {
                    send_log(&ui_tx, "Select a source first.");
                    continue;
                }

                backup_running.store(true, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::Status(format!(
                    "Running backup for source '{source_label}'..."
                )));

                let mut any_backed_up = false;
                for repo in &runtime.repos {
                    let matching_sources: Vec<config::SourceEntry> = repo
                        .sources
                        .iter()
                        .filter(|s| s.label == source_label)
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
                        format!("No repositories found with source '{source_label}'."),
                    );
                }

                backup_running.store(false, Ordering::SeqCst);
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::FetchAllRepoInfo => {
                let _ = ui_tx.send(UiEvent::Status("Fetching repository info...".to_string()));

                let mut items = Vec::new();
                let mut labels = Vec::new();

                for repo in &runtime.repos {
                    let repo_name = format_repo_name(repo);
                    let url = repo.config.repository.url.clone();
                    let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                        Ok(p) => p,
                        Err(e) => {
                            send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                            continue;
                        }
                    };

                    match vger_core::commands::info::run(&repo.config, passphrase.as_deref()) {
                        Ok(stats) => {
                            let last_snapshot = stats
                                .last_snapshot_time
                                .map(|t| {
                                    let local: DateTime<Local> = t.with_timezone(&Local);
                                    local.format("%Y-%m-%d %H:%M:%S").to_string()
                                })
                                .unwrap_or_else(|| "N/A".to_string());

                            items.push(RepoInfoData {
                                name: repo_name.clone(),
                                url,
                                snapshots: stats.snapshot_count.to_string(),
                                last_snapshot,
                            });
                            labels.push(repo_name);
                        }
                        Err(e) => {
                            send_log(&ui_tx, format!("[{repo_name}] info failed: {e}"));
                        }
                    }
                }

                let _ = ui_tx.send(UiEvent::RepoModelData { items, labels });
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::RefreshSnapshots { repo_selector } => {
                let _ = ui_tx.send(UiEvent::Status("Loading snapshots...".to_string()));

                let repos_to_scan = match select_repos(&runtime.repos, &repo_selector) {
                    Ok(repos) => repos,
                    Err(e) => {
                        send_log(&ui_tx, format!("Failed to select repository: {e}"));
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                let mut rows = Vec::new();
                let mut snapshot_ids = Vec::new();
                let mut repo_names = Vec::new();

                for repo in repos_to_scan {
                    let repo_name = format_repo_name(repo);
                    let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                        Ok(pass) => pass,
                        Err(e) => {
                            send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                            continue;
                        }
                    };

                    match operations::list_snapshots(&repo.config, passphrase.as_deref()) {
                        Ok(mut snapshots) => {
                            snapshots.sort_by_key(|s| s.time);
                            for s in snapshots {
                                let ts: DateTime<Local> = s.time.with_timezone(&Local);
                                // Source column: show actual paths, fallback to source_label
                                let sources = if s.source_paths.is_empty() {
                                    if s.source_label.is_empty() {
                                        "-".to_string()
                                    } else {
                                        s.source_label.clone()
                                    }
                                } else {
                                    s.source_paths.join("\n")
                                };
                                // Label column: show source_label
                                let label = if s.source_label.is_empty() {
                                    "-".to_string()
                                } else {
                                    s.source_label.clone()
                                };
                                rows.push(vec![
                                    s.name.clone(),
                                    sources,
                                    label,
                                    ts.format("%Y-%m-%d %H:%M:%S").to_string(),
                                ]);
                                snapshot_ids.push(s.name.clone());
                                repo_names.push(repo_name.clone());
                            }
                        }
                        Err(e) => {
                            send_log(
                                &ui_tx,
                                format!("[{repo_name}] snapshot listing failed: {e}"),
                            );
                        }
                    }
                }

                let _ = ui_tx.send(UiEvent::SnapshotTableData {
                    rows,
                    snapshot_ids,
                    repo_names,
                });
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::FetchSnapshotContents {
                repo_name,
                snapshot_name,
            } => {
                let _ = ui_tx.send(UiEvent::Status("Loading snapshot contents...".to_string()));

                match find_repo_for_snapshot(
                    &runtime.repos,
                    &repo_name,
                    &snapshot_name,
                    &mut passphrases,
                ) {
                    Ok((repo, passphrase)) => {
                        match operations::list_snapshot_items(
                            &repo.config,
                            passphrase.as_deref(),
                            &snapshot_name,
                        ) {
                            Ok(items) => {
                                let rows: Vec<Vec<String>> = items
                                    .iter()
                                    .map(|item| {
                                        let type_str = match item.entry_type {
                                            vger_core::snapshot::item::ItemType::Directory => "dir",
                                            vger_core::snapshot::item::ItemType::RegularFile => {
                                                "file"
                                            }
                                            vger_core::snapshot::item::ItemType::Symlink => "link",
                                        };
                                        vec![
                                            item.path.clone(),
                                            type_str.to_string(),
                                            format!("{:o}", item.mode & 0o7777),
                                            format_bytes(item.size),
                                        ]
                                    })
                                    .collect();

                                send_log(
                                    &ui_tx,
                                    format!(
                                        "Loaded {} item(s) from snapshot {} in [{}]",
                                        rows.len(),
                                        snapshot_name,
                                        format_repo_name(repo)
                                    ),
                                );

                                let _ = ui_tx.send(UiEvent::SnapshotContentsData { rows });
                            }
                            Err(e) => {
                                send_log(&ui_tx, format!("Failed to load snapshot items: {e}"));
                            }
                        }
                    }
                    Err(e) => {
                        send_log(&ui_tx, format!("Failed to resolve snapshot: {e}"));
                    }
                }

                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::Extract {
                repo_name,
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
                    &repo_name,
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
            AppCommand::CheckRepo { repo_name } => {
                let _ = ui_tx.send(UiEvent::Status("Checking repository...".to_string()));

                let repo = match config::select_repo(&runtime.repos, &repo_name) {
                    Some(r) => r,
                    None => {
                        send_log(&ui_tx, format!("No repository matching '{repo_name}'."));
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                    Ok(p) => p,
                    Err(e) => {
                        send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                match operations::check_repo(&repo.config, passphrase.as_deref(), false) {
                    Ok(result) => {
                        send_log(
                            &ui_tx,
                            format!(
                                "[{repo_name}] Check complete: {} snapshots, {} items checked, {} errors",
                                result.snapshots_checked, result.items_checked, result.errors.len()
                            ),
                        );
                        for err in &result.errors {
                            send_log(
                                &ui_tx,
                                format!("  [{repo_name}] {}: {}", err.context, err.message),
                            );
                        }
                    }
                    Err(e) => send_log(&ui_tx, format!("[{repo_name}] check failed: {e}")),
                }
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::DeleteSnapshot {
                repo_name,
                snapshot_name,
            } => {
                // Confirm with user
                let confirmed = tinyfiledialogs::message_box_yes_no(
                    "Delete Snapshot",
                    &format!(
                        "Are you sure you want to delete snapshot '{snapshot_name}' from [{repo_name}]?"
                    ),
                    tinyfiledialogs::MessageBoxIcon::Question,
                    tinyfiledialogs::YesNo::No,
                );

                if confirmed == tinyfiledialogs::YesNo::No {
                    send_log(&ui_tx, "Snapshot deletion cancelled.");
                    continue;
                }

                let _ = ui_tx.send(UiEvent::Status("Deleting snapshot...".to_string()));

                let repo = match config::select_repo(&runtime.repos, &repo_name) {
                    Some(r) => r,
                    None => {
                        send_log(&ui_tx, format!("No repository matching '{repo_name}'."));
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                let passphrase = match get_or_resolve_passphrase(repo, &mut passphrases) {
                    Ok(p) => p,
                    Err(e) => {
                        send_log(&ui_tx, format!("[{repo_name}] passphrase error: {e}"));
                        let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
                        continue;
                    }
                };

                match operations::delete_snapshot(
                    &repo.config,
                    passphrase.as_deref(),
                    &snapshot_name,
                ) {
                    Ok(stats) => {
                        send_log(
                            &ui_tx,
                            format!(
                                "[{repo_name}] Deleted snapshot '{}': {} chunks freed, {} reclaimed",
                                stats.snapshot_name,
                                stats.chunks_deleted,
                                format_bytes(stats.space_freed),
                            ),
                        );
                        // Auto-refresh snapshots
                        let _ = app_tx.send(AppCommand::RefreshSnapshots {
                            repo_selector: repo_name,
                        });
                    }
                    Err(e) => {
                        send_log(
                            &ui_tx,
                            format!("[{repo_name}] delete failed: {e}"),
                        );
                    }
                }
                let _ = ui_tx.send(UiEvent::Status("Idle".to_string()));
            }
            AppCommand::OpenConfigFile => {
                let path = runtime.source.path().display().to_string();
                send_log(&ui_tx, format!("Opening config file: {path}"));
                let _ = std::process::Command::new("open").arg(&path).spawn();
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
                        send_structured_data(&ui_tx, &runtime.repos);
                        let _ = app_tx.send(AppCommand::FetchAllRepoInfo);
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

// ── Main ──

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

    let restore_win = RestoreWindow::new()?;

    // Parallel arrays for looking up names by table row index.
    // Wrapped in Arc<Mutex<>> so callbacks and the event loop can share them.
    let repo_labels: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let source_labels: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let snapshot_ids: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let snapshot_repo_names: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    // ── Event loop consumer ──

    let ui_weak_for_events = ui.as_weak();
    let restore_weak_for_events = restore_win.as_weak();
    let repo_labels_for_events = repo_labels.clone();
    let source_labels_for_events = source_labels.clone();
    let snapshot_ids_for_events = snapshot_ids.clone();
    let snapshot_repo_names_for_events = snapshot_repo_names.clone();

    thread::spawn(move || {
        while let Ok(event) = ui_rx.recv() {
            let ui_weak = ui_weak_for_events.clone();
            let restore_weak = restore_weak_for_events.clone();
            let repo_labels = repo_labels_for_events.clone();
            let source_labels = source_labels_for_events.clone();
            let snapshot_ids = snapshot_ids_for_events.clone();
            let snapshot_repo_names = snapshot_repo_names_for_events.clone();

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
                    UiEvent::RepoNames(names) => {
                        let first = names.first().cloned().unwrap_or_default();
                        ui.set_repo_names(to_string_model(names));
                        // Pre-select first repo in snapshots combo
                        if ui.get_snapshots_repo_combo_value().is_empty() {
                            ui.set_snapshots_repo_combo_value(first.into());
                        }
                    }
                    UiEvent::RepoModelData {
                        items,
                        labels,
                    } => {
                        if let Ok(mut rl) = repo_labels.lock() {
                            *rl = labels;
                        }
                        let model: Vec<RepoInfo> = items
                            .into_iter()
                            .map(|d| RepoInfo {
                                name: d.name.into(),
                                url: d.url.into(),
                                snapshots: d.snapshots.into(),
                                last_snapshot: d.last_snapshot.into(),
                            })
                            .collect();
                        ui.set_repo_model(ModelRc::new(VecModel::from(model)));
                    }
                    UiEvent::SourceTableData {
                        rows,
                        source_labels: labels,
                    } => {
                        if let Ok(mut sl) = source_labels.lock() {
                            *sl = labels;
                        }
                        ui.set_source_rows(to_table_model(rows));
                    }
                    UiEvent::SnapshotTableData {
                        rows,
                        snapshot_ids: ids,
                        repo_names: rnames,
                    } => {
                        if let Ok(mut si) = snapshot_ids.lock() {
                            *si = ids;
                        }
                        if let Ok(mut sr) = snapshot_repo_names.lock() {
                            *sr = rnames;
                        }
                        ui.set_snapshot_rows(to_table_model(rows));
                    }
                    UiEvent::SnapshotContentsData { rows } => {
                        if let Some(rw) = restore_weak.upgrade() {
                            rw.set_contents_rows(to_table_model(rows));
                            rw.set_status_text("Ready".into());
                        }
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

    // ── Callback wiring: MainWindow ──

    let tx = app_tx.clone();
    ui.on_open_config_clicked(move || {
        let _ = tx.send(AppCommand::OpenConfigFile);
    });

    let tx = app_tx.clone();
    ui.on_backup_all_clicked(move || {
        let _ = tx.send(AppCommand::RunBackupAll { scheduled: false });
    });

    let tx = app_tx.clone();
    ui.on_reload_config_clicked(move || {
        let _ = tx.send(AppCommand::ReloadConfig);
    });

    let tx = app_tx.clone();
    let rl = repo_labels.clone();
    ui.on_backup_repo_clicked(move |idx| {
        if idx < 0 {
            return;
        }
        if let Ok(labels) = rl.lock() {
            if let Some(name) = labels.get(idx as usize) {
                let _ = tx.send(AppCommand::RunBackupRepo {
                    repo_name: name.clone(),
                });
            }
        }
    });

    let tx = app_tx.clone();
    let rl = repo_labels.clone();
    ui.on_check_repo_clicked(move |idx| {
        if idx < 0 {
            return;
        }
        if let Ok(labels) = rl.lock() {
            if let Some(name) = labels.get(idx as usize) {
                let _ = tx.send(AppCommand::CheckRepo {
                    repo_name: name.clone(),
                });
            }
        }
    });

    let tx = app_tx.clone();
    let sl = source_labels.clone();
    ui.on_backup_selected_source_clicked(move |row| {
        if row < 0 {
            return;
        }
        if let Ok(labels) = sl.lock() {
            if let Some(label) = labels.get(row as usize) {
                let _ = tx.send(AppCommand::RunBackupSource {
                    source_label: label.clone(),
                });
            }
        }
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
    ui.on_snapshots_repo_changed({
        let tx = tx.clone();
        move |value| {
            let _ = tx.send(AppCommand::RefreshSnapshots {
                repo_selector: value.to_string(),
            });
        }
    });

    let tx = app_tx.clone();
    let si = snapshot_ids.clone();
    let sr = snapshot_repo_names.clone();
    let rw_weak = restore_win.as_weak();
    ui.on_restore_selected_snapshot_clicked(move |row| {
        if row < 0 {
            return;
        }
        let (snap_name, rname) = {
            let ids = si.lock().unwrap_or_else(|e| e.into_inner());
            let rnames = sr.lock().unwrap_or_else(|e| e.into_inner());
            match (ids.get(row as usize), rnames.get(row as usize)) {
                (Some(id), Some(rn)) => (id.clone(), rn.clone()),
                _ => return,
            }
        };

        if let Some(rw) = rw_weak.upgrade() {
            rw.set_snapshot_name(snap_name.clone().into());
            rw.set_repo_name(rname.clone().into());
            rw.set_status_text("Loading contents...".into());
            rw.set_contents_rows(to_table_model(vec![]));
            let _ = rw.show();
        }

        let _ = tx.send(AppCommand::FetchSnapshotContents {
            repo_name: rname,
            snapshot_name: snap_name,
        });
    });

    let tx = app_tx.clone();
    let si = snapshot_ids.clone();
    let sr = snapshot_repo_names.clone();
    ui.on_delete_selected_snapshot_clicked(move |row| {
        if row < 0 {
            return;
        }
        let (snap_name, rname) = {
            let ids = si.lock().unwrap_or_else(|e| e.into_inner());
            let rnames = sr.lock().unwrap_or_else(|e| e.into_inner());
            match (ids.get(row as usize), rnames.get(row as usize)) {
                (Some(id), Some(rn)) => (id.clone(), rn.clone()),
                _ => return,
            }
        };

        let _ = tx.send(AppCommand::DeleteSnapshot {
            repo_name: rname,
            snapshot_name: snap_name,
        });
    });

    // ── Callback wiring: RestoreWindow ──

    let tx = app_tx.clone();
    let rw_weak = restore_win.as_weak();
    restore_win.on_restore_all_clicked(move || {
        let Some(rw) = rw_weak.upgrade() else {
            return;
        };
        let _ = tx.send(AppCommand::Extract {
            repo_name: rw.get_repo_name().to_string(),
            snapshot: rw.get_snapshot_name().to_string(),
            dest: rw.get_extract_dest().to_string(),
            pattern: None,
        });
    });

    let tx = app_tx.clone();
    let rw_weak = restore_win.as_weak();
    restore_win.on_restore_filtered_clicked(move || {
        let Some(rw) = rw_weak.upgrade() else {
            return;
        };
        let pattern_raw = rw.get_extract_pattern().to_string();
        let pattern = if pattern_raw.trim().is_empty() {
            None
        } else {
            Some(pattern_raw)
        };
        let _ = tx.send(AppCommand::Extract {
            repo_name: rw.get_repo_name().to_string(),
            snapshot: rw.get_snapshot_name().to_string(),
            dest: rw.get_extract_dest().to_string(),
            pattern,
        });
    });

    // ── Close-to-tray behavior ──

    ui.window().on_close_requested({
        let ui_weak = ui.as_weak();
        move || {
            if let Some(ui) = ui_weak.upgrade() {
                let _ = ui.hide();
            }
            slint::CloseRequestResponse::HideWindow
        }
    });

    // ── Tray icon ──

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
