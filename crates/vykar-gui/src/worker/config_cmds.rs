use std::path::PathBuf;

use crate::config_helpers;
use crate::messages::UiEvent;
use crate::repo_helpers::send_log;

use super::WorkerContext;

pub(super) fn handle_open_config_file(ctx: &WorkerContext) {
    let path = ctx.runtime.source.path().display().to_string();
    send_log(&ctx.ui_tx, format!("Opening config file: {path}"));
    let _ = std::process::Command::new("open").arg(&path).spawn();
}

pub(super) fn handle_reload_config(ctx: &mut WorkerContext) {
    let config_path = dunce::canonicalize(ctx.runtime.source.path())
        .unwrap_or_else(|_| ctx.runtime.source.path().to_path_buf());
    config_helpers::apply_config(
        config_path,
        false,
        &mut ctx.runtime,
        &mut ctx.config_display_path,
        &mut ctx.passphrases,
        &ctx.scheduler,
        ctx.schedule_paused,
        ctx.scheduler_lock_held,
        &ctx.ui_tx,
        &ctx.app_tx,
        &ctx.sched_notify_tx,
    );
}

pub(super) fn handle_switch_config(ctx: &mut WorkerContext) {
    let picked = tinyfiledialogs::open_file_dialog(
        "Open vykar config",
        "",
        Some((&["*.yaml", "*.yml"], "YAML files")),
    );
    if let Some(path_str) = picked {
        config_helpers::apply_config(
            PathBuf::from(path_str),
            true,
            &mut ctx.runtime,
            &mut ctx.config_display_path,
            &mut ctx.passphrases,
            &ctx.scheduler,
            ctx.schedule_paused,
            ctx.scheduler_lock_held,
            &ctx.ui_tx,
            &ctx.app_tx,
            &ctx.sched_notify_tx,
        );
    }
}

pub(super) fn handle_save_and_apply_config(ctx: &mut WorkerContext, yaml_text: String) {
    let config_path = ctx.config_display_path.clone();
    let tmp_path = config_path.with_extension("yaml.tmp");
    if let Err(e) = std::fs::write(&tmp_path, &yaml_text) {
        let _ = ctx
            .ui_tx
            .send(UiEvent::ConfigSaveError(format!("Write failed: {e}")));
        return;
    }

    if let Err(msg) = config_helpers::validate_config(&tmp_path) {
        let _ = std::fs::remove_file(&tmp_path);
        let _ = ctx.ui_tx.send(UiEvent::ConfigSaveError(msg));
        return;
    }

    if let Err(e) = std::fs::rename(&tmp_path, &config_path) {
        let _ = std::fs::remove_file(&tmp_path);
        let _ = ctx
            .ui_tx
            .send(UiEvent::ConfigSaveError(format!("Rename failed: {e}")));
        return;
    }

    // apply_config re-runs validate_config internally, which is
    // redundant but harmless — it keeps the function self-contained.
    if config_helpers::apply_config(
        config_path,
        false,
        &mut ctx.runtime,
        &mut ctx.config_display_path,
        &mut ctx.passphrases,
        &ctx.scheduler,
        ctx.schedule_paused,
        ctx.scheduler_lock_held,
        &ctx.ui_tx,
        &ctx.app_tx,
        &ctx.sched_notify_tx,
    ) {
        send_log(&ctx.ui_tx, "Configuration saved and applied.");
    } else {
        let _ = ctx.ui_tx.send(UiEvent::ConfigSaveError(
            "Config saved to disk but failed to apply. Check log for details.".into(),
        ));
    }
}
