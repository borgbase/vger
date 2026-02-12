use std::process::Command;

use crate::config::HooksConfig;
use crate::error::{Result, VgerError};

use crate::config::SourceHooksConfig;

/// Context passed to hook commands via environment variables and variable substitution.
pub struct HookContext {
    pub command: String,
    pub repository: String,
    pub label: Option<String>,
    pub error: Option<String>,
    pub source_label: Option<String>,
    pub source_paths: Option<Vec<String>>,
}

/// Run the full hook lifecycle around an action:
///
/// 1. `before` / `before_<cmd>` hooks (global then repo, bare then specific)
/// 2. The action itself
/// 3. On success: `after_<cmd>` then `after` (repo then global)
///    On failure: `failed_<cmd>` then `failed` (repo then global)
/// 4. Always: `finally_<cmd>` then `finally` (repo then global)
///
/// `before` hook failure aborts the action and triggers `failed` + `finally`.
/// `after` / `failed` / `finally` hook failures are logged but don't affect the result.
pub fn run_with_hooks<F, T>(
    global: &HooksConfig,
    repo: &HooksConfig,
    ctx: &mut HookContext,
    action: F,
) -> std::result::Result<T, Box<dyn std::error::Error>>
where
    F: FnOnce() -> std::result::Result<T, Box<dyn std::error::Error>>,
{
    let cmd = ctx.command.clone();
    let before_key = format!("before_{cmd}");
    let after_key = format!("after_{cmd}");
    let failed_key = format!("failed_{cmd}");
    let finally_key = format!("finally_{cmd}");

    // 1. Run before hooks: global bare → repo bare → global specific → repo specific
    let before_result = (|| -> Result<()> {
        run_hook_list(global.get_hooks("before"), ctx)?;
        run_hook_list(repo.get_hooks("before"), ctx)?;
        run_hook_list(global.get_hooks(&before_key), ctx)?;
        run_hook_list(repo.get_hooks(&before_key), ctx)?;
        Ok(())
    })();

    let action_result = if let Err(e) = before_result {
        // Before hook failed — skip action, go to failed/finally
        ctx.error = Some(e.to_string());
        Err(e.into())
    } else {
        // 2. Run the action
        action()
    };

    // 3. After or Failed hooks
    match &action_result {
        Ok(_) => {
            // after: repo specific → global specific → repo bare → global bare
            log_hook_errors(run_hook_list(repo.get_hooks(&after_key), ctx));
            log_hook_errors(run_hook_list(global.get_hooks(&after_key), ctx));
            log_hook_errors(run_hook_list(repo.get_hooks("after"), ctx));
            log_hook_errors(run_hook_list(global.get_hooks("after"), ctx));
        }
        Err(e) => {
            if ctx.error.is_none() {
                ctx.error = Some(e.to_string());
            }
            // failed: repo specific → global specific → repo bare → global bare
            log_hook_errors(run_hook_list(repo.get_hooks(&failed_key), ctx));
            log_hook_errors(run_hook_list(global.get_hooks(&failed_key), ctx));
            log_hook_errors(run_hook_list(repo.get_hooks("failed"), ctx));
            log_hook_errors(run_hook_list(global.get_hooks("failed"), ctx));
        }
    }

    // 4. Finally hooks: repo specific → global specific → repo bare → global bare
    log_hook_errors(run_hook_list(repo.get_hooks(&finally_key), ctx));
    log_hook_errors(run_hook_list(global.get_hooks(&finally_key), ctx));
    log_hook_errors(run_hook_list(repo.get_hooks("finally"), ctx));
    log_hook_errors(run_hook_list(global.get_hooks("finally"), ctx));

    action_result
}

fn run_hook_list(cmds: &[String], ctx: &HookContext) -> Result<()> {
    for cmd in cmds {
        execute_hook_command(cmd, ctx)?;
    }
    Ok(())
}

fn execute_hook_command(cmd: &str, ctx: &HookContext) -> Result<()> {
    let expanded = substitute_variables(cmd, ctx);
    tracing::info!("Running hook: {expanded}");

    let mut child = Command::new("sh");
    child.arg("-c").arg(&expanded);

    // Set environment variables
    child.env("VGER_COMMAND", &ctx.command);
    child.env("VGER_REPOSITORY", &ctx.repository);
    if let Some(ref label) = ctx.label {
        child.env("VGER_LABEL", label);
    }
    if let Some(ref error) = ctx.error {
        child.env("VGER_ERROR", error);
    }
    if let Some(ref source_label) = ctx.source_label {
        child.env("VGER_SOURCE_LABEL", source_label);
    }
    if let Some(ref source_paths) = ctx.source_paths {
        child.env("VGER_SOURCE_PATH", source_paths.join(":"));
    }

    let output = child
        .output()
        .map_err(|e| VgerError::Hook(format!("failed to execute '{expanded}': {e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let code = output
            .status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "signal".to_string());
        return Err(VgerError::Hook(format!(
            "hook '{expanded}' exited with {code}: {stderr}"
        )));
    }

    Ok(())
}

fn substitute_variables(cmd: &str, ctx: &HookContext) -> String {
    let mut result = cmd.replace("{command}", &shell_escape(&ctx.command));
    result = result.replace("{repository}", &shell_escape(&ctx.repository));
    result = result.replace("{label}", &shell_escape(ctx.label.as_deref().unwrap_or("")));
    result = result.replace("{error}", &shell_escape(ctx.error.as_deref().unwrap_or("")));
    result = result.replace(
        "{source_label}",
        &shell_escape(ctx.source_label.as_deref().unwrap_or("")),
    );
    let source_path_str = ctx
        .source_paths
        .as_ref()
        .map(|ps| ps.join(":"))
        .unwrap_or_default();
    result = result.replace("{source_path}", &shell_escape(&source_path_str));
    result
}

fn shell_escape(input: &str) -> String {
    if input.is_empty() {
        return "''".to_string();
    }
    let escaped = input.replace('\'', "'\"'\"'");
    format!("'{escaped}'")
}

/// Run source-level hooks (before/after/failed/finally) around an action.
pub fn run_source_hooks<F, T>(
    hooks: &SourceHooksConfig,
    ctx: &mut HookContext,
    action: F,
) -> std::result::Result<T, Box<dyn std::error::Error>>
where
    F: FnOnce() -> std::result::Result<T, Box<dyn std::error::Error>>,
{
    // 1. Run before hooks
    let before_result = run_hook_list(&hooks.before, ctx);

    let action_result = if let Err(e) = before_result {
        ctx.error = Some(e.to_string());
        Err(e.into())
    } else {
        action()
    };

    // 2. After or Failed hooks
    match &action_result {
        Ok(_) => {
            log_hook_errors(run_hook_list(&hooks.after, ctx));
        }
        Err(e) => {
            if ctx.error.is_none() {
                ctx.error = Some(e.to_string());
            }
            log_hook_errors(run_hook_list(&hooks.failed, ctx));
        }
    }

    // 3. Finally hooks
    log_hook_errors(run_hook_list(&hooks.finally, ctx));

    action_result
}

fn log_hook_errors(result: Result<()>) {
    if let Err(e) = result {
        tracing::warn!("Hook warning: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_ctx(command: &str) -> HookContext {
        HookContext {
            command: command.to_string(),
            repository: "/tmp/repo".to_string(),
            label: Some("test".to_string()),
            error: None,
            source_label: None,
            source_paths: None,
        }
    }

    fn hooks_from(pairs: &[(&str, Vec<&str>)]) -> HooksConfig {
        let mut hooks = std::collections::HashMap::new();
        for (key, cmds) in pairs {
            hooks.insert(
                key.to_string(),
                cmds.iter().map(|s| s.to_string()).collect(),
            );
        }
        HooksConfig { hooks }
    }

    #[test]
    fn test_variable_substitution() {
        let ctx = HookContext {
            command: "backup".into(),
            repository: "/mnt/nas".into(),
            label: Some("nas".into()),
            error: Some("disk full".into()),
            source_label: Some("docs".into()),
            source_paths: Some(vec!["/home/user/docs".into()]),
        };
        let result = substitute_variables(
            "echo {command} {repository} {label} {error} {source_label} {source_path}",
            &ctx,
        );
        assert_eq!(
            result,
            "echo 'backup' '/mnt/nas' 'nas' 'disk full' 'docs' '/home/user/docs'"
        );
    }

    #[test]
    fn test_variable_substitution_multi_paths() {
        let ctx = HookContext {
            command: "backup".into(),
            repository: "/mnt/nas".into(),
            label: Some("nas".into()),
            error: None,
            source_label: Some("default".into()),
            source_paths: Some(vec!["/home/user/docs".into(), "/home/user/photos".into()]),
        };
        let result = substitute_variables("paths={source_path}", &ctx);
        assert_eq!(result, "paths='/home/user/docs:/home/user/photos'");
    }

    #[test]
    fn test_variable_substitution_missing_optionals() {
        let ctx = HookContext {
            command: "backup".into(),
            repository: "/tmp/repo".into(),
            label: None,
            error: None,
            source_label: None,
            source_paths: None,
        };
        let result = substitute_variables("cmd={command} label={label} err={error}", &ctx);
        assert_eq!(result, "cmd='backup' label='' err=''");
    }

    #[test]
    fn test_hook_env_vars() {
        // Use env to print vars, verify they're set
        let global = HooksConfig::default();
        let repo = hooks_from(&[(
            "before_backup",
            vec!["test \"$VGER_COMMAND\" = backup && test \"$VGER_REPOSITORY\" = /tmp/repo && test \"$VGER_LABEL\" = test"],
        )]);
        let mut ctx = make_ctx("backup");

        let result = run_with_hooks(&global, &repo, &mut ctx, || Ok(()));
        assert!(result.is_ok(), "env vars should be set: {:?}", result.err());
    }

    #[test]
    fn test_before_hook_success() {
        let global = hooks_from(&[("before", vec!["true"])]);
        let repo = HooksConfig::default();
        let mut ctx = make_ctx("backup");

        let result = run_with_hooks(&global, &repo, &mut ctx, || Ok(42));
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_before_hook_failure_aborts() {
        let global = hooks_from(&[("before", vec!["false"])]);
        let repo = HooksConfig::default();
        let mut ctx = make_ctx("backup");
        let mut action_ran = false;

        let result = run_with_hooks(&global, &repo, &mut ctx, || {
            action_ran = true;
            Ok(())
        });

        assert!(result.is_err());
        assert!(!action_ran, "action should not run when before hook fails");
    }

    #[test]
    fn test_after_runs_on_success_only() {
        // after hook writes a marker file
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("after_ran");
        let cmd = format!("touch {}", marker.display());

        let global = hooks_from(&[("after", vec![&cmd])]);
        let repo = HooksConfig::default();
        let mut ctx = make_ctx("backup");

        let result = run_with_hooks(&global, &repo, &mut ctx, || Ok(()));
        assert!(result.is_ok());
        assert!(marker.exists(), "after hook should run on success");

        // Now test failure case
        let marker2 = dir.path().join("after_ran2");
        let cmd2 = format!("touch {}", marker2.display());
        let global2 = hooks_from(&[("after", vec![&cmd2])]);
        let mut ctx2 = make_ctx("backup");

        let _result: std::result::Result<(), _> =
            run_with_hooks(&global2, &repo, &mut ctx2, || Err("action failed".into()));
        assert!(!marker2.exists(), "after hook should NOT run on failure");
    }

    #[test]
    fn test_failed_runs_on_failure_only() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("failed_ran");
        let cmd = format!("touch {}", marker.display());

        let global = hooks_from(&[("failed", vec![&cmd])]);
        let repo = HooksConfig::default();
        let mut ctx = make_ctx("backup");

        let _result: std::result::Result<(), _> =
            run_with_hooks(&global, &repo, &mut ctx, || Err("something broke".into()));
        assert!(marker.exists(), "failed hook should run on failure");

        // Now test success case
        let marker2 = dir.path().join("failed_ran2");
        let cmd2 = format!("touch {}", marker2.display());
        let global2 = hooks_from(&[("failed", vec![&cmd2])]);
        let mut ctx2 = make_ctx("backup");

        let _result = run_with_hooks(&global2, &repo, &mut ctx2, || Ok(()));
        assert!(!marker2.exists(), "failed hook should NOT run on success");
    }

    #[test]
    fn test_finally_runs_always() {
        let dir = tempfile::tempdir().unwrap();

        // Test on success
        let marker1 = dir.path().join("finally_success");
        let cmd1 = format!("touch {}", marker1.display());
        let global1 = hooks_from(&[("finally", vec![&cmd1])]);
        let repo = HooksConfig::default();
        let mut ctx1 = make_ctx("backup");

        let _result = run_with_hooks(&global1, &repo, &mut ctx1, || Ok(()));
        assert!(marker1.exists(), "finally should run on success");

        // Test on failure
        let marker2 = dir.path().join("finally_failure");
        let cmd2 = format!("touch {}", marker2.display());
        let global2 = hooks_from(&[("finally", vec![&cmd2])]);
        let mut ctx2 = make_ctx("backup");

        let _result: std::result::Result<(), _> =
            run_with_hooks(&global2, &repo, &mut ctx2, || Err("error".into()));
        assert!(marker2.exists(), "finally should run on failure");
    }

    #[test]
    fn test_command_specific_hooks() {
        let dir = tempfile::tempdir().unwrap();
        let marker_backup = dir.path().join("before_backup_ran");
        let marker_prune = dir.path().join("before_prune_ran");
        let cmd_backup = format!("touch {}", marker_backup.display());
        let cmd_prune = format!("touch {}", marker_prune.display());

        let global = hooks_from(&[
            ("before_backup", vec![&cmd_backup]),
            ("before_prune", vec![&cmd_prune]),
        ]);
        let repo = HooksConfig::default();
        let mut ctx = make_ctx("backup");

        let result = run_with_hooks(&global, &repo, &mut ctx, || Ok(()));
        assert!(result.is_ok());
        assert!(
            marker_backup.exists(),
            "before_backup should run for backup command"
        );
        assert!(
            !marker_prune.exists(),
            "before_prune should NOT run for backup command"
        );
    }

    #[test]
    fn test_before_failure_triggers_failed_and_finally() {
        let dir = tempfile::tempdir().unwrap();
        let failed_marker = dir.path().join("failed_ran");
        let finally_marker = dir.path().join("finally_ran");
        let failed_cmd = format!("touch {}", failed_marker.display());
        let finally_cmd = format!("touch {}", finally_marker.display());

        let global = hooks_from(&[
            ("before", vec!["false"]),
            ("failed", vec![&failed_cmd]),
            ("finally", vec![&finally_cmd]),
        ]);
        let repo = HooksConfig::default();
        let mut ctx = make_ctx("backup");

        let result = run_with_hooks(&global, &repo, &mut ctx, || Ok(()));
        assert!(result.is_err());
        assert!(
            failed_marker.exists(),
            "failed hook should run when before hook fails"
        );
        assert!(
            finally_marker.exists(),
            "finally hook should run when before hook fails"
        );
    }
}
