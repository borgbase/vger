use std::sync::Mutex;

use crate::app::passphrase::{resolve_passphrase, PassphrasePrompt};
use crate::config::EncryptionModeConfig;

use super::helpers::make_test_config;

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn set_vger_passphrase(value: Option<&str>) {
    unsafe {
        match value {
            Some(v) => std::env::set_var("VGER_PASSPHRASE", v),
            None => std::env::remove_var("VGER_PASSPHRASE"),
        }
    }
}

#[cfg(not(windows))]
fn print_script(text: &str) -> String {
    format!("printf '{text}'")
}

#[cfg(windows)]
fn print_script(text: &str) -> String {
    format!("Write-Output \"{text}\"")
}

#[test]
fn resolve_passphrase_returns_none_when_encryption_mode_is_none() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::None;
    config.encryption.passcommand = Some(print_script("cmd-pass"));
    set_vger_passphrase(Some("env-pass"));

    let mut prompted = false;
    let pass = resolve_passphrase(&config, Some("repo-a"), |_prompt| {
        prompted = true;
        Ok(Some("prompt-pass".into()))
    })
    .unwrap();

    assert!(pass.is_none());
    assert!(!prompted);
    set_vger_passphrase(None);
}

#[test]
fn resolve_passphrase_uses_expected_precedence() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Aes256Gcm;

    set_vger_passphrase(Some("env-pass"));
    config.encryption.passphrase = Some("inline-pass".into());
    config.encryption.passcommand = Some(print_script("cmd-pass"));
    let pass = resolve_passphrase(&config, None, |_prompt| Ok(Some("prompt-pass".into()))).unwrap();
    assert_eq!(pass.as_deref(), Some("inline-pass"));

    config.encryption.passphrase = None;
    let pass = resolve_passphrase(&config, None, |_prompt| Ok(Some("prompt-pass".into()))).unwrap();
    assert_eq!(pass.as_deref(), Some("cmd-pass"));

    config.encryption.passcommand = None;
    let pass = resolve_passphrase(&config, None, |_prompt| Ok(Some("prompt-pass".into()))).unwrap();
    assert_eq!(pass.as_deref(), Some("env-pass"));

    set_vger_passphrase(None);
    let pass = resolve_passphrase(&config, None, |_prompt| Ok(Some("prompt-pass".into()))).unwrap();
    assert_eq!(pass.as_deref(), Some("prompt-pass"));
}

#[test]
fn resolve_passphrase_surfaces_passcommand_failure() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
    config.encryption.passphrase = None;
    config.encryption.passcommand = Some("exit 7".into());
    set_vger_passphrase(None);

    let err = resolve_passphrase(&config, None, |_prompt| Ok(Some("prompt-pass".into())))
        .err()
        .unwrap();
    assert!(format!("{err}").contains("passcommand failed"));
}

#[test]
fn resolve_passphrase_passes_prompt_context() {
    let _lock = ENV_LOCK.lock().unwrap();
    let tmp = tempfile::tempdir().unwrap();
    let repo_dir = tmp.path().join("repo");
    std::fs::create_dir_all(&repo_dir).unwrap();
    let mut config = make_test_config(&repo_dir);
    config.encryption.mode = EncryptionModeConfig::Aes256Gcm;
    config.encryption.passphrase = None;
    config.encryption.passcommand = None;
    config.schedule.passphrase_prompt_timeout_seconds = 17;
    set_vger_passphrase(None);

    let mut seen_prompt: Option<PassphrasePrompt> = None;
    let pass = resolve_passphrase(&config, Some("repo-1"), |prompt| {
        seen_prompt = Some(prompt);
        Ok(Some("prompt-pass".into()))
    })
    .unwrap();

    assert_eq!(pass.as_deref(), Some("prompt-pass"));
    let prompt = seen_prompt.expect("prompt should have been invoked");
    assert_eq!(prompt.repository_label.as_deref(), Some("repo-1"));
    assert_eq!(prompt.repository_url, config.repository.url);
    assert_eq!(prompt.timeout_seconds, 17);
}
