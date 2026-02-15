use std::time::Duration;

use zeroize::{Zeroize, Zeroizing};

use crate::config::{EncryptionModeConfig, VgerConfig};
use crate::error::{Result, VgerError};
use crate::platform::shell;

/// Default timeout for passcommand execution (60 seconds).
const PASSCOMMAND_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone)]
pub struct PassphrasePrompt {
    pub repository_label: Option<String>,
    pub repository_url: String,
    pub timeout_seconds: u64,
}

pub fn configured_passphrase(config: &VgerConfig) -> Result<Option<Zeroizing<String>>> {
    if let Some(ref p) = config.encryption.passphrase {
        return Ok(Some(Zeroizing::new(p.clone())));
    }

    if let Some(ref cmd) = config.encryption.passcommand {
        let output =
            shell::run_script_with_timeout(cmd, PASSCOMMAND_TIMEOUT).map_err(VgerError::Io)?;

        if !output.status.success() {
            return Err(VgerError::Config(format!(
                "passcommand failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        let mut raw = String::from_utf8(output.stdout)
            .map_err(|e| VgerError::Config(format!("passcommand output is not UTF-8: {e}")))?;
        let pass = Zeroizing::new(raw.trim().to_string());
        raw.zeroize();

        if pass.is_empty() {
            return Err(VgerError::Config(
                "passcommand returned an empty passphrase".into(),
            ));
        }

        return Ok(Some(pass));
    }

    if let Ok(pass) = std::env::var("VGER_PASSPHRASE") {
        if !pass.is_empty() {
            return Ok(Some(Zeroizing::new(pass)));
        }
    }

    Ok(None)
}

pub fn resolve_passphrase<F>(
    config: &VgerConfig,
    label: Option<&str>,
    mut prompt: F,
) -> Result<Option<Zeroizing<String>>>
where
    F: FnMut(PassphrasePrompt) -> Result<Option<Zeroizing<String>>>,
{
    if config.encryption.mode == EncryptionModeConfig::None {
        return Ok(None);
    }

    if let Some(pass) = configured_passphrase(config)? {
        return Ok(Some(pass));
    }

    prompt(PassphrasePrompt {
        repository_label: label.map(|s| s.to_string()),
        repository_url: config.repository.url.clone(),
        timeout_seconds: config.schedule.passphrase_prompt_timeout_seconds,
    })
}
