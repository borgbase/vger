use crate::config::{EncryptionModeConfig, VgerConfig};
use crate::error::{Result, VgerError};

#[derive(Debug, Clone)]
pub struct PassphrasePrompt {
    pub repository_label: Option<String>,
    pub repository_url: String,
    pub timeout_seconds: u64,
}

pub fn configured_passphrase(config: &VgerConfig) -> Result<Option<String>> {
    if let Some(ref p) = config.encryption.passphrase {
        return Ok(Some(p.clone()));
    }

    if let Some(ref cmd) = config.encryption.passcommand {
        let output = std::process::Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .output()
            .map_err(VgerError::Io)?;

        if !output.status.success() {
            return Err(VgerError::Config(format!(
                "passcommand failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }

        let pass = String::from_utf8(output.stdout)
            .map_err(|e| VgerError::Config(format!("passcommand output is not UTF-8: {e}")))?
            .trim()
            .to_string();

        if pass.is_empty() {
            return Err(VgerError::Config(
                "passcommand returned an empty passphrase".into(),
            ));
        }

        return Ok(Some(pass));
    }

    if let Ok(pass) = std::env::var("VGER_PASSPHRASE") {
        if !pass.is_empty() {
            return Ok(Some(pass));
        }
    }

    Ok(None)
}

pub fn resolve_passphrase<F>(
    config: &VgerConfig,
    label: Option<&str>,
    mut prompt: F,
) -> Result<Option<String>>
where
    F: FnMut(PassphrasePrompt) -> Result<Option<String>>,
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
