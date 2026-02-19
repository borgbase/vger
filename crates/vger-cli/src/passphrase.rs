use zeroize::Zeroizing;

use crate::prompt::prompt_hidden;
use vger_core::config::{EncryptionModeConfig, VgerConfig};

pub(crate) fn with_repo_passphrase<T>(
    config: &VgerConfig,
    label: Option<&str>,
    action: impl FnOnce(Option<&str>) -> Result<T, Box<dyn std::error::Error>>,
) -> Result<T, Box<dyn std::error::Error>> {
    let passphrase = get_passphrase(config, label)?;
    action(passphrase.as_deref().map(|s| s.as_str()))
}

pub(crate) fn get_passphrase(
    config: &VgerConfig,
    label: Option<&str>,
) -> Result<Option<Zeroizing<String>>, Box<dyn std::error::Error>> {
    if config.encryption.mode == EncryptionModeConfig::None {
        return Ok(None);
    }

    if let Some(pass) = configured_passphrase(config)? {
        return Ok(Some(pass));
    }

    // Interactive prompt
    let prompt = match label {
        Some(l) => format!("Enter passphrase for '{l}': "),
        None => "Enter passphrase: ".to_string(),
    };
    let pass = Zeroizing::new(prompt_hidden(&prompt)?);
    Ok(Some(pass))
}

fn configured_passphrase(
    config: &VgerConfig,
) -> Result<Option<Zeroizing<String>>, Box<dyn std::error::Error>> {
    vger_core::app::passphrase::configured_passphrase(config)
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
}

pub(crate) fn get_init_passphrase(
    config: &VgerConfig,
    label: Option<&str>,
) -> Result<Option<Zeroizing<String>>, Box<dyn std::error::Error>> {
    if config.encryption.mode == EncryptionModeConfig::None {
        return Ok(None);
    }
    if let Some(pass) = configured_passphrase(config)? {
        return Ok(Some(pass));
    }

    let suffix = label.map(|l| format!(" for '{l}'")).unwrap_or_default();
    let p1 = Zeroizing::new(prompt_hidden(&format!("Enter new passphrase{suffix}: "))?);
    let p2 = Zeroizing::new(prompt_hidden(&format!("Confirm passphrase{suffix}: "))?);
    if *p1 != *p2 {
        return Err("passphrases do not match".into());
    }
    Ok(Some(p1))
}
