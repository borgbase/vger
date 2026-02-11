use crate::config::BorgConfig;
use crate::error::Result;
use crate::repo::{EncryptionMode, Repository};
use crate::storage;

/// Run `borg-rs init`.
pub fn run(config: &BorgConfig, passphrase: Option<&str>) -> Result<Repository> {
    let backend = storage::backend_from_config(&config.repository)?;

    let encryption = match config.encryption.mode.as_str() {
        "none" => EncryptionMode::None,
        "aes256gcm" => EncryptionMode::Aes256Gcm,
        other => {
            return Err(crate::error::BorgError::Config(format!(
                "unknown encryption mode: {other}"
            )))
        }
    };

    let repo = Repository::init(
        backend,
        encryption,
        config.chunker.clone(),
        passphrase,
        Some(&config.repository),
    )?;

    Ok(repo)
}
