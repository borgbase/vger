use crate::config::{EncryptionModeConfig, VgerConfig};
use crate::error::Result;
use crate::repo::{EncryptionMode, Repository};
use crate::storage;

/// Run `vger init`.
pub fn run(config: &VgerConfig, passphrase: Option<&str>) -> Result<Repository> {
    let backend = storage::backend_from_config(&config.repository)?;

    let encryption = match config.encryption.mode {
        EncryptionModeConfig::None => EncryptionMode::None,
        EncryptionModeConfig::Aes256Gcm => EncryptionMode::Aes256Gcm,
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
