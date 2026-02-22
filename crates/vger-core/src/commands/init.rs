use crate::config::{EncryptionModeConfig, VgerConfig};
use crate::repo::{EncryptionMode, Repository};
use crate::storage;
use vger_crypto::select::{self, AutoAeadMode};
use vger_types::error::Result;

/// Run `vger init`.
pub fn run(config: &VgerConfig, passphrase: Option<&str>) -> Result<Repository> {
    let backend = storage::backend_from_config(&config.repository)?;

    let encryption = match config.encryption.mode {
        EncryptionModeConfig::None => EncryptionMode::None,
        EncryptionModeConfig::Auto => match select::select_best_aead() {
            AutoAeadMode::Aes256Gcm => EncryptionMode::Aes256Gcm,
            AutoAeadMode::Chacha20Poly1305 => EncryptionMode::Chacha20Poly1305,
        },
        EncryptionModeConfig::Aes256Gcm => EncryptionMode::Aes256Gcm,
        EncryptionModeConfig::Chacha20Poly1305 => EncryptionMode::Chacha20Poly1305,
    };

    let repo = Repository::init(
        backend,
        encryption,
        config.chunker.clone(),
        passphrase,
        Some(&config.repository),
        super::util::cache_dir_from_config(config),
    )?;

    Ok(repo)
}
