use vger_crypto::key::{EncryptedKey, MasterKey};

#[test]
fn generate_produces_nonzero_keys() {
    let key = MasterKey::generate();
    assert_ne!(key.encryption_key, [0u8; 32]);
    assert_ne!(key.chunk_id_key, [0u8; 32]);
}

#[test]
fn generate_produces_different_keys_each_time() {
    let k1 = MasterKey::generate();
    let k2 = MasterKey::generate();
    assert_ne!(k1.encryption_key, k2.encryption_key);
    assert_ne!(k1.chunk_id_key, k2.chunk_id_key);
}

#[test]
fn encryption_key_and_chunk_id_key_are_different() {
    let key = MasterKey::generate();
    assert_ne!(key.encryption_key, key.chunk_id_key);
}

#[test]
#[ignore] // Argon2id KDF is slow (~64 MiB memory)
fn to_encrypted_from_encrypted_roundtrip() {
    let key = MasterKey::generate();
    let passphrase = "test-passphrase-123";
    let encrypted = key.to_encrypted(passphrase).unwrap();
    let recovered = MasterKey::from_encrypted(&encrypted, passphrase).unwrap();
    assert_eq!(key.encryption_key, recovered.encryption_key);
    assert_eq!(key.chunk_id_key, recovered.chunk_id_key);
}

#[test]
#[ignore] // Argon2id KDF is slow
fn wrong_passphrase_fails_decrypt() {
    let key = MasterKey::generate();
    let encrypted = key.to_encrypted("correct").unwrap();
    let result = MasterKey::from_encrypted(&encrypted, "wrong");
    assert!(result.is_err());
}

#[test]
#[ignore] // Argon2id KDF is slow
fn encrypted_key_serde_roundtrip() {
    let key = MasterKey::generate();
    let encrypted = key.to_encrypted("pass").unwrap();
    let serialized = rmp_serde::to_vec(&encrypted).unwrap();
    let deserialized: EncryptedKey = rmp_serde::from_slice(&serialized).unwrap();
    // Verify the deserialized key can decrypt successfully
    let recovered = MasterKey::from_encrypted(&deserialized, "pass").unwrap();
    assert_eq!(key.encryption_key, recovered.encryption_key);
    assert_eq!(key.chunk_id_key, recovered.chunk_id_key);
}
