use crate::crypto::aes_gcm::Aes256GcmEngine;
use crate::crypto::PlaintextEngine;
use crate::error::VgerError;
use crate::repo::format::{pack_object, unpack_object, ObjectType};

#[test]
fn roundtrip_plaintext() {
    let engine = PlaintextEngine::new(&[0xAA; 32]);
    let data = b"manifest data here";
    let packed = pack_object(ObjectType::Manifest, data, &engine).unwrap();
    let (obj_type, unpacked) = unpack_object(&packed, &engine).unwrap();
    assert_eq!(obj_type, ObjectType::Manifest);
    assert_eq!(unpacked, data);
}

#[test]
fn roundtrip_encrypted() {
    let engine = Aes256GcmEngine::new(&[0x11; 32], &[0x22; 32]);
    let data = b"secret chunk data";
    let packed = pack_object(ObjectType::ChunkData, data, &engine).unwrap();
    let (obj_type, unpacked) = unpack_object(&packed, &engine).unwrap();
    assert_eq!(obj_type, ObjectType::ChunkData);
    assert_eq!(unpacked, data);
}

#[test]
fn type_tag_is_first_byte() {
    let engine = PlaintextEngine::new(&[0xAA; 32]);
    let packed = pack_object(ObjectType::Manifest, b"data", &engine).unwrap();
    assert_eq!(packed[0], ObjectType::Manifest as u8);

    let packed2 = pack_object(ObjectType::ChunkData, b"data", &engine).unwrap();
    assert_eq!(packed2[0], ObjectType::ChunkData as u8);
}

#[test]
fn wrong_type_tag_encrypted_fails_aad() {
    let engine = Aes256GcmEngine::new(&[0x11; 32], &[0x22; 32]);
    let data = b"secret";
    let mut packed = pack_object(ObjectType::Manifest, data, &engine).unwrap();
    // Change the type tag byte — AAD mismatch should cause decryption failure
    packed[0] = ObjectType::ChunkData as u8;
    let result = unpack_object(&packed, &engine);
    assert!(result.is_err());
}

#[test]
fn empty_data_fails() {
    let engine = PlaintextEngine::new(&[0xAA; 32]);
    let result = unpack_object(b"", &engine);
    assert!(result.is_err());
    match result.unwrap_err() {
        VgerError::InvalidFormat(msg) => assert_eq!(msg, "empty object"),
        other => panic!("expected InvalidFormat, got: {other}"),
    }
}

#[test]
fn unknown_type_tag_fails() {
    let engine = PlaintextEngine::new(&[0xAA; 32]);
    let result = unpack_object(&[0xFF, 0x01, 0x02], &engine);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), VgerError::UnknownObjectType(0xFF)));
}

#[test]
fn object_type_from_u8_valid() {
    assert_eq!(ObjectType::from_u8(0).unwrap(), ObjectType::Config);
    assert_eq!(ObjectType::from_u8(1).unwrap(), ObjectType::Manifest);
    assert_eq!(ObjectType::from_u8(2).unwrap(), ObjectType::SnapshotMeta);
    assert_eq!(ObjectType::from_u8(3).unwrap(), ObjectType::ChunkData);
    assert_eq!(ObjectType::from_u8(4).unwrap(), ObjectType::ChunkIndex);
    assert_eq!(ObjectType::from_u8(5).unwrap(), ObjectType::PackHeader);
}

#[test]
fn object_type_from_u8_invalid() {
    assert!(ObjectType::from_u8(6).is_err());
    assert!(ObjectType::from_u8(255).is_err());
}
