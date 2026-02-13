use crate::crypto::CryptoEngine;
use crate::error::{Result, VgerError};

/// Object type tags for the repo envelope format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ObjectType {
    Config = 0,
    Manifest = 1,
    SnapshotMeta = 2,
    ChunkData = 3,
    ChunkIndex = 4,
    PackHeader = 5,
    FileCache = 6,
}

impl ObjectType {
    pub fn from_u8(v: u8) -> Result<Self> {
        match v {
            0 => Ok(Self::Config),
            1 => Ok(Self::Manifest),
            2 => Ok(Self::SnapshotMeta),
            3 => Ok(Self::ChunkData),
            4 => Ok(Self::ChunkIndex),
            5 => Ok(Self::PackHeader),
            6 => Ok(Self::FileCache),
            _ => Err(VgerError::UnknownObjectType(v)),
        }
    }
}

/// Serialize a typed payload into an encrypted repo object.
///
/// Wire format (encrypted): `[1-byte type_tag][encrypted_blob]`
///   where encrypted_blob = `[12-byte nonce][ciphertext + 16-byte GCM tag]`
///
/// Wire format (plaintext): `[1-byte type_tag][plaintext]`
pub fn pack_object(
    obj_type: ObjectType,
    plaintext: &[u8],
    crypto: &dyn CryptoEngine,
) -> Result<Vec<u8>> {
    let tag = obj_type as u8;
    let aad = [tag]; // authenticate the type tag
    let encrypted = crypto.encrypt(plaintext, &aad)?;

    let mut out = Vec::with_capacity(1 + encrypted.len());
    out.push(tag);
    out.extend_from_slice(&encrypted);
    Ok(out)
}

/// Deserialize and decrypt a repo object.
/// Returns `(object_type, plaintext)`.
pub fn unpack_object(data: &[u8], crypto: &dyn CryptoEngine) -> Result<(ObjectType, Vec<u8>)> {
    if data.is_empty() {
        return Err(VgerError::InvalidFormat("empty object".into()));
    }
    let tag = data[0];
    let obj_type = ObjectType::from_u8(tag)?;
    let aad = [tag];
    let plaintext = crypto.decrypt(&data[1..], &aad)?;
    Ok((obj_type, plaintext))
}

/// Deserialize and decrypt a repo object, ensuring its type tag matches.
pub fn unpack_object_expect(
    data: &[u8],
    expected_type: ObjectType,
    crypto: &dyn CryptoEngine,
) -> Result<Vec<u8>> {
    let (obj_type, plaintext) = unpack_object(data, crypto)?;
    if obj_type != expected_type {
        return Err(VgerError::InvalidFormat(format!(
            "unexpected object type: expected {:?}, got {:?}",
            expected_type, obj_type
        )));
    }
    Ok(plaintext)
}
