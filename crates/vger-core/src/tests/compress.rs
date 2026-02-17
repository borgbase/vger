use crate::compress::{compress, decompress, decompress_with_hint, Compression};
use crate::error::VgerError;

#[test]
fn roundtrip_none() {
    let data = b"hello world, no compression";
    let compressed = compress(Compression::None, data).unwrap();
    let decompressed = decompress(&compressed).unwrap();
    assert_eq!(decompressed, data);
}

#[test]
fn roundtrip_lz4() {
    let data = b"hello world, lz4 compression test data here";
    let compressed = compress(Compression::Lz4, data).unwrap();
    let decompressed = decompress(&compressed).unwrap();
    assert_eq!(decompressed, data);
}

#[test]
fn roundtrip_zstd() {
    let data = b"hello world, zstd compression test data here";
    let compressed = compress(Compression::Zstd { level: 3 }, data).unwrap();
    let decompressed = decompress(&compressed).unwrap();
    assert_eq!(decompressed, data);
}

#[test]
fn lz4_actually_compresses() {
    // Highly repetitive data should compress well
    let data = vec![0x42u8; 10_000];
    let compressed = compress(Compression::Lz4, &data).unwrap();
    assert!(compressed.len() < data.len());
}

#[test]
fn decompress_empty_data_fails() {
    let result = decompress(b"");
    assert!(result.is_err());
    match result.unwrap_err() {
        VgerError::Decompression(msg) => assert_eq!(msg, "empty data"),
        other => panic!("expected Decompression error, got: {other}"),
    }
}

#[test]
fn decompress_unknown_tag_fails() {
    let result = decompress(&[0xFF, 0x00, 0x01]);
    assert!(result.is_err());
    match result.unwrap_err() {
        VgerError::UnknownCompressionTag(0xFF) => {}
        other => panic!("expected UnknownCompressionTag(0xFF), got: {other}"),
    }
}

#[test]
fn roundtrip_empty_payload_none() {
    let compressed = compress(Compression::None, b"").unwrap();
    let decompressed = decompress(&compressed).unwrap();
    assert_eq!(decompressed, b"");
}

#[test]
fn roundtrip_empty_payload_lz4() {
    let compressed = compress(Compression::Lz4, b"").unwrap();
    let decompressed = decompress(&compressed).unwrap();
    assert_eq!(decompressed, b"");
}

#[test]
fn roundtrip_empty_payload_zstd() {
    let compressed = compress(Compression::Zstd { level: 3 }, b"").unwrap();
    let decompressed = decompress(&compressed).unwrap();
    assert_eq!(decompressed, b"");
}

#[test]
fn from_config_valid() {
    assert_eq!(
        Compression::from_config("none", 3).unwrap(),
        Compression::None
    );
    assert_eq!(
        Compression::from_config("lz4", 3).unwrap(),
        Compression::Lz4
    );
    assert_eq!(
        Compression::from_config("zstd", 5).unwrap(),
        Compression::Zstd { level: 5 }
    );
}

#[test]
fn from_config_invalid() {
    let result = Compression::from_config("brotli", 3);
    assert!(result.is_err());
}

#[test]
fn zstd_level_change_reinit() {
    let data = b"hello world, test zstd level change reinit path";

    let compressed_1 = compress(Compression::Zstd { level: 1 }, data).unwrap();
    let decompressed_1 = decompress(&compressed_1).unwrap();
    assert_eq!(decompressed_1, data);

    let compressed_9 = compress(Compression::Zstd { level: 9 }, data).unwrap();
    let decompressed_9 = decompress(&compressed_9).unwrap();
    assert_eq!(decompressed_9, data);
}

#[test]
fn decompress_with_hint_matches_decompress() {
    let payloads: &[&[u8]] = &[
        b"",
        b"tiny",
        b"larger test payload for decompress hint checks",
    ];
    let codecs = [
        Compression::None,
        Compression::Lz4,
        Compression::Zstd { level: 3 },
    ];

    for codec in codecs {
        for payload in payloads {
            let encoded = compress(codec, payload).unwrap();
            let plain_a = decompress(&encoded).unwrap();
            let plain_b = decompress_with_hint(&encoded, Some(payload.len())).unwrap();
            assert_eq!(plain_a, plain_b);
        }
    }
}

#[test]
fn decompress_with_hint_accepts_huge_hint() {
    let payload = vec![0x7F; 4096];
    let encoded = compress(Compression::Zstd { level: 3 }, &payload).unwrap();
    let decoded = decompress_with_hint(&encoded, Some(usize::MAX)).unwrap();
    assert_eq!(decoded, payload);
}
