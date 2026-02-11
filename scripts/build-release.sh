#!/usr/bin/env bash
set -euo pipefail

VERSION="${1:?Usage: $0 <version-tag, e.g. v0.1.0>}"
DIST_DIR="dist"
TARGETS_ZIGBUILD=(
    x86_64-unknown-linux-gnu
    aarch64-unknown-linux-gnu
)
TARGET_NATIVE="aarch64-apple-darwin"

# Prefer rustup-managed toolchain over Homebrew
if [ -d "$HOME/.rustup/toolchains" ]; then
    RUSTUP_CARGO="$(rustup which cargo 2>/dev/null || true)"
    if [ -n "$RUSTUP_CARGO" ]; then
        TOOLCHAIN_BIN="$(dirname "$RUSTUP_CARGO")"
        export PATH="$TOOLCHAIN_BIN:$PATH"
        echo "==> Using rustup cargo: $(which cargo)"
    fi
fi

# Ensure tools are available
command -v cargo >/dev/null 2>&1 || { echo "cargo not found"; exit 1; }
command -v cargo-zigbuild >/dev/null 2>&1 || { echo "cargo-zigbuild not found — install with: cargo install cargo-zigbuild"; exit 1; }

# Install required Rust targets
for target in "${TARGETS_ZIGBUILD[@]}" "$TARGET_NATIVE"; do
    rustup target add "$target"
done

# Clean dist directory
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

# Build Linux targets with zigbuild
for target in "${TARGETS_ZIGBUILD[@]}"; do
    echo "==> Building $target (zigbuild)..."
    cargo zigbuild --release --target "$target"

    archive="borg-rs-${VERSION}-${target}.tar.gz"
    tar -czf "$DIST_DIR/$archive" -C "target/$target/release" borg-rs
    echo "    Created $DIST_DIR/$archive"
done

# Build native macOS target
echo "==> Building $TARGET_NATIVE (native)..."
cargo build --release --target "$TARGET_NATIVE"

archive="borg-rs-${VERSION}-${TARGET_NATIVE}.tar.gz"
tar -czf "$DIST_DIR/$archive" -C "target/$TARGET_NATIVE/release" borg-rs
echo "    Created $DIST_DIR/$archive"

# Generate checksums
echo "==> Generating checksums..."
cd "$DIST_DIR"
for f in *.tar.gz; do
    shasum -a 256 "$f" > "$f.sha256"
done
shasum -a 256 *.tar.gz > SHA256SUMS
cd ..

echo ""
echo "==> Release artifacts in $DIST_DIR/:"
ls -lh "$DIST_DIR"
echo ""
echo "Done. Next steps:"
echo "  git tag -a $VERSION -m \"Release $VERSION\""
echo "  git push origin main --follow-tags"
echo "  gh release create $VERSION --title \"borg-rs $VERSION\" --notes \"Initial release\" $DIST_DIR/*"
