#!/bin/bash
# Generate AppIcon.icns from a source image (SVG or PNG) using macOS built-in tools.
# SVG input requires rsvg-convert (brew install librsvg).
# Usage: ./create-icns.sh <source> <output.icns>
set -euo pipefail

SRC="${1:?Usage: create-icns.sh <source.svg|png> <output.icns>}"
OUT="${2:?Usage: create-icns.sh <source.svg|png> <output.icns>}"

TMPDIR=$(mktemp -d)
ICONSET="$TMPDIR/AppIcon.iconset"
mkdir -p "$ICONSET"

# If SVG, rasterise to a 1024x1024 PNG first
if [[ "$SRC" == *.svg ]]; then
    if ! command -v rsvg-convert &>/dev/null; then
        echo "Error: rsvg-convert not found. Install with: brew install librsvg" >&2
        exit 1
    fi
    SRC_PNG="$TMPDIR/source.png"
    rsvg-convert -w 1024 -h 1024 "$SRC" -o "$SRC_PNG"
    SRC="$SRC_PNG"
fi

for SIZE in 16 32 128 256 512; do
    sips -z $SIZE $SIZE "$SRC" --out "$ICONSET/icon_${SIZE}x${SIZE}.png" >/dev/null
    DOUBLE=$((SIZE * 2))
    sips -z $DOUBLE $DOUBLE "$SRC" --out "$ICONSET/icon_${SIZE}x${SIZE}@2x.png" >/dev/null
done

iconutil -c icns "$ICONSET" -o "$OUT"
rm -rf "$TMPDIR"
echo "Created $OUT"
