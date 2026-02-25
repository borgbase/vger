#!/usr/bin/env bash
# Sign and notarize the vger-gui macOS app bundle.
# Adapted from https://github.com/borgbase/vorta/blob/master/package/macos-package-app.sh
#
# Required environment variables:
#   CERTIFICATE_NAME    - Developer ID Application identity (for codesign)
#   APPLE_API_KEY       - Path to App Store Connect .p8 key file (for notarization)
#   APPLE_API_KEY_ID    - API key ID
#   APPLE_API_ISSUER_ID - Issuer ID from App Store Connect
#
# Usage: ./scripts/macos-sign-app.sh "path/to/Vger Backup.app"

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
APP_BUNDLE="${1:?Usage: $0 <path/to/App.app>}"

if [[ ! -d "$APP_BUNDLE" ]]; then
    echo "Error: $APP_BUNDLE is not a directory"
    exit 1
fi

# --- Sign ---
echo "==> Signing ${APP_BUNDLE}..."
codesign --verbose --force --sign "$CERTIFICATE_NAME" \
    --timestamp --deep --options runtime \
    --entitlements "${SCRIPT_DIR}/macos-entitlements.plist" \
    "$APP_BUNDLE"

echo "==> Verifying signature..."
codesign --verify --verbose "$APP_BUNDLE"

# --- Notarize ---
echo "==> Creating zip for notarization..."
ZIP_PATH="${APP_BUNDLE%.app}.zip"
ditto -c -k --keepParent "$APP_BUNDLE" "$ZIP_PATH"

echo "==> Submitting for notarization..."
xcrun notarytool submit "$ZIP_PATH" \
    --key "$APPLE_API_KEY" \
    --key-id "$APPLE_API_KEY_ID" \
    --issuer "$APPLE_API_ISSUER_ID" \
    --wait --timeout 10m

rm -f "$ZIP_PATH"

# --- Staple ---
echo "==> Stapling notarization ticket..."
xcrun stapler staple "$APP_BUNDLE"

echo "==> Done. ${APP_BUNDLE} is signed and notarized."
