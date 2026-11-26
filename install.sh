#!/usr/bin/env sh
set -eu

REPO="imrmnabil/github-artifact-fetcher"
BIN="ghfetcher"
INSTALL_DIR="${GHFETCHER_INSTALL_DIR:-$HOME/.local/bin}"
VERSION="${GHFETCHER_VERSION:-}"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Error: required command '$1' not found." >&2
    exit 1
  fi
}

need_cmd curl
need_cmd tar

OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$OS" in
  linux|darwin) ;;
  *)
    echo "Unsupported OS: $OS (download Windows .zip from https://github.com/$REPO/releases)" >&2
    exit 1
    ;;
esac

case "$ARCH" in
  x86_64|amd64) ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  *)
    echo "Unsupported architecture: $ARCH" >&2
    exit 1
    ;;
esac

if [ -z "$VERSION" ]; then
  # Pick the newest published release (including pre-releases during beta)
  TAG="$(curl -fsSL "https://api.github.com/repos/$REPO/releases" \
    | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' \
    | head -n 1)"
  if [ -z "$TAG" ]; then
    echo "Error: could not determine latest release tag from GitHub." >&2
    exit 1
  fi
else
  TAG="$VERSION"
fi

VER_NO_V="${TAG#v}"
ARCHIVE="${BIN}_${VER_NO_V}_${OS}_${ARCH}.tar.gz"
BASE_URL="https://github.com/$REPO/releases/download/$TAG"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

echo "Downloading $BIN $TAG ($OS/$ARCH)..."
curl -fsSL "$BASE_URL/$ARCHIVE" -o "$TMP_DIR/$ARCHIVE"
curl -fsSL "$BASE_URL/checksums.txt" -o "$TMP_DIR/checksums.txt"

if command -v sha256sum >/dev/null 2>&1; then
  (cd "$TMP_DIR" && grep " $ARCHIVE\$" checksums.txt | sha256sum -c - >/dev/null)
elif command -v shasum >/dev/null 2>&1; then
  (cd "$TMP_DIR" && grep " $ARCHIVE\$" checksums.txt | shasum -a 256 -c - >/dev/null)
fi

tar -xzf "$TMP_DIR/$ARCHIVE" -C "$TMP_DIR"
mkdir -p "$INSTALL_DIR"
mv "$TMP_DIR/$BIN" "$INSTALL_DIR/$BIN"
chmod +x "$INSTALL_DIR/$BIN"

echo "Installed $BIN $TAG to $INSTALL_DIR/$BIN"
case ":$PATH:" in
  *":$INSTALL_DIR:"*) ;;
  *) echo "Note: add '$INSTALL_DIR' to your PATH (e.g., export PATH=\"$INSTALL_DIR:\$PATH\")" ;;
esac
