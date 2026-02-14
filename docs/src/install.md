# Installing

## Quick install

```bash
curl -fsSL https://vger.pages.dev/install.sh | sh
```

Or download the latest release for your platform from the [releases page](https://github.com/borgbase/vger/releases).


## Pre-built binaries

Extract the archive and place the `vger` binary somewhere on your `PATH`:

```bash
# Example for Linux/macOS
tar xzf vger-*.tar.gz
sudo cp vger /usr/local/bin/
```

For Windows CLI releases:

```powershell
Expand-Archive vger-*.zip -DestinationPath .
Move-Item .\vger.exe "$env:USERPROFILE\\bin\\vger.exe"
```

Add your chosen directory (for example, `%USERPROFILE%\bin`) to `PATH` if needed.


## Build from source

Requires Rust 1.88 or later.

```bash
git clone https://github.com/borgbase/vger.git
cd vger
cargo build --release
```

The binary is at `target/release/vger`. Copy it to a directory on your `PATH`:

```bash
cp target/release/vger /usr/local/bin/
```

## Verify installation

```bash
vger --version
```

## Next steps

- [Initialize and Set Up a Repository](init-setup.md)
