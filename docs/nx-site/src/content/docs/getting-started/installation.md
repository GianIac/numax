---
title: Installation
description: Install Numax on Linux, macOS, Windows or with Cargo.
---

Numax installs the `nx` CLI.

For `v0.1.0`, the recommended path is:

1. download a prebuilt binary from the GitHub Release;
2. or build/install from source with Cargo.

Package-manager installs such as Homebrew, winget and crates.io are not available yet.

---

## Requirements

For the prebuilt CLI:

- Linux, macOS or Windows;
- a terminal;
- `curl` or a browser to download the release asset.

For building modules and examples:

- [Rust](https://rustup.rs/);
- the `wasm32-unknown-unknown` target.

```bash
rustup target add wasm32-unknown-unknown
```

---

## Linux

Use the Linux x86_64 musl build:

```bash
VERSION=v0.1.0
TARGET=x86_64-unknown-linux-musl
ARCHIVE="numax-${VERSION}-${TARGET}.tar.gz"

curl -LO "https://github.com/GianIac/numax/releases/download/${VERSION}/${ARCHIVE}"
curl -LO "https://github.com/GianIac/numax/releases/download/${VERSION}/SHA256SUMS"

grep " ${ARCHIVE}$" SHA256SUMS | sha256sum -c -
tar -xzf "${ARCHIVE}"
sudo install -m 755 "numax-${VERSION}-${TARGET}/nx" /usr/local/bin/nx

nx --version
```

For ARM64 Linux, use `TARGET=aarch64-unknown-linux-musl`.

---

## macOS

Apple Silicon:

```bash
VERSION=v0.1.0
TARGET=aarch64-apple-darwin
ARCHIVE="numax-${VERSION}-${TARGET}.tar.gz"

curl -LO "https://github.com/GianIac/numax/releases/download/${VERSION}/${ARCHIVE}"
curl -LO "https://github.com/GianIac/numax/releases/download/${VERSION}/SHA256SUMS"

grep " ${ARCHIVE}$" SHA256SUMS | shasum -a 256 -c -
tar -xzf "${ARCHIVE}"
sudo install -m 755 "numax-${VERSION}-${TARGET}/nx" /usr/local/bin/nx

nx --version
```

Intel Mac:

```bash
VERSION=v0.1.0
TARGET=x86_64-apple-darwin
ARCHIVE="numax-${VERSION}-${TARGET}.tar.gz"

curl -LO "https://github.com/GianIac/numax/releases/download/${VERSION}/${ARCHIVE}"
curl -LO "https://github.com/GianIac/numax/releases/download/${VERSION}/SHA256SUMS"

grep " ${ARCHIVE}$" SHA256SUMS | shasum -a 256 -c -
tar -xzf "${ARCHIVE}"
sudo install -m 755 "numax-${VERSION}-${TARGET}/nx" /usr/local/bin/nx

nx --version
```

---

## Windows

Open PowerShell:

```powershell
$Version = "v0.1.0"
$Target = "x86_64-pc-windows-msvc"
$Archive = "numax-$Version-$Target.zip"
$Base = "https://github.com/GianIac/numax/releases/download/$Version"
$InstallDir = "$env:USERPROFILE\.numax\bin"

Invoke-WebRequest "$Base/$Archive" -OutFile $Archive
Invoke-WebRequest "$Base/SHA256SUMS" -OutFile "SHA256SUMS"

$Expected = ((Select-String -Path "SHA256SUMS" -Pattern $Archive).Line -split "\s+")[0].ToLower()
$Actual = (Get-FileHash $Archive -Algorithm SHA256).Hash.ToLower()
if ($Actual -ne $Expected) { throw "Checksum mismatch for $Archive" }

Expand-Archive $Archive -DestinationPath . -Force
New-Item -ItemType Directory -Force $InstallDir | Out-Null
Copy-Item "numax-$Version-$Target\nx.exe" "$InstallDir\nx.exe" -Force

[Environment]::SetEnvironmentVariable(
  "Path",
  [Environment]::GetEnvironmentVariable("Path", "User") + ";$InstallDir",
  "User"
)

& "$InstallDir\nx.exe" --version
```

Open a new terminal after changing `Path`.

---

## Install from source

This is the most reliable path while Numax is still early.

```bash
git clone https://github.com/GianIac/numax
cd numax
cargo install --path crates/nx-cli --locked

nx --version
```

If you only want to build the binary in the repository:

```bash
git clone https://github.com/GianIac/numax
cd numax
cargo build --release -p nx-cli --locked

./target/release/nx --version
```

---

## Build WASM modules

The CLI runs `.wasm` modules. To compile Rust guest modules, install the WASM target:

```bash
rustup target add wasm32-unknown-unknown
```

Then build an example:

```bash
cd examples/distributed_counter
cargo build --release --target wasm32-unknown-unknown
```

The generated module is:

```text
examples/distributed_counter/target/wasm32-unknown-unknown/release/distributed_counter.wasm
```

---

## Verify the install

Check the CLI:

```bash
nx --version
nx config init --output numax.toml
nx config validate --config numax.toml
```

Run a module:

```bash
nx run path/to/module.wasm --datastore-path ./nx-data
```

Enable sync by adding a listen address:

```bash
nx run path/to/module.wasm \
  --listen 0.0.0.0:9000 \
  --peer 127.0.0.1:9001 \
  --datastore-path ./node-a
```

---

## Related

- [Quickstart: 5 Minutes](/numax/getting-started/quickstart-5-min/) - run two synced nodes
- [Your First Module](/numax/getting-started/your-first-module/) - build a guest module
- [CLI reference](/numax/reference/cli/) - all `nx` commands and flags
