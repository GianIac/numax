#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 3 ]]; then
  echo "usage: $0 <target> <release-tag> <output-directory>" >&2
  exit 2
fi

target="$1"
release_tag="$2"
output_dir="$3"

if [[ ! "$target" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "invalid Rust target: $target" >&2
  exit 2
fi

if [[ ! "$release_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([.-][A-Za-z0-9][A-Za-z0-9.-]*)?$ ]]; then
  echo "invalid release tag: $release_tag" >&2
  exit 2
fi

command -v cargo-cyclonedx >/dev/null || {
  echo "cargo-cyclonedx is required" >&2
  exit 1
}
command -v jq >/dev/null || {
  echo "jq is required" >&2
  exit 1
}

cyclonedx_cli="${CYCLONEDX_CLI:-cyclonedx}"
if [[ "$cyclonedx_cli" == */* ]]; then
  if [[ ! -x "$cyclonedx_cli" ]]; then
    echo "CycloneDX CLI validator is required: $cyclonedx_cli" >&2
    exit 1
  fi
elif ! command -v "$cyclonedx_cli" >/dev/null; then
  echo "CycloneDX CLI validator is required: $cyclonedx_cli" >&2
  exit 1
fi

crate_version="$(sed -n 's/^version = "\(.*\)"/\1/p' crates/nx-cli/Cargo.toml | head -n1)"
tag_version="${release_tag#v}"
tag_base_version="${tag_version%%-*}"

if [[ "$tag_base_version" != "$crate_version" ]]; then
  echo "release tag base version ($tag_base_version) does not match nx-cli ($crate_version)" >&2
  exit 1
fi

if [[ -z "${SOURCE_DATE_EPOCH:-}" ]]; then
  SOURCE_DATE_EPOCH="$(git show -s --format=%ct HEAD)"
  export SOURCE_DATE_EPOCH
fi

generated="crates/nx-cli/nx_bin_${target}.cdx.json"
asset="${output_dir}/numax-${release_tag}-${target}.cdx.json"

if [[ -e "$generated" || -e "$asset" ]]; then
  echo "refusing to overwrite an existing SBOM for $target" >&2
  exit 1
fi

cleanup() {
  if [[ -f "$generated" ]]; then
    rm -f -- "$generated"
  fi
}
trap cleanup EXIT

mkdir -p "$output_dir"

cargo cyclonedx \
  --manifest-path crates/nx-cli/Cargo.toml \
  --format json \
  --describe binaries \
  --target "$target" \
  --target-in-filename \
  --spec-version 1.5

test -s "$generated"

jq -e \
  --arg crate_version "$crate_version" \
  --arg target "$target" \
  '
    .bomFormat == "CycloneDX" and
    .specVersion == "1.5" and
    .metadata.component.name == "nx" and
    .metadata.component.version == $crate_version and
    any(.metadata.tools[]?;
      .name == "cargo-cyclonedx" and .version == "0.5.9") and
    any(.metadata.properties[]?;
      .name == "cdx:rustc:sbom:target:triple" and .value == $target) and
    (.components | type == "array" and length > 0) and
    (.dependencies | type == "array" and length > 0)
  ' \
  "$generated" >/dev/null

"$cyclonedx_cli" validate \
  --input-file "$generated" \
  --input-format json \
  --input-version v1_5 \
  --fail-on-errors

mv "$generated" "$asset"
trap - EXIT
echo "validated CycloneDX SBOM: $asset"
