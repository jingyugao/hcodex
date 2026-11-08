#!/usr/bin/env bash
# Builds the hcodex release and optionally publishes it as a GitHub Release.
#
#   scripts/release.sh                 # build and package into target/dist
#   scripts/release.sh --publish v0.1.0
#
# - hcodex-linux-x64.tar.gz: hcodex's codex-app-server plus upstream's codex-exec-server and
#   codex-code-mode-host from the pinned codex commit (codex.rev, checked out or symlinked at
#   ./codex). Binaries sit at the archive root, so the extracted directory can be Colony's
#   `codex-artifacts` build context. Build on Ubuntu 24.04 (glibc 2.39), the runtime image's base.
# - hcodex-chat-<version>.tgz: the chat SDK as an npm package, installable from the release URL.
#
# Publishing tags the current commit, which must be pushed; the tag then triggers CI as an
# after-the-fact check.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$root"
publish=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --publish) publish="${2:?--publish needs a tag}"; shift 2 ;;
    -h | --help) sed -n '2,14p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

log() { printf '\033[1;34m==>\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

rev="$(tr -d '[:space:]' < codex.rev)"
[[ -f codex/codex-rs/Cargo.toml ]] || die "missing ./codex: clone jingyugao/codex at $rev or symlink it"
[[ "$(git -C codex rev-parse HEAD)" == "$rev" ]] || die "./codex is not at codex.rev ($rev)"
[[ -z "$(git -C codex status --porcelain -- codex-rs)" ]] || die "./codex has uncommitted changes"
commit="$(git rev-parse HEAD)"
if [[ -n $publish ]]; then
  [[ -z "$(git status --porcelain)" ]] || die "uncommitted changes; publish builds a committed tree only"
  git fetch -q origin
  [[ -n "$(git branch -r --contains "$commit")" ]] || die "push $commit before publishing"
  ! gh release view "$publish" -R jingyugao/hcodex > /dev/null 2>&1 || die "release $publish exists"
fi

# Prebuilt V8 for the Code Mode host, matching codex's Cargo.lock.
v8="$(awk '/^name = "v8"$/ { getline; gsub(/version = |"/, ""); print; exit }' codex/codex-rs/Cargo.lock)"
target=x86_64-unknown-linux-gnu
profile=ptrcomp_sandbox_release
v8_dir="${XDG_CACHE_HOME:-$HOME/.cache}/hcodex/rusty-v8-$v8"
sums="rusty_v8_${profile}_$target.sha256"
archive="librusty_v8_${profile}_$target.a.gz"
binding="src_binding_${profile}_$target.rs"
if ! (cd "$v8_dir" 2> /dev/null && sha256sum -c --quiet "$sums" 2> /dev/null); then
  log "downloading V8 $v8"
  mkdir -p "$v8_dir"
  for file in "$sums" "$archive" "$binding"; do
    curl -fsSL -o "$v8_dir/$file" \
      "https://github.com/openai/codex/releases/download/rusty-v8-v$v8/$file"
  done
  (cd "$v8_dir" && sha256sum -c --quiet "$sums")
fi
export RUSTY_V8_ARCHIVE="$v8_dir/$archive"
export RUSTY_V8_SRC_BINDING_PATH="$v8_dir/$binding"

log "building (hcodex $commit, codex $rev)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$root/target}"
export CARGO_PROFILE_RELEASE_DEBUG="${CARGO_PROFILE_RELEASE_DEBUG:-0}"  # stripped anyway
cargo build --release --locked -p hcodex-server --bin codex-app-server
cargo build --release --locked --manifest-path codex/codex-rs/Cargo.toml \
  -p codex-exec-server-cli --bin codex-exec-server
cargo build --release --locked --manifest-path codex/codex-rs/Cargo.toml \
  -p codex-code-mode-host --bin codex-code-mode-host

binaries=(codex-app-server codex-exec-server codex-code-mode-host)
package="$CARGO_TARGET_DIR/package/hcodex"
dist="$CARGO_TARGET_DIR/dist"
rm -rf "$package" "$dist"
mkdir -p "$package" "$dist"
for binary in "${binaries[@]}"; do
  install -m 0755 "$CARGO_TARGET_DIR/release/$binary" "$package/$binary"
  strip --strip-debug --strip-unneeded "$package/$binary"
done
cp LICENSE NOTICE "$package/"
built_on="$(. /etc/os-release && echo "$ID-$VERSION_ID")"
dirty=false
[[ -z "$(git status --porcelain)" ]] || dirty=true
jq -n --arg commit "$commit" --arg codex "$rev" --arg builtOn "$built_on" --argjson dirty "$dirty" \
  --arg rust "$(rustc --version | awk '{print $2}')" \
  '{repository:"jingyugao/hcodex",commit:$commit,dirty:$dirty,codexRepository:"jingyugao/codex",
    codexCommit:$codex,target:"x86_64-unknown-linux-gnu",builtOn:$builtOn,rust:$rust,
    profile:"release"}' > "$package/build-info.json"
tar -czf "$dist/hcodex-linux-x64.tar.gz" -C "$package" .

# Build the SDK in its checkout, then package compiled artifacts with the release version.
log "packing the chat SDK"
version="${publish#v}"
[[ -n $version ]] || version="0.0.0-${commit:0:7}"
sdk="$CARGO_TARGET_DIR/package/chat"
rm -rf "$sdk"
mkdir -p "$sdk"
(cd chat && npm ci --no-audit --no-fund --loglevel=error && npm run build)
cp -r chat/dist chat/sql "$sdk/"
jq --arg version "$version" '.version = $version' chat/package.json > "$sdk/package.json"
(cd "$sdk" && npm pack --ignore-scripts --pack-destination "$dist" --loglevel=error > /dev/null)
(cd "$dist" && sha256sum hcodex-linux-x64.tar.gz "hcodex-chat-$version.tgz" > SHA256SUMS)

log "smoke test"
for binary in "${binaries[@]}"; do
  echo "$binary needs: $(readelf -d "$package/$binary" | awk '/NEEDED/ {print $5}' | tr -d '[]' | xargs)"
done
"$package/codex-app-server" --version
"$package/codex-exec-server" --help > /dev/null
"$package/codex-code-mode-host" --help > /dev/null
ls -l "$dist"

[[ -n $publish ]] || exit 0
log "publishing $publish"
gh release create "$publish" -R jingyugao/hcodex --target "$commit" \
  --title "hcodex $publish (Linux x86_64, glibc)" \
  --notes "Built from $commit on $built_on; see build-info.json in the archive for the codex commit." \
  "$dist/hcodex-linux-x64.tar.gz" "$dist/hcodex-chat-$version.tgz" "$dist/SHA256SUMS"
