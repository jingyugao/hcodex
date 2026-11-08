#!/usr/bin/env bash
# Copies the build settings hcodex must share with the pinned codex checkout at ./codex:
#   - the [patch.*] and [workspace.lints*] tables of codex-rs/Cargo.toml, into the marked block of
#     Cargo.toml (Cargo applies only the root workspace's patches, so hcodex must repeat them)
#   - rust-toolchain.toml, clippy.toml and rustfmt.toml
#   - Cargo.lock, so hcodex builds the same dependency versions as codex (needs cargo)
# Run after changing codex.rev and checking ./codex out at that commit.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
codex_rs="$root/codex/codex-rs"
[[ -f $codex_rs/Cargo.toml ]] || {
  echo "missing $codex_rs: clone jingyugao/codex at \$(cat codex.rev) into ./codex, or symlink it" >&2
  exit 1
}
want="$(tr -d '[:space:]' < "$root/codex.rev")"
have="$(git -C "$root/codex" rev-parse HEAD 2>/dev/null || echo unknown)"
[[ $have == "$want" ]] || echo "warning: ./codex is at $have, codex.rev pins $want" >&2

python3 - "$codex_rs/Cargo.toml" "$root/Cargo.toml" <<'PY'
import re, sys
src = open(sys.argv[1]).read()
dst_path = sys.argv[2]
dst = open(dst_path).read()
heads = list(re.finditer(r'^\[([^\[\]]+)\]\s*$', src, re.M))
tables = []
for i, head in enumerate(heads):
    name = head.group(1).strip()
    if name.startswith("patch.") or name.startswith("workspace.lints"):
        end = heads[i + 1].start() if i + 1 < len(heads) else len(src)
        tables.append(src[head.start():end].rstrip() + "\n")
block = ("# BEGIN synced from codex by scripts/sync-codex.sh; do not edit by hand\n"
         + "\n".join(tables) + "# END synced from codex\n")
pattern = re.compile(r"# BEGIN synced from codex.*?# END synced from codex\n", re.S)
if not pattern.search(dst):
    sys.exit("Cargo.toml has no synced block")
open(dst_path, "w").write(pattern.sub(lambda _: block, dst))
PY
for file in rust-toolchain.toml clippy.toml rustfmt.toml; do
  cp "$codex_rs/$file" "$root/$file"
done
cp "$codex_rs/Cargo.lock" "$root/Cargo.lock"
(cd "$root" && cargo metadata --format-version 1 > /dev/null)
echo "synced from codex $have"
