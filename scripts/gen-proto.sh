#!/usr/bin/env bash
# Regenerate the checked-in protobuf code for both languages from proto/headgate.proto.
# Downstream builds never need protoc (AGENTS.md Phase 1 step 3) — run this only when
# the .proto changes, and commit the output.
#
# Requires: protoc, protoc-gen-go
# (go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12), and cargo (a
# throwaway prost-build generator is built in a temp dir).
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$PATH:$(go env GOPATH 2>/dev/null || echo "$HOME/go")/bin"

echo "== go =="
protoc --proto_path=proto \
  --go_out=go --go_opt=module=github.com/mujhtech/headgate/go \
  proto/headgate.proto
# protoc and protoc-gen-go versions are build-environment details, not part of the
# generated API. Removing their banner keeps checked-in output byte-stable across the
# supported local and CI protoc distributions while preserving every generated symbol.
GO_PROTO=go/proto/headgatev1/headgate.pb.go
GO_PROTO_NORMALIZED=$(mktemp)
awk '
  $0 == "// versions:" { skipping_versions = 1; next }
  skipping_versions && /^\/\/ source:/ { skipping_versions = 0 }
  !skipping_versions { print }
' "$GO_PROTO" > "$GO_PROTO_NORMALIZED"
mv "$GO_PROTO_NORMALIZED" "$GO_PROTO"
echo " ok go/proto/headgatev1/headgate.pb.go"

echo "== rust =="
GEN_DIR=$(mktemp -d)
trap 'rm -rf "$GEN_DIR"' EXIT
mkdir -p "$GEN_DIR/src"
cat > "$GEN_DIR/Cargo.toml" <<'EOF'
[package]
name = "hg-proto-gen"
version = "0.0.0"
edition = "2021"
[dependencies]
prost-build = "0.13"
EOF
cat > "$GEN_DIR/src/main.rs" <<EOF
fn main() {
    let out = std::env::args().nth(1).expect("out dir");
    let repo = std::env::args().nth(2).expect("repo root");
    let mut cfg = prost_build::Config::new();
    cfg.out_dir(&out);
    cfg.compile_protos(
        &[format!("{repo}/proto/headgate.proto")],
        &[format!("{repo}/proto")],
    )
    .expect("prost");
}
EOF
REPO="$(pwd)"
(cd "$GEN_DIR" && PROTOC="$(command -v protoc)" cargo run -q -- "$GEN_DIR" "$REPO")
mkdir -p crates/headgate-proto/src
cp "$GEN_DIR/headgate.v1.rs" crates/headgate-proto/src/headgate.v1.rs
echo " ok crates/headgate-proto/src/headgate.v1.rs"
