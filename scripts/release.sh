#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

readonly repository="github.com/mujhtech/headgate"
readonly go_module_dirs=(
  go
  go/driver/headgatemysql
  go/driver/headgatepgx
  go/driver/headgateredis
  go/headgateapi
  go/headgatectl
  go/headgatemigrate
  go/headgateotel
  go/headgatetest
  go/headgateui
)
readonly rust_crates=(
  headgate-core
  headgate-macros
  headgate-proto
  headgate-sql
  headgate-ui
  headgate-otel
  headgate-migrate
  headgate
  headgate-testkit
  headgate-postgres
  headgate-mysql
  headgate-redis
  headgate-workflow
  headgate-crypto
  headgate-api
)

usage() {
  echo "usage: $0 <check|package-cli|package-rust|tag-go|publish-rust> <version> [output-dir]" >&2
  exit 2
}

[[ $# -ge 2 ]] || usage
command_name=$1
version=${2#v}

if [[ ! $version =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?$ ]]; then
  echo "invalid release version: $2" >&2
  exit 2
fi

check_release() {
  local workspace_version
  workspace_version=$(cargo metadata --no-deps --format-version 1 |
    python3 -c 'import json,sys; versions={p["version"] for p in json.load(sys.stdin)["packages"]}; print(next(iter(versions))) if len(versions)==1 else sys.exit("workspace crates do not share one version: "+", ".join(sorted(versions)))')
  if [[ $workspace_version != "$version" ]]; then
    echo "workspace version is $workspace_version, release version is $version" >&2
    exit 1
  fi

  local dir actual expected
  for dir in "${go_module_dirs[@]}"; do
    actual=$(sed -n 's/^module //p' "$dir/go.mod")
    expected="$repository/$dir"
    if [[ $actual != "$expected" ]]; then
      echo "$dir/go.mod declares $actual, expected $expected" >&2
      exit 1
    fi
  done

  local bad_versions
  bad_versions=$(find go -name go.mod -print0 |
    xargs -0 awk -v expected="v$version" '
      $1 ~ /^github.com\/mujhtech\/headgate\/go(\/|$)/ && $2 ~ /^v/ && $2 != expected {
        print FILENAME ":" FNR ": " $1 " requires " $2 ", expected " expected
      }
    ')
  if [[ -n $bad_versions ]]; then
    printf '%s\n' "$bad_versions" >&2
    exit 1
  fi

  [[ -f README.md ]] || { echo "README.md is required for release" >&2; exit 1; }
  [[ -f LICENSE-MIT ]] || { echo "LICENSE-MIT is required for release" >&2; exit 1; }
  [[ -f LICENSE-APACHE ]] || { echo "LICENSE-APACHE is required for release" >&2; exit 1; }
}

package_cli() {
  local output_dir=${3:-dist}
  local target os arch binary archive stage
  local targets=(
    linux/amd64
    linux/arm64
    darwin/amd64
    darwin/arm64
    windows/amd64
    windows/arm64
  )

  mkdir -p "$output_dir"
  for target in "${targets[@]}"; do
    os=${target%/*}
    arch=${target#*/}
    binary=headgatectl
    [[ $os == windows ]] && binary=headgatectl.exe
    stage="$output_dir/headgatectl_${version}_${os}_${arch}"
    mkdir -p "$stage"
    (
      cd go/headgatectl
      CGO_ENABLED=0 GOOS=$os GOARCH=$arch go build -trimpath -ldflags='-s -w' -o "../../$stage/$binary" .
    )
    if [[ $os == windows ]]; then
      archive="${stage}.zip"
      (cd "$stage" && zip -q -r "../$(basename "$archive")" .)
    else
      archive="${stage}.tar.gz"
      tar -C "$stage" -czf "$archive" .
    fi
    rm -r "$stage"
  done
  if command -v sha256sum >/dev/null 2>&1; then
    (cd "$output_dir" && sha256sum ./*.tar.gz ./*.zip > checksums.txt)
  else
    (cd "$output_dir" && shasum -a 256 ./*.tar.gz ./*.zip > checksums.txt)
  fi
}

package_rust_crates() {
  local crate
  local package_args=(--locked --list)
  [[ ${RELEASE_ALLOW_DIRTY:-} == 1 ]] && package_args+=(--allow-dirty)
  for crate in "${rust_crates[@]}"; do
    echo "checking package contents: $crate $version"
    cargo package "${package_args[@]}" -p "$crate" >/dev/null
  done
}

tag_go_modules() {
  local dir tag existing
  local pending=()
  git config user.name "github-actions[bot]"
  git config user.email "41898282+github-actions[bot]@users.noreply.github.com"

  for dir in "${go_module_dirs[@]}"; do
    tag="$dir/v$version"
    existing=$(git rev-list -n 1 "$tag" 2>/dev/null || true)
    if [[ -n $existing ]]; then
      if [[ $existing != "$GITHUB_SHA" ]]; then
        echo "$tag already points to $existing, not $GITHUB_SHA" >&2
        exit 1
      fi
      echo "already tagged: $tag"
      continue
    fi
    git tag -a "$tag" "$GITHUB_SHA" -m "headgate Go modules v$version"
    pending+=("refs/tags/$tag")
  done

  if ((${#pending[@]})); then
    git push --atomic origin "${pending[@]}"
  fi
}

publish_rust_crates() {
  [[ -n ${CARGO_REGISTRY_TOKEN:-} ]] || {
    echo "CARGO_REGISTRY_TOKEN is required to publish Rust crates" >&2
    exit 1
  }

  local crate attempt
  for crate in "${rust_crates[@]}"; do
    if curl --fail --silent --show-error \
      --user-agent "headgate-release/$version" \
      "https://crates.io/api/v1/crates/$crate/$version" >/dev/null 2>&1; then
      echo "already published: $crate $version"
      continue
    fi

    for attempt in 1 2 3 4 5 6; do
      # CI has already verified the complete workspace. --no-verify avoids rebuilding
      # packaged crates after Cargo removes their path-only cyclic dev-dependencies.
      if cargo publish --locked --no-verify -p "$crate"; then
        break
      fi
      if [[ $attempt == 6 ]]; then
        echo "failed to publish $crate after $attempt attempts" >&2
        exit 1
      fi
      echo "waiting for crates.io to index dependencies before retrying $crate"
      sleep 10
    done
  done
}

case "$command_name" in
  check) check_release ;;
  package-cli) check_release; package_cli "$@" ;;
  package-rust) check_release; package_rust_crates ;;
  tag-go) check_release; tag_go_modules ;;
  publish-rust) check_release; publish_rust_crates ;;
  *) usage ;;
esac
