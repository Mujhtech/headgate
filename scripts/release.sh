#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

readonly repository="github.com/mujhtech/headgate"
readonly go_module_dirs=(
  go
  go/driver/headgatemysql
  go/driver/headgatepgx
  go/driver/headgateredis
  go/headgatecrypto
  go/headgateapi
  go/headgatectl
  go/headgatemigrate
  go/headgateotel
  go/headgatetest
  go/headgateui
  go/headgateworkflow
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
  echo "usage: $0 <check|package-rust|tag-go|index-go|publish-rust> <version>" >&2
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

  local dir actual expected crate
  for dir in "${go_module_dirs[@]}"; do
    actual=$(sed -n 's/^module //p' "$dir/go.mod")
    expected="$repository/$dir"
    if [[ $actual != "$expected" ]]; then
      echo "$dir/go.mod declares $actual, expected $expected" >&2
      exit 1
    fi

    if [[ ! -f "$dir/LICENSE" ]]; then
      echo "$dir/LICENSE is required so pkg.go.dev can display documentation" >&2
      exit 1
    fi
    if ! cmp --silent LICENSE "$dir/LICENSE"; then
      echo "$dir/LICENSE differs from the repository Apache-2.0 license" >&2
      exit 1
    fi
  done

  for crate in "${rust_crates[@]}"; do
    if [[ ! -f "crates/$crate/LICENSE" ]]; then
      echo "crates/$crate/LICENSE is required in the published crate" >&2
      exit 1
    fi
    if ! cmp --silent LICENSE "crates/$crate/LICENSE"; then
      echo "crates/$crate/LICENSE differs from the repository Apache-2.0 license" >&2
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
  [[ -f LICENSE ]] || { echo "LICENSE is required for release" >&2; exit 1; }
}

package_rust_crates() {
  local crate package_contents
  local package_args=(--locked --list)
  [[ ${RELEASE_ALLOW_DIRTY:-} == 1 ]] && package_args+=(--allow-dirty)
  for crate in "${rust_crates[@]}"; do
    echo "checking package contents: $crate $version"
    package_contents=$(cargo package "${package_args[@]}" -p "$crate")
    if ! grep --fixed-strings --line-regexp --quiet LICENSE <<<"$package_contents"; then
      echo "$crate package archive does not contain LICENSE" >&2
      exit 1
    fi
  done
}

index_go_modules() {
  local dir module attempt
  for dir in "${go_module_dirs[@]}"; do
    module=$(sed -n 's/^module //p' "$dir/go.mod")
    for attempt in 1 2 3 4 5 6; do
      echo "requesting Go proxy index: $module v$version"
      if GOWORK=off GOPROXY=https://proxy.golang.org \
        go list -m "$module@v$version"; then
        break
      fi
      if [[ $attempt == 6 ]]; then
        echo "Go proxy did not resolve $module v$version after $attempt attempts" >&2
        exit 1
      fi
      sleep 10
    done
  done
}

crate_is_published() {
  local crate=$1
  curl --fail --silent --show-error \
    --user-agent "headgate-release/$version" \
    "https://crates.io/api/v1/crates/$crate/$version" >/dev/null 2>&1
}

retry_after_seconds() {
  local output_file=$1 retry_at
  retry_at=$(sed -n \
    's/.*Please try again after \(.* GMT\) and see .*/\1/p' \
    "$output_file" | tail -n 1)
  [[ -n $retry_at ]] || return 1
  python3 -c \
    'from datetime import datetime, timezone; from email.utils import parsedate_to_datetime; import sys; target=parsedate_to_datetime(sys.argv[1]); now=datetime.now(timezone.utc); print(max(1, int((target-now).total_seconds())+5))' \
    "$retry_at"
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

  local crate attempt output_file wait_seconds
  for crate in "${rust_crates[@]}"; do
    if crate_is_published "$crate"; then
      echo "already published: $crate $version"
      continue
    fi

    for attempt in $(seq 1 20); do
      if crate_is_published "$crate"; then
        echo "published while waiting: $crate $version"
        break
      fi
      output_file=$(mktemp)
      # CI has already verified the complete workspace. --no-verify avoids rebuilding
      # packaged crates after Cargo removes their path-only cyclic dev-dependencies.
      if cargo publish --locked --no-verify -p "$crate" 2>&1 | tee "$output_file"; then
        rm -f "$output_file"
        break
      fi
      if wait_seconds=$(retry_after_seconds "$output_file"); then
        echo "crates.io asked us to wait ${wait_seconds}s before retrying $crate"
      else
        wait_seconds=10
        echo "waiting ${wait_seconds}s for crates.io to index dependencies before retrying $crate"
      fi
      rm -f "$output_file"
      if [[ $attempt == 20 ]]; then
        echo "failed to publish $crate after $attempt attempts" >&2
        exit 1
      fi
      sleep "$wait_seconds"
    done
  done
}

case "$command_name" in
  check) check_release ;;
  package-rust) check_release; package_rust_crates ;;
  tag-go) check_release; tag_go_modules ;;
  index-go) check_release; index_go_modules ;;
  publish-rust) check_release; publish_rust_crates ;;
  *) usage ;;
esac
