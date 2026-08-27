#!/usr/bin/env python3
"""Fail on migration asset drift across drivers, Rust, and Go.

Migration SQL is embedded in independently publishable Rust and Go modules, so it must
exist below each package root. Duplication is acceptable only with a byte-identity gate:
otherwise two applications can both report schema version 1 while installing different
schemas, which makes the version table actively misleading.
"""

from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parent.parent

PAIRS = [
    (
        "Postgres driver ↔ Rust migrator up v1",
        "crates/headgate-postgres/migrations/0001_init.sql",
        "crates/headgate-migrate/migrations/postgres/0001_init.up.sql",
    ),
    (
        "MySQL driver ↔ Rust migrator up v1",
        "crates/headgate-mysql/migrations/0001_init.sql",
        "crates/headgate-migrate/migrations/mysql/0001_init.up.sql",
    ),
    (
        "Postgres Rust ↔ Go up v1",
        "crates/headgate-migrate/migrations/postgres/0001_init.up.sql",
        "go/headgatemigrate/migrations/postgres/0001_init.up.sql",
    ),
    (
        "Postgres Rust ↔ Go down v1",
        "crates/headgate-migrate/migrations/postgres/0001_init.down.sql",
        "go/headgatemigrate/migrations/postgres/0001_init.down.sql",
    ),
    (
        "MySQL Rust ↔ Go up v1",
        "crates/headgate-migrate/migrations/mysql/0001_init.up.sql",
        "go/headgatemigrate/migrations/mysql/0001_init.up.sql",
    ),
    (
        "MySQL Rust ↔ Go down v1",
        "crates/headgate-migrate/migrations/mysql/0001_init.down.sql",
        "go/headgatemigrate/migrations/mysql/0001_init.down.sql",
    ),
    (
        "Postgres driver ↔ Rust migrator up v2",
        "crates/headgate-postgres/migrations/0002_enqueue_backpressure.sql",
        "crates/headgate-migrate/migrations/postgres/0002_enqueue_backpressure.up.sql",
    ),
    (
        "MySQL driver ↔ Rust migrator up v2",
        "crates/headgate-mysql/migrations/0002_enqueue_backpressure.sql",
        "crates/headgate-migrate/migrations/mysql/0002_enqueue_backpressure.up.sql",
    ),
    (
        "Postgres Rust ↔ Go up v2",
        "crates/headgate-migrate/migrations/postgres/0002_enqueue_backpressure.up.sql",
        "go/headgatemigrate/migrations/postgres/0002_enqueue_backpressure.up.sql",
    ),
    (
        "Postgres Rust ↔ Go down v2",
        "crates/headgate-migrate/migrations/postgres/0002_enqueue_backpressure.down.sql",
        "go/headgatemigrate/migrations/postgres/0002_enqueue_backpressure.down.sql",
    ),
    (
        "MySQL Rust ↔ Go up v2",
        "crates/headgate-migrate/migrations/mysql/0002_enqueue_backpressure.up.sql",
        "go/headgatemigrate/migrations/mysql/0002_enqueue_backpressure.up.sql",
    ),
    (
        "MySQL Rust ↔ Go down v2",
        "crates/headgate-migrate/migrations/mysql/0002_enqueue_backpressure.down.sql",
        "go/headgatemigrate/migrations/mysql/0002_enqueue_backpressure.down.sql",
    ),
    (
        "Postgres driver ↔ Rust migrator up v3",
        "crates/headgate-postgres/migrations/0003_job_results.sql",
        "crates/headgate-migrate/migrations/postgres/0003_job_results.up.sql",
    ),
    (
        "MySQL driver ↔ Rust migrator up v3",
        "crates/headgate-mysql/migrations/0003_job_results.sql",
        "crates/headgate-migrate/migrations/mysql/0003_job_results.up.sql",
    ),
    (
        "Postgres Rust ↔ Go up v3",
        "crates/headgate-migrate/migrations/postgres/0003_job_results.up.sql",
        "go/headgatemigrate/migrations/postgres/0003_job_results.up.sql",
    ),
    (
        "Postgres Rust ↔ Go down v3",
        "crates/headgate-migrate/migrations/postgres/0003_job_results.down.sql",
        "go/headgatemigrate/migrations/postgres/0003_job_results.down.sql",
    ),
    (
        "MySQL Rust ↔ Go up v3",
        "crates/headgate-migrate/migrations/mysql/0003_job_results.up.sql",
        "go/headgatemigrate/migrations/mysql/0003_job_results.up.sql",
    ),
    (
        "MySQL Rust ↔ Go down v3",
        "crates/headgate-migrate/migrations/mysql/0003_job_results.down.sql",
        "go/headgatemigrate/migrations/mysql/0003_job_results.down.sql",
    ),
    (
        "Postgres driver ↔ Rust migrator up v4",
        "crates/headgate-postgres/migrations/0004_mid_run_output.sql",
        "crates/headgate-migrate/migrations/postgres/0004_mid_run_output.up.sql",
    ),
    (
        "MySQL driver ↔ Rust migrator up v4",
        "crates/headgate-mysql/migrations/0004_mid_run_output.sql",
        "crates/headgate-migrate/migrations/mysql/0004_mid_run_output.up.sql",
    ),
    (
        "Postgres Rust ↔ Go up v4",
        "crates/headgate-migrate/migrations/postgres/0004_mid_run_output.up.sql",
        "go/headgatemigrate/migrations/postgres/0004_mid_run_output.up.sql",
    ),
    (
        "Postgres Rust ↔ Go down v4",
        "crates/headgate-migrate/migrations/postgres/0004_mid_run_output.down.sql",
        "go/headgatemigrate/migrations/postgres/0004_mid_run_output.down.sql",
    ),
    (
        "MySQL Rust ↔ Go up v4",
        "crates/headgate-migrate/migrations/mysql/0004_mid_run_output.up.sql",
        "go/headgatemigrate/migrations/mysql/0004_mid_run_output.up.sql",
    ),
    (
        "MySQL Rust ↔ Go down v4",
        "crates/headgate-migrate/migrations/mysql/0004_mid_run_output.down.sql",
        "go/headgatemigrate/migrations/mysql/0004_mid_run_output.down.sql",
    ),
    (
        "Postgres driver ↔ Rust migrator up v5",
        "crates/headgate-postgres/migrations/0005_job_progress.sql",
        "crates/headgate-migrate/migrations/postgres/0005_job_progress.up.sql",
    ),
    (
        "MySQL driver ↔ Rust migrator up v5",
        "crates/headgate-mysql/migrations/0005_job_progress.sql",
        "crates/headgate-migrate/migrations/mysql/0005_job_progress.up.sql",
    ),
    (
        "Postgres Rust ↔ Go up v5",
        "crates/headgate-migrate/migrations/postgres/0005_job_progress.up.sql",
        "go/headgatemigrate/migrations/postgres/0005_job_progress.up.sql",
    ),
    (
        "Postgres Rust ↔ Go down v5",
        "crates/headgate-migrate/migrations/postgres/0005_job_progress.down.sql",
        "go/headgatemigrate/migrations/postgres/0005_job_progress.down.sql",
    ),
    (
        "MySQL Rust ↔ Go up v5",
        "crates/headgate-migrate/migrations/mysql/0005_job_progress.up.sql",
        "go/headgatemigrate/migrations/mysql/0005_job_progress.up.sql",
    ),
    (
        "MySQL Rust ↔ Go down v5",
        "crates/headgate-migrate/migrations/mysql/0005_job_progress.down.sql",
        "go/headgatemigrate/migrations/mysql/0005_job_progress.down.sql",
    ),
    (
        "Postgres driver ↔ Rust migrator up v6",
        "crates/headgate-postgres/migrations/0006_periodic_origin.sql",
        "crates/headgate-migrate/migrations/postgres/0006_periodic_origin.up.sql",
    ),
    (
        "MySQL driver ↔ Rust migrator up v6",
        "crates/headgate-mysql/migrations/0006_periodic_origin.sql",
        "crates/headgate-migrate/migrations/mysql/0006_periodic_origin.up.sql",
    ),
    (
        "Postgres Rust ↔ Go up v6",
        "crates/headgate-migrate/migrations/postgres/0006_periodic_origin.up.sql",
        "go/headgatemigrate/migrations/postgres/0006_periodic_origin.up.sql",
    ),
    (
        "Postgres Rust ↔ Go down v6",
        "crates/headgate-migrate/migrations/postgres/0006_periodic_origin.down.sql",
        "go/headgatemigrate/migrations/postgres/0006_periodic_origin.down.sql",
    ),
    (
        "MySQL Rust ↔ Go up v6",
        "crates/headgate-migrate/migrations/mysql/0006_periodic_origin.up.sql",
        "go/headgatemigrate/migrations/mysql/0006_periodic_origin.up.sql",
    ),
    (
        "MySQL Rust ↔ Go down v6",
        "crates/headgate-migrate/migrations/mysql/0006_periodic_origin.down.sql",
        "go/headgatemigrate/migrations/mysql/0006_periodic_origin.down.sql",
    ),
    (
        "Postgres driver ↔ Rust migrator up v7",
        "crates/headgate-postgres/migrations/0007_scheduler_events.sql",
        "crates/headgate-migrate/migrations/postgres/0007_scheduler_events.up.sql",
    ),
    (
        "MySQL driver ↔ Rust migrator up v7",
        "crates/headgate-mysql/migrations/0007_scheduler_events.sql",
        "crates/headgate-migrate/migrations/mysql/0007_scheduler_events.up.sql",
    ),
    (
        "Postgres Rust ↔ Go up v7",
        "crates/headgate-migrate/migrations/postgres/0007_scheduler_events.up.sql",
        "go/headgatemigrate/migrations/postgres/0007_scheduler_events.up.sql",
    ),
    (
        "Postgres Rust ↔ Go down v7",
        "crates/headgate-migrate/migrations/postgres/0007_scheduler_events.down.sql",
        "go/headgatemigrate/migrations/postgres/0007_scheduler_events.down.sql",
    ),
    (
        "MySQL Rust ↔ Go up v7",
        "crates/headgate-migrate/migrations/mysql/0007_scheduler_events.up.sql",
        "go/headgatemigrate/migrations/mysql/0007_scheduler_events.up.sql",
    ),
    (
        "MySQL Rust ↔ Go down v7",
        "crates/headgate-migrate/migrations/mysql/0007_scheduler_events.down.sql",
        "go/headgatemigrate/migrations/mysql/0007_scheduler_events.down.sql",
    ),
    (
        "Postgres driver ↔ Rust migrator up v8",
        "crates/headgate-postgres/migrations/0008_pending_tags_metrics.sql",
        "crates/headgate-migrate/migrations/postgres/0008_pending_tags_metrics.up.sql",
    ),
    (
        "MySQL driver ↔ Rust migrator up v8",
        "crates/headgate-mysql/migrations/0008_pending_tags_metrics.sql",
        "crates/headgate-migrate/migrations/mysql/0008_pending_tags_metrics.up.sql",
    ),
    (
        "Postgres Rust ↔ Go up v8",
        "crates/headgate-migrate/migrations/postgres/0008_pending_tags_metrics.up.sql",
        "go/headgatemigrate/migrations/postgres/0008_pending_tags_metrics.up.sql",
    ),
    (
        "Postgres Rust ↔ Go down v8",
        "crates/headgate-migrate/migrations/postgres/0008_pending_tags_metrics.down.sql",
        "go/headgatemigrate/migrations/postgres/0008_pending_tags_metrics.down.sql",
    ),
    (
        "MySQL Rust ↔ Go up v8",
        "crates/headgate-migrate/migrations/mysql/0008_pending_tags_metrics.up.sql",
        "go/headgatemigrate/migrations/mysql/0008_pending_tags_metrics.up.sql",
    ),
    (
        "MySQL Rust ↔ Go down v8",
        "crates/headgate-migrate/migrations/mysql/0008_pending_tags_metrics.down.sql",
        "go/headgatemigrate/migrations/mysql/0008_pending_tags_metrics.down.sql",
    ),
    (
        "Postgres driver ↔ Rust migrator up v9",
        "crates/headgate-postgres/migrations/0009_pending_tags_metrics.sql",
        "crates/headgate-migrate/migrations/postgres/0009_pending_tags_metrics.up.sql",
    ),
    (
        "MySQL driver ↔ Rust migrator up v9",
        "crates/headgate-mysql/migrations/0009_pending_tags_metrics.sql",
        "crates/headgate-migrate/migrations/mysql/0009_pending_tags_metrics.up.sql",
    ),
    (
        "Postgres Rust ↔ Go up v9",
        "crates/headgate-migrate/migrations/postgres/0009_pending_tags_metrics.up.sql",
        "go/headgatemigrate/migrations/postgres/0009_pending_tags_metrics.up.sql",
    ),
    (
        "Postgres Rust ↔ Go down v9",
        "crates/headgate-migrate/migrations/postgres/0009_pending_tags_metrics.down.sql",
        "go/headgatemigrate/migrations/postgres/0009_pending_tags_metrics.down.sql",
    ),
    (
        "MySQL Rust ↔ Go up v9",
        "crates/headgate-migrate/migrations/mysql/0009_pending_tags_metrics.up.sql",
        "go/headgatemigrate/migrations/mysql/0009_pending_tags_metrics.up.sql",
    ),
    (
        "MySQL Rust ↔ Go down v9",
        "crates/headgate-migrate/migrations/mysql/0009_pending_tags_metrics.down.sql",
        "go/headgatemigrate/migrations/mysql/0009_pending_tags_metrics.down.sql",
    ),
    (
        "Postgres driver ↔ Rust migrator up v10",
        "crates/headgate-postgres/migrations/0010_sticky_routing.sql",
        "crates/headgate-migrate/migrations/postgres/0010_sticky_routing.up.sql",
    ),
    (
        "MySQL driver ↔ Rust migrator up v10",
        "crates/headgate-mysql/migrations/0010_sticky_routing.sql",
        "crates/headgate-migrate/migrations/mysql/0010_sticky_routing.up.sql",
    ),
    (
        "Postgres Rust ↔ Go up v10",
        "crates/headgate-migrate/migrations/postgres/0010_sticky_routing.up.sql",
        "go/headgatemigrate/migrations/postgres/0010_sticky_routing.up.sql",
    ),
    (
        "Postgres Rust ↔ Go down v10",
        "crates/headgate-migrate/migrations/postgres/0010_sticky_routing.down.sql",
        "go/headgatemigrate/migrations/postgres/0010_sticky_routing.down.sql",
    ),
    (
        "MySQL Rust ↔ Go up v10",
        "crates/headgate-migrate/migrations/mysql/0010_sticky_routing.up.sql",
        "go/headgatemigrate/migrations/mysql/0010_sticky_routing.up.sql",
    ),
    (
        "MySQL Rust ↔ Go down v10",
        "crates/headgate-migrate/migrations/mysql/0010_sticky_routing.down.sql",
        "go/headgatemigrate/migrations/mysql/0010_sticky_routing.down.sql",
    ),
    (
        "Postgres driver ↔ Rust migrator up v11",
        "crates/headgate-postgres/migrations/0011_partitioned_archive.sql",
        "crates/headgate-migrate/migrations/postgres/0011_partitioned_archive.up.sql",
    ),
    (
        "MySQL driver ↔ Rust migrator up v11",
        "crates/headgate-mysql/migrations/0011_partitioned_archive.sql",
        "crates/headgate-migrate/migrations/mysql/0011_partitioned_archive.up.sql",
    ),
    (
        "Postgres Rust ↔ Go up v11",
        "crates/headgate-migrate/migrations/postgres/0011_partitioned_archive.up.sql",
        "go/headgatemigrate/migrations/postgres/0011_partitioned_archive.up.sql",
    ),
    (
        "Postgres Rust ↔ Go down v11",
        "crates/headgate-migrate/migrations/postgres/0011_partitioned_archive.down.sql",
        "go/headgatemigrate/migrations/postgres/0011_partitioned_archive.down.sql",
    ),
    (
        "MySQL Rust ↔ Go up v11",
        "crates/headgate-migrate/migrations/mysql/0011_partitioned_archive.up.sql",
        "go/headgatemigrate/migrations/mysql/0011_partitioned_archive.up.sql",
    ),
    (
        "MySQL Rust ↔ Go down v11",
        "crates/headgate-migrate/migrations/mysql/0011_partitioned_archive.down.sql",
        "go/headgatemigrate/migrations/mysql/0011_partitioned_archive.down.sql",
    ),
]


def versions(directory: str, direction: str) -> list[int]:
    found = []
    for path in (ROOT / directory).glob(f"*.{direction}.sql"):
        match = re.fullmatch(r"(\d{4})_[a-z0-9_]+\.(?:up|down)\.sql", path.name)
        if not match:
            print(f"FAIL: malformed migration filename: {path.relative_to(ROOT)}")
            sys.exit(1)
        found.append(int(match.group(1)))
    return sorted(found)


def main() -> int:
    failed = False
    for label, left_name, right_name in PAIRS:
        left, right = ROOT / left_name, ROOT / right_name
        if not left.exists() or not right.exists():
            print(f"FAIL: {label}: missing {left_name if not left.exists() else right_name}")
            failed = True
        elif left.read_bytes() != right.read_bytes():
            print(f"FAIL: {label}: bytes differ ({left_name} vs {right_name})")
            failed = True
        else:
            print(f"  ok {label}")

    for backend in ("postgres", "mysql"):
        rust_dir = f"crates/headgate-migrate/migrations/{backend}"
        go_dir = f"go/headgatemigrate/migrations/{backend}"
        rust_up, rust_down = versions(rust_dir, "up"), versions(rust_dir, "down")
        go_up, go_down = versions(go_dir, "up"), versions(go_dir, "down")
        expected = list(range(1, len(rust_up) + 1))
        if rust_up != expected or rust_down != expected:
            print(
                f"FAIL: {backend}: Rust versions must be contiguous and have both directions; "
                f"up={rust_up} down={rust_down}"
            )
            failed = True
        if go_up != rust_up or go_down != rust_down:
            print(
                f"FAIL: {backend}: Go version set differs; "
                f"rust={rust_up}/{rust_down} go={go_up}/{go_down}"
            )
            failed = True

    cargo = (ROOT / "Cargo.toml").read_text()
    gowork = (ROOT / "go/go.work").read_text()
    if '"crates/headgate-migrate"' not in cargo:
        print("FAIL: headgate-migrate is not a Cargo workspace member")
        failed = True
    if "./headgatemigrate" not in gowork:
        print("FAIL: headgatemigrate is not a Go workspace module")
        failed = True

    if failed:
        return 1
    print("migration asset gate: 2 backends, 2 languages, byte-identical; versions contiguous")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
