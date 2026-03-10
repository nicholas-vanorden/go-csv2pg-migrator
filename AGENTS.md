# AGENTS.md

## Quick Summary

- Config-driven CLI to load CSV files into Postgres using `pgx.CopyFrom`.
- Dry-run parses/transforms rows and prints a limited sample; it does not write or truncate.
- Optional table creation via config/flag requires column types.

## Purpose

This repository contains a golang CLI tool to move data from CSV files into a Postgres database.

## Scope

- Primary runtime file: `cmd/migrator/main.go`
- Documentation: `README.md`
- Config parsing: `internal/config/config.go`
- Load pipeline: `internal/loader/runner.go`, `internal/loader/table_loader.go`
- Transforms: `internal/transform/registry.go`

## CLI Arguments

- -config (path to config file, default: "config.yaml")
- -dry-run (Run without committing to database, default: false)
- -stop-on-error (Stop when a record fails, default: false)
- -batch-size (Table insert batch size, default: 1000)
- -create-tables (Create tables if not exist, tables created during dry-run as well, default: false)

## Configuration (YAML)

- database:
  - dsn: postgres connection string
- options:
  - dry_run: true/false
  - stop_on_error: true/false
  - batch_size: number, default: 1000
  - create_tables_if_not_exist: true/false, default: false
- tables: (can be 1 to many)
  - name: table name
  - file: csv file
  - truncate_before_load: true/false
  - columns: (can be 1 to many)
    - column_name:
      - source: csv_column_name
      - transform: (optional transform function)
      - type: postgres column type (required when create_tables_if_not_exist is true)
  - ignore_columns (can be 0 to many)
    - csv_column_name

## Runtime Behavior

- Tables are processed in the order defined in config (define parent tables before dependent tables).
- If `create_tables_if_not_exist` is true, `CREATE TABLE IF NOT EXISTS` is executed before loading.
- If `truncate_before_load` is true and not in dry-run, the table is truncated inside a transaction.
- Loads use `pgx.CopyFrom` with `batch_size` rows per batch.
- Schema-qualified table names (e.g., `schema.table`) are supported.
- Errors include CSV line numbers and raw values where relevant.

## Transforms

- `date`: parses common date formats (e.g., `YYYY-MM-DD`, `MM/DD/YYYY`, RFC3339).
- `boolean`: accepts `1/0`, `t/f`, `true/false`, `y/n`, `yes/no`.
- `money`: accepts `$` and `,` formatting and parentheses for negatives.

## Data Requirements

- CSV header names must match `columns.*.source`.
- A column must exist for each configured `columns` mapping.
- When table creation is enabled, every configured column must include a `type`.

## Known Limitations

- Non-dry-run inserts rely solely on `CopyFrom` (no per-row fallback).
- Table creation does not add PK/FK/index/constraints beyond column definitions.
- Multi-part identifiers are treated as `pgx.Identifier` segments, not quoted as a single name.

## Editing Guardrails

- Keep changes minimal and consistent with current style.
- If changing configuration shape, update both README.md and AGENTS.md.

## Validation Checklist (before merge)

1. Golang syntax check passes for all .go files.
