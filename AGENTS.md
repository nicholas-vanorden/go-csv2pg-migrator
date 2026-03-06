# AGENTS.md

## Purpose

This repository contains a golang CLI tool to move data from CSV files into a Postgres database.

## Scope

- Primary runtime file: `cmd/migrator/main.go`
- Documentation: `README.md`

## CLI Arguments

- -config (path to config file, default: "config.yaml")
- -dry-run (Run without committing to database, default: false)

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

## Editing Guardrails

- Keep changes minimal and consistent with current style.
- If changing configuration shape, update both README.md and AGENTS.md.

## Validation Checklist (before merge)

1. Golang syntax check passes for all .go files.
