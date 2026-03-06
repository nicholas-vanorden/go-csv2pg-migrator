# go-csv2pg-migrator

Config-driven CLI tool to migrate data from CSV files to Postgres.

## CLI Args

-config string
    Path to config file (default "config.yaml")
-dry-run
    Run without committing to database

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
