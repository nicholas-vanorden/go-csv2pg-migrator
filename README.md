# go-csv2pg-migrator

Config-driven CLI tool to migrate data from CSV files into Postgres with batching and optional table creation.

## Features

- CSV-to-Postgres loading using `pgx.CopyFrom` with configurable batch size
- Optional `CREATE TABLE IF NOT EXISTS` support
- Dry-run mode that validates transforms and prints a limited row sample
- Schema-qualified table names supported (e.g., `schema.table`)

## Requirements

- Go 1.20+ (compatible with current `go.mod`)
- Postgres 12+

## Quick Start

```bash
git clone https://github.com/nicholas-vanorden/go-csv2pg-migrator.git
cd go-csv2pg-migrator
cp config.yaml.example config.yaml
go build -o csv2pg ./cmd/migrator
./csv2pg -config config.yaml
```

## CLI Args

- `-config` Path to config file (default `config.yaml`)
- `-postgres-dsn` Postgres connection string (overrides `database.dsn` in config when provided)
- `-dry-run` Run without committing to database
- `-stop-on-error` Stop when a record fails
- `-batch-size` Table insert batch size (overrides config)
- `-create-tables` Create tables if not exist (tables created during dry-run as well)

### CLI and Config Precedence

- `-postgres-dsn` uses the CLI value if provided; otherwise it uses `database.dsn` from `config.yaml`.
- Boolean flags (`-dry-run`, `-stop-on-error`, `-create-tables`) are `true` if provided. If not provided, they use the value in `config.yaml`. If not in `config.yaml`, they default to `false`.
- `-batch-size` uses the CLI value only if provided and greater than `0`. If not provided or not greater than `0`, it uses `options.batch_size` from `config.yaml`. If neither is valid, it defaults to `1000`.

## Configuration (YAML)

### Top-Level

- `database.dsn` Postgres connection string
- `options.dry_run` `true/false`
- `options.stop_on_error` `true/false`
- `options.batch_size` number, default `1000`
- `options.create_tables_if_not_exist` `true/false`, default `false`
- `tables` list of table mappings

### Tables

- `name` table name (supports `schema.table`)
- `file` CSV file path (absolute or relative)
- `truncate_before_load` `true/false`
- `columns` mapping of target column to CSV source and transform/type
- `ignore_columns` list of CSV columns to ignore

### Columns

- `source` CSV column name
- `transform` optional transform function
- `type` Postgres column type (required when `create_tables_if_not_exist` is true)

### Example

```yaml
database:
  dsn: "postgres://user:pass@localhost:5432/mydb?sslmode=disable"

options:
  dry_run: false
  stop_on_error: false
  batch_size: 1000
  create_tables_if_not_exist: true

tables:
  - name: public.users
    file: data/users.csv
    truncate_before_load: true
    columns:
      id:
        source: USER_ID
        type: TEXT
      email:
        source: EMAIL_ADDRESS
        type: TEXT
      created_at:
        source: CREATED_DATE
        transform: date
        type: DATE
      is_active:
        source: ACTIVE_FLAG
        transform: boolean
        type: BOOLEAN
    ignore_columns:
      - LEGACY_STATUS

  - name: public.orders
    file: data/orders.csv
    truncate_before_load: true
    columns:
      id:
        source: ORDER_ID
        type: TEXT
      user_id:
        source: USER_ID
        type: TEXT
      total:
        source: TOTAL_AMOUNT
        transform: money
        type: NUMERIC(12,2)
      created_at:
        source: CREATED_DATE
        transform: date
        type: DATE
```

## Transforms

Available transform functions:

- `date` parses common formats (e.g., `YYYY-MM-DD`, `MM/DD/YYYY`, RFC3339)
- `boolean` accepts `1/0`, `t/f`, `true/false`, `y/n`, `yes/no`
- `money` accepts `$` and `,` formatting and parentheses for negatives

## Behavior Notes

- Tables load in the order listed in config.
- `create_tables_if_not_exist` uses `CREATE TABLE IF NOT EXISTS` with configured column types (executes even in dry-run mode).
- `truncate_before_load` truncates before load in non-dry-run mode only.
- Dry-run prints the first 10 rows per table and summarizes suppressed rows.
- Errors include CSV line numbers and raw values where possible.

## Build and Run

```bash
go build -o csv2pg ./cmd/migrator
./csv2pg -config config.yaml
```

## Development

```bash
go test ./...
```

## Troubleshooting

- `source column not found in CSV headers`:
  - Ensure `columns.*.source` matches the CSV header exactly.
- `transform ... failed`:
  - Confirm the raw CSV value matches the expected format for the transform.
- `column ... is missing required type`:
  - Provide `columns.*.type` when `create_tables_if_not_exist` is enabled.
