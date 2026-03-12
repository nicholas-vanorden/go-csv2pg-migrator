package loader

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/nicholas-vanorden/go-csv2pg-migrator/internal/config"

	"github.com/jackc/pgx/v5/pgxpool"
)

var allowedColumnType = regexp.MustCompile(`(?i)^(smallint|integer|bigint|numeric(\(\d+\s*(,\s*\d+)?\))?|real|double precision|text|varchar(\(\d+\))?|boolean|date|timestamp(\s+with(out)?\s+time\s+zone)?)$`)

type Runner struct {
	cfg *config.Config
}

func NewRunner(cfg *config.Config) *Runner {
	return &Runner{cfg: cfg}
}

func (r *Runner) Run(ctx context.Context) error {
	pool, err := pgxpool.New(ctx, r.cfg.Database.DSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	for _, table := range r.cfg.Tables {
		log.Printf("Loading table: %s\n", table.Name)

		if r.cfg.Options.CreateTablesIfNotExist {
			if err := r.createTableIfNotExists(ctx, pool, table); err != nil {
				if r.cfg.Options.StopOnError {
					return err
				}
				log.Printf("Error creating table %s: %v\n", table.Name, err)
				continue
			}
		}

		loader := NewTableLoader(pool, r.cfg, table)
		if err := loader.Load(ctx); err != nil {
			if r.cfg.Options.StopOnError {
				return err
			}
			log.Printf("Error loading %s: %v\n", table.Name, err)
		}
	}

	return nil
}

func (r *Runner) createTableIfNotExists(ctx context.Context, pool *pgxpool.Pool, table config.TableConfig) error {
	if len(table.Columns) == 0 {
		return fmt.Errorf("table %q has no columns configured", table.Name)
	}

	columnNames := make([]string, 0, len(table.Columns))
	for name := range table.Columns {
		columnNames = append(columnNames, name)
	}
	sort.Strings(columnNames)

	columnDefs := make([]string, 0, len(columnNames))
	primaryKeyCount := 0
	for _, colName := range columnNames {
		colCfg := table.Columns[colName]
		colType := strings.TrimSpace(colCfg.Type)
		if colType == "" {
			return fmt.Errorf("column %q in table %q is missing required type when create_tables_if_not_exist is enabled", colName, table.Name)
		}
		if !allowedColumnType.MatchString(colType) {
			return fmt.Errorf("column %q in table %q has unsupported type %q", colName, table.Name, colType)
		}
		columnDef := fmt.Sprintf("%s %s", pgx.Identifier{colName}.Sanitize(), colType)
		if colCfg.PrimaryKey {
			primaryKeyCount++
			columnDef = fmt.Sprintf("%s PRIMARY KEY", columnDef)
		}
		if colCfg.ForeignKey != nil {
			refTableID, err := tableIdentifier(colCfg.ForeignKey.Table)
			if err != nil {
				return fmt.Errorf("column %q in table %q has invalid foreign_key table: %w", colName, table.Name, err)
			}
			refColumn := strings.TrimSpace(colCfg.ForeignKey.Column)
			if refColumn == "" {
				return fmt.Errorf("column %q in table %q has foreign_key set but is missing column", colName, table.Name)
			}
			columnDef = fmt.Sprintf(
				"%s REFERENCES %s (%s)",
				columnDef,
				refTableID.Sanitize(),
				pgx.Identifier{refColumn}.Sanitize(),
			)
		}
		columnDefs = append(columnDefs, columnDef)
	}
	if primaryKeyCount > 1 {
		return fmt.Errorf("table %q has more than one primary key column configured", table.Name)
	}

	tableID, err := tableIdentifier(table.Name)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s)",
		tableID.Sanitize(),
		strings.Join(columnDefs, ", "),
	)

	if _, err := pool.Exec(ctx, query); err != nil {
		return fmt.Errorf("create table %q: %w", table.Name, err)
	}
	return nil
}
