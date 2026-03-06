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
	for _, colName := range columnNames {
		colCfg := table.Columns[colName]
		colType := strings.TrimSpace(colCfg.Type)
		if colType == "" {
			return fmt.Errorf("column %q in table %q is missing required type when create_tables_if_not_exist is enabled", colName, table.Name)
		}
		if !allowedColumnType.MatchString(colType) {
			return fmt.Errorf("column %q in table %q has unsupported type %q", colName, table.Name, colType)
		}
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s", pgx.Identifier{colName}.Sanitize(), colType))
	}

	query := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s (%s)",
		pgx.Identifier{table.Name}.Sanitize(),
		strings.Join(columnDefs, ", "),
	)

	_, err := pool.Exec(ctx, query)
	return err
}
