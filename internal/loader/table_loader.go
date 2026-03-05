package loader

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nicholas-vanorden/go-csv2pg-migrator/internal/config"
	"github.com/nicholas-vanorden/go-csv2pg-migrator/internal/transform"
)

type TableLoader struct {
	pool  *pgxpool.Pool
	cfg   *config.Config
	table config.TableConfig
}

func NewTableLoader(pool *pgxpool.Pool, cfg *config.Config, table config.TableConfig) *TableLoader {
	return &TableLoader{
		pool:  pool,
		cfg:   cfg,
		table: table,
	}
}

func (t *TableLoader) Load(ctx context.Context) error {
	file, err := os.Open(t.table.File)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	headers, err := reader.Read()
	if err != nil {
		return err
	}

	headerIndex := make(map[string]int)
	for i, h := range headers {
		headerIndex[h] = i
	}

	tx, err := t.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if t.table.TruncateBeforeLoad {
		_, err := tx.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", t.table.Name))
		if err != nil {
			return err
		}
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading CSV record: %w", err)
		}

		row := make(map[string]any)

		for targetCol, colCfg := range t.table.Columns {
			idx, ok := headerIndex[colCfg.Source]
			if !ok {
				return fmt.Errorf("source column %q not found in CSV headers", colCfg.Source)
			}
			if idx >= len(record) {
				return fmt.Errorf("record has fewer columns than expected (need index %d)", idx)
			}
			raw := record[idx]

			if colCfg.Transform != "" {
				tf, ok := transform.Registry[colCfg.Transform]
				if !ok {
					return fmt.Errorf("unknown transform %q for column %q", colCfg.Transform, targetCol)
				}
				val, err := tf(raw)
				if err != nil {
					fmt.Printf("transform error: %v\n", err)
					continue
				}
				row[targetCol] = val
			} else {
				row[targetCol] = raw
			}
		}

		if !t.cfg.Options.DryRun {
			// TODO: Use CopyFrom for performance (simplified for skeleton)
			fmt.Printf("Would insert row: %v\n", row)
		}
	}

	if t.cfg.Options.DryRun {
		fmt.Println("Dry run - rolling back")
		return nil
	}

	return tx.Commit(ctx)
}
