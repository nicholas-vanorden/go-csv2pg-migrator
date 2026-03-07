package loader

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
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

func tableIdentifier(tableName string) (pgx.Identifier, error) {
	trimmed := strings.TrimSpace(tableName)
	if trimmed == "" {
		return nil, fmt.Errorf("table name is empty")
	}

	parts := strings.Split(trimmed, ".")
	identifier := make(pgx.Identifier, 0, len(parts))
	for _, part := range parts {
		segment := strings.TrimSpace(part)
		if segment == "" {
			return nil, fmt.Errorf("invalid table name %q: empty identifier segment", tableName)
		}
		identifier = append(identifier, segment)
	}

	return identifier, nil
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

	targetColumns := make([]string, 0, len(t.table.Columns))
	for targetCol := range t.table.Columns {
		targetColumns = append(targetColumns, targetCol)
	}
	sort.Strings(targetColumns)

	sourceIndexes := make([]int, len(targetColumns))
	transforms := make([]transform.TransformFunc, len(targetColumns))
	for i, targetCol := range targetColumns {
		colCfg := t.table.Columns[targetCol]
		idx, ok := headerIndex[colCfg.Source]
		if !ok {
			return fmt.Errorf("source column %q not found in CSV headers", colCfg.Source)
		}
		sourceIndexes[i] = idx

		if colCfg.Transform != "" {
			tf, ok := transform.Registry[colCfg.Transform]
			if !ok {
				return fmt.Errorf("unknown transform %q for column %q", colCfg.Transform, targetCol)
			}
			transforms[i] = tf
		}
	}

	tableID, err := tableIdentifier(t.table.Name)
	if err != nil {
		return err
	}
	dryRun := t.cfg.Options.DryRun

	var tx pgx.Tx
	if !dryRun {
		tx, err = t.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		if t.table.TruncateBeforeLoad {
			// Use pgx.Identifier to safely quote the table name
			_, err := tx.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", tableID.Sanitize()))
			if err != nil {
				return err
			}
		}
	}

	batchSize := t.cfg.Options.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	batchRows := make([][]any, 0, batchSize)
	copyBatch := func(batchStartLine int) error {
		if len(batchRows) == 0 {
			return nil
		}
		batchEndLine := batchStartLine + len(batchRows) - 1
		_, err := tx.CopyFrom(ctx, tableID, targetColumns, pgx.CopyFromRows(batchRows))
		if err != nil {
			return fmt.Errorf(
				"copy batch failed for table %q (csv lines %d-%d): %w",
				t.table.Name,
				batchStartLine,
				batchEndLine,
				err,
			)
		}
		batchRows = batchRows[:0]
		return nil
	}

	lineNum := 1 // header line already read
	batchStartLine := 0
	dryRunPrintLimit := 10
	dryRunPrintedRows := 0
	dryRunTotalRows := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading CSV record at line %d: %w", lineNum+1, err)
		}
		lineNum++

		row := make([]any, len(targetColumns))
		for i, targetCol := range targetColumns {
			idx := sourceIndexes[i]
			if idx >= len(record) {
				return fmt.Errorf(
					"record has fewer columns than expected at line %d (need index %d, got %d)",
					lineNum,
					idx,
					len(record),
				)
			}
			raw := record[idx]
			tf := transforms[i]
			if tf == nil {
				row[i] = raw
				continue
			}
			val, err := tf(raw)
			if err != nil {
				colCfg := t.table.Columns[targetCol]
				return fmt.Errorf(
					"transform %q failed for target column %q (source %q) at line %d (column_index=%d): %w",
					colCfg.Transform,
					targetCol,
					colCfg.Source,
					lineNum,
					idx,
					err,
				)
			}
			row[i] = val
		}

		if dryRun {
			dryRunTotalRows++
			if dryRunPrintedRows >= dryRunPrintLimit {
				continue
			}
			printableRow := make(map[string]any, len(targetColumns))
			for i, col := range targetColumns {
				printableRow[col] = row[i]
			}
			fmt.Printf("Would insert row: %v\n", printableRow)
			dryRunPrintedRows++
			continue
		}

		if len(batchRows) == 0 {
			batchStartLine = lineNum
		}
		batchRows = append(batchRows, row)
		if len(batchRows) >= batchSize {
			if err := copyBatch(batchStartLine); err != nil {
				return err
			}
		}
	}

	if dryRun {
		if dryRunTotalRows > dryRunPrintedRows {
			fmt.Printf("... and %d more rows\n", dryRunTotalRows-dryRunPrintedRows)
		}
		fmt.Println("Dry run - no transaction started")
		return nil
	}

	if err := copyBatch(batchStartLine); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	return nil
}
