package loader

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nicholas-vanorden/go-csv2pg-migrator/internal/config"
	"github.com/nicholas-vanorden/go-csv2pg-migrator/internal/report"
	"github.com/nicholas-vanorden/go-csv2pg-migrator/internal/transform"
)

type TableLoader struct {
	pool  *pgxpool.Pool
	cfg   *config.Config
	table config.TableConfig
}

type TableResult struct {
	Report    report.TableReport
	RowErrors []report.RowError
}

func NewTableLoader(pool *pgxpool.Pool, cfg *config.Config, table config.TableConfig) *TableLoader {
	return &TableLoader{
		pool:  pool,
		cfg:   cfg,
		table: table,
	}
}

func (t *TableLoader) Load(ctx context.Context) (result TableResult, err error) {
	start := time.Now()
	defer func() {
		result.Report.DurationSeconds = time.Since(start).Seconds()
	}()

	result.Report.Table = t.table.Name
	result.Report.SourceFile = t.table.File

	file, err := os.Open(t.table.File)
	if err != nil {
		return result, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = []rune(t.table.Delimiter)[0]
	headers, err := reader.Read()
	if err != nil {
		return result, err
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
			return result, fmt.Errorf("source column %q not found in CSV headers", colCfg.Source)
		}
		sourceIndexes[i] = idx

		if colCfg.Transform != "" {
			tf, ok := transform.Registry[colCfg.Transform]
			if !ok {
				return result, fmt.Errorf("unknown transform %q for column %q", colCfg.Transform, targetCol)
			}
			transforms[i] = tf
		}
	}

	tableID, err := tableIdentifier(t.table.Name)
	if err != nil {
		return result, err
	}
	dryRun := t.cfg.Options.DryRun
	stopOnError := t.cfg.Options.StopOnError

	var tx pgx.Tx
	if !dryRun {
		tx, err = t.pool.Begin(ctx)
		if err != nil {
			return result, err
		}
		defer tx.Rollback(ctx)

		if t.table.TruncateBeforeLoad {
			// Use pgx.Identifier to safely quote the table name
			_, err := tx.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s CASCADE", tableID.Sanitize()))
			if err != nil {
				return result, err
			}
		}
	}

	batchSize := t.cfg.Options.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	batchRows := make([]BatchRow, 0, batchSize)
	flushBatch := func() error {
		if len(batchRows) == 0 {
			return nil
		}
		batchResult, err := insertBatch(ctx, tx, tableID, targetColumns, batchRows)
		if err != nil {
			return err
		}
		result.Report.RowsInserted += batchResult.Inserted
		for _, failure := range batchResult.Failures {
			result.RowErrors = append(result.RowErrors, report.RowError{
				LineNumber: failure.Row.LineNumber,
				Error:      failure.Err.Error(),
				RawRow:     failure.Row.RawRow,
			})
			result.Report.RowsFailed++
		}
		if stopOnError && len(batchResult.Failures) > 0 {
			return batchResult.Failures[0].Err
		}
		batchRows = batchRows[:0]
		return nil
	}

	lineNum := 1 // header line already read
	dryRunPrintLimit := 10
	dryRunPrintedRows := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return result, fmt.Errorf("error reading CSV record at line %d: %w", lineNum+1, err)
		}
		lineNum++
		result.Report.RowsTotal++

		row := make([]any, len(targetColumns))
		rowValid := true
		for i, targetCol := range targetColumns {
			idx := sourceIndexes[i]
			if idx >= len(record) {
				rowErr := fmt.Errorf(
					"record has fewer columns than expected at line %d (need index %d, got %d)",
					lineNum,
					idx,
					len(record),
				)
				result.RowErrors = append(result.RowErrors, report.RowError{
					LineNumber: lineNum,
					Error:      rowErr.Error(),
					RawRow:     record,
				})
				result.Report.RowsFailed++
				rowValid = false
				if stopOnError {
					return result, rowErr
				}
				break
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
				rowErr := fmt.Errorf(
					"transform %q failed for target column %q (source %q) at line %d (column_index=%d): %w",
					colCfg.Transform,
					targetCol,
					colCfg.Source,
					lineNum,
					idx,
					err,
				)
				result.RowErrors = append(result.RowErrors, report.RowError{
					LineNumber: lineNum,
					Error:      rowErr.Error(),
					RawRow:     record,
				})
				result.Report.RowsFailed++
				rowValid = false
				if stopOnError {
					return result, rowErr
				}
				break
			}
			row[i] = val
		}

		if !rowValid {
			continue
		}

		if dryRun {
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

		batchRows = append(batchRows, BatchRow{
			LineNumber: lineNum,
			Values:     row,
			RawRow:     record,
		})
		if len(batchRows) >= batchSize {
			if err := flushBatch(); err != nil {
				return result, err
			}
		}
	}

	if dryRun {
		if result.Report.RowsTotal > dryRunPrintedRows {
			fmt.Printf("... and %d more rows\n", result.Report.RowsTotal-dryRunPrintedRows)
		}
		fmt.Println("Dry run - no transaction started")
		return result, nil
	}

	if err := flushBatch(); err != nil {
		return result, err
	}

	if err := tx.Commit(ctx); err != nil {
		return result, err
	}

	return result, nil
}
