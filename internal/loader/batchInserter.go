package loader

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

type BatchRow struct {
	LineNumber int
	Values     []any
	RawRow     []string
}

type BatchFailure struct {
	Row BatchRow
	Err error
}

type BatchResult struct {
	Inserted int
	Failures []BatchFailure
}

func insertBatch(
	ctx context.Context,
	tx pgx.Tx,
	tableID pgx.Identifier,
	columns []string,
	rows []BatchRow,
) (BatchResult, error) {

	savepointCounter := 0
	nextSavepoint := func(prefix string) string {
		savepointCounter++
		return fmt.Sprintf("%s_%d", prefix, savepointCounter)
	}

	valueRows := make([][]any, len(rows))

	for i, r := range rows {
		valueRows[i] = r.Values
	}

	copySavepoint := nextSavepoint("copy")
	if _, err := tx.Exec(ctx, "SAVEPOINT "+copySavepoint); err != nil {
		return BatchResult{}, fmt.Errorf("create savepoint for copy: %w", err)
	}

	count, err := tx.CopyFrom(
		ctx,
		tableID,
		columns,
		pgx.CopyFromRows(valueRows),
	)

	if err == nil {
		if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT "+copySavepoint); err != nil {
			return BatchResult{}, fmt.Errorf("release savepoint for copy: %w", err)
		}
		return BatchResult{Inserted: int(count)}, nil
	}

	if _, rollbackErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+copySavepoint); rollbackErr != nil {
		return BatchResult{}, fmt.Errorf("rollback to savepoint for copy: %w", rollbackErr)
	}
	if _, releaseErr := tx.Exec(ctx, "RELEASE SAVEPOINT "+copySavepoint); releaseErr != nil {
		return BatchResult{}, fmt.Errorf("release savepoint for copy after rollback: %w", releaseErr)
	}

	// fallback mode
	result := BatchResult{}

	for _, r := range rows {
		rowSavepoint := nextSavepoint("row")
		if _, err := tx.Exec(ctx, "SAVEPOINT "+rowSavepoint); err != nil {
			return BatchResult{}, fmt.Errorf("create savepoint for row: %w", err)
		}

		err := insertSingleRow(ctx, tx, tableID, columns, r.Values)

		if err != nil {
			if _, rollbackErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+rowSavepoint); rollbackErr != nil {
				return BatchResult{}, fmt.Errorf("rollback to savepoint for row: %w", rollbackErr)
			}
			if _, releaseErr := tx.Exec(ctx, "RELEASE SAVEPOINT "+rowSavepoint); releaseErr != nil {
				return BatchResult{}, fmt.Errorf("release savepoint for row after rollback: %w", releaseErr)
			}
			result.Failures = append(result.Failures, BatchFailure{
				Row: r,
				Err: err,
			})
			continue
		}
		if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT "+rowSavepoint); err != nil {
			return BatchResult{}, fmt.Errorf("release savepoint for row: %w", err)
		}
		result.Inserted++
	}

	return result, nil
}

func insertSingleRow(
	ctx context.Context,
	tx pgx.Tx,
	tableID pgx.Identifier,
	columns []string,
	values []any,
) error {

	placeholders := make([]string, len(values))
	quotedColumns := make([]string, len(columns))

	for i := range values {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	for i, col := range columns {
		quotedColumns[i] = pgx.Identifier{col}.Sanitize()
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		tableID.Sanitize(),
		strings.Join(quotedColumns, ", "),
		strings.Join(placeholders, ", "),
	)

	_, err := tx.Exec(ctx, query, values...)

	return err
}
