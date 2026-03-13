package report

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"
)

type MigrationReport struct {
	MigrationStarted  time.Time     `json:"migration_started"`
	MigrationFinished time.Time     `json:"migration_finished"`
	DryRun            bool          `json:"dry_run"`
	Tables            []TableReport `json:"tables"`
	TotalRows         int           `json:"total_rows_processed"`
	TotalFailed       int           `json:"total_rows_failed"`
}

type TableReport struct {
	Table           string  `json:"table"`
	SourceFile      string  `json:"source_file"`
	RowsTotal       int     `json:"rows_total"`
	RowsInserted    int     `json:"rows_inserted"`
	RowsFailed      int     `json:"rows_failed"`
	DurationSeconds float64 `json:"duration_seconds"`
	ErrorFile       string  `json:"error_file,omitempty"`
}

type RowError struct {
	LineNumber int
	Error      string
	RawRow     []string
}

func WriteJSONReport(path string, report MigrationReport) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	return encoder.Encode(report)
}

func WriteErrorCSV(path string, errors []RowError) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{"line_number", "error", "raw_row"})

	for _, e := range errors {
		writer.Write([]string{
			strconv.Itoa(e.LineNumber),
			e.Error,
			strings.Join(e.RawRow, ","),
		})
	}

	return nil
}
