package main

import (
	"context"
	"flag"
	"log"
	"strings"

	"github.com/nicholas-vanorden/go-csv2pg-migrator/internal/config"
	"github.com/nicholas-vanorden/go-csv2pg-migrator/internal/loader"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config file")
	postgresDsn := flag.String("postgres-dsn", "", "Postgres data source name (DSN) in URI format")
	dryRun := flag.Bool("dry-run", false, "Run without committing to database")
	stopOnError := flag.Bool("stop-on-error", false, "Stop when a record fails")
	batchSize := flag.Int("batch-size", 0, "Table insert batch size")
	createTables := flag.Bool("create-tables", false, "Create tables if not exist (tables created during dry-run as well)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	trimmedDsn := strings.TrimSpace(*postgresDsn)
	if trimmedDsn != "" {
		cfg.Database.DSN = trimmedDsn
	}

	if *dryRun {
		cfg.Options.DryRun = true
	}

	if *stopOnError {
		cfg.Options.StopOnError = true
	}

	if *batchSize > 0 {
		cfg.Options.BatchSize = *batchSize
	}

	if *createTables {
		cfg.Options.CreateTablesIfNotExist = true
	}

	runner := loader.NewRunner(cfg)

	if err := runner.Run(context.Background()); err != nil {
		log.Fatalf("migration failed: %v", err)
	}

	log.Println("Migration complete")
}
