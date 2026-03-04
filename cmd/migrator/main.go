package main

import (
	"context"
	"flag"
	"log"

	"oracle2pg/internal/config"
	"oracle2pg/internal/loader"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to config file")
	dryRun := flag.Bool("dry-run", false, "Run without committing to database")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	if *dryRun {
		cfg.Options.DryRun = true
	}

	runner := loader.NewRunner(cfg)

	if err := runner.Run(context.Background()); err != nil {
		log.Fatalf("migration failed: %v", err)
	}

	log.Println("Migration complete")
}
