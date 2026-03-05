package loader

import (
	"context"
	"log"

	"github.com/nicholas-vanorden/go-csv2pg-migrator/internal/config"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
