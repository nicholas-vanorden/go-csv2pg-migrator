package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Database DatabaseConfig `yaml:"database"`
	Options  Options        `yaml:"options"`
	Tables   []TableConfig  `yaml:"tables"`
}

type DatabaseConfig struct {
	DSN string `yaml:"dsn"`
}

type Options struct {
	DryRun      bool `yaml:"dry_run"`
	StopOnError bool `yaml:"stop_on_error"`
	BatchSize   int  `yaml:"batch_size"`
}

type TableConfig struct {
	Name               string                  `yaml:"name"`
	File               string                  `yaml:"file"`
	TruncateBeforeLoad bool                    `yaml:"truncate_before_load"`
	Columns            map[string]ColumnConfig `yaml:"columns"`
	IgnoreColumns      []string                `yaml:"ignore_columns"`
}

type ColumnConfig struct {
	Source    string `yaml:"source"`
	Transform string `yaml:"transform"`
}

func Load(path string) (*Config, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(bytes, &cfg); err != nil {
		return nil, err
	}

	if cfg.Options.BatchSize <= 0 {
		cfg.Options.BatchSize = 1000 // default batch size
	}

	return &cfg, nil
}
