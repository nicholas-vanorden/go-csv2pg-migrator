package config

import (
	"bytes"
	"fmt"
	"os"
	"strings"

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
	DryRun                 bool `yaml:"dry_run"`
	StopOnError            bool `yaml:"stop_on_error"`
	BatchSize              int  `yaml:"batch_size"`
	CreateTablesIfNotExist bool `yaml:"create_tables_if_not_exist"`
}

type TableConfig struct {
	Name               string                  `yaml:"name"`
	File               string                  `yaml:"file"`
	TruncateBeforeLoad bool                    `yaml:"truncate_before_load"`
	Columns            map[string]ColumnConfig `yaml:"columns"`
	IgnoreColumns      []string                `yaml:"ignore_columns"`
}

type ColumnConfig struct {
	Source     string             `yaml:"source"`
	Transform  string             `yaml:"transform"`
	Type       string             `yaml:"type"`
	PrimaryKey bool               `yaml:"primary_key"`
	ForeignKey *ForeignKeyConfig  `yaml:"foreign_key"`
}

type ForeignKeyConfig struct {
	Table  string `yaml:"table"`
	Column string `yaml:"column"`
}

func Load(path string) (*Config, error) {
	configBytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(configBytes))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, err
	}

	if cfg.Options.BatchSize <= 0 {
		cfg.Options.BatchSize = 1000 // default batch size
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	tableByName := make(map[string]TableConfig, len(c.Tables))
	for _, table := range c.Tables {
		tableByName[strings.TrimSpace(table.Name)] = table
	}

	for _, table := range c.Tables {
		primaryKeyCount := 0
		for colName, colCfg := range table.Columns {
			if colCfg.PrimaryKey {
				primaryKeyCount++
			}
			if colCfg.ForeignKey != nil {
				fkTable := strings.TrimSpace(colCfg.ForeignKey.Table)
				fkColumn := strings.TrimSpace(colCfg.ForeignKey.Column)
				if fkTable == "" || fkColumn == "" {
					return fmt.Errorf("column %q in table %q has foreign_key set but is missing table or column", colName, table.Name)
				}
				targetTable, ok := tableByName[fkTable]
				if !ok {
					return fmt.Errorf(
						"column %q in table %q references missing foreign key table %q",
						colName,
						table.Name,
						fkTable,
					)
				}
				if _, ok := targetTable.Columns[fkColumn]; !ok {
					return fmt.Errorf(
						"column %q in table %q references missing foreign key column %q on table %q",
						colName,
						table.Name,
						fkColumn,
						fkTable,
					)
				}
			}
		}
		if primaryKeyCount > 1 {
			return fmt.Errorf("table %q has more than one primary key column configured", table.Name)
		}
	}
	return nil
}
