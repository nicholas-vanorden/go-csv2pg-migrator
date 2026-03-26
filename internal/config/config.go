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
	Delimiter          string                  `yaml:"delimiter"`
	Columns            map[string]ColumnConfig `yaml:"columns"`
	IgnoreColumns      []string                `yaml:"ignore_columns"`
}

type ColumnConfig struct {
	Source     string            `yaml:"source"`
	Transform  string            `yaml:"transform"`
	Param      string            `yaml:"param"`
	Type       string            `yaml:"type"`
	PrimaryKey bool              `yaml:"primary_key"`
	ForeignKey *ForeignKeyConfig `yaml:"foreign_key"`
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
	tableIndex := make(map[string]int, len(c.Tables))
	tableByName := make(map[string]TableConfig, len(c.Tables))
	for i, table := range c.Tables {
		normalizedName, err := normalizeTableName(table.Name)
		if err != nil {
			return err
		}
		tableByName[normalizedName] = table
		tableIndex[normalizedName] = i
	}

	for i, table := range c.Tables {
		if table.Delimiter == "" {
			c.Tables[i].Delimiter = ","
		}
		primaryKeyCount := 0
		for colName, colCfg := range table.Columns {
			if colCfg.PrimaryKey {
				primaryKeyCount++
			}
			if colCfg.ForeignKey != nil {
				fkTable, err := normalizeTableName(colCfg.ForeignKey.Table)
				if err != nil {
					return fmt.Errorf("column %q in table %q has invalid foreign_key table: %w", colName, table.Name, err)
				}
				fkColumn := strings.TrimSpace(colCfg.ForeignKey.Column)
				if fkColumn == "" {
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
				targetCol, ok := targetTable.Columns[fkColumn]
				if !ok {
					return fmt.Errorf(
						"column %q in table %q references missing foreign key column %q on table %q",
						colName,
						table.Name,
						fkColumn,
						fkTable,
					)
				}
				if c.Options.CreateTablesIfNotExist {
					targetIndex := tableIndex[fkTable]
					if targetIndex > i {
						return fmt.Errorf(
							"column %q in table %q references foreign key table %q defined later in config",
							colName,
							table.Name,
							fkTable,
						)
					}
					if !targetCol.PrimaryKey {
						return fmt.Errorf(
							"column %q in table %q references non-primary-key column %q on table %q",
							colName,
							table.Name,
							fkColumn,
							fkTable,
						)
					}
				}
			}
		}
		if primaryKeyCount > 1 {
			return fmt.Errorf("table %q has more than one primary key column configured", table.Name)
		}
	}
	return nil
}

func normalizeTableName(name string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", fmt.Errorf("table name is empty")
	}
	parts := strings.Split(trimmed, ".")
	if len(parts) > 2 {
		return "", fmt.Errorf("invalid table name %q: too many identifier segments (%d)", name, len(parts))
	}
	normalizedParts := make([]string, 0, len(parts))
	for _, part := range parts {
		segment := strings.TrimSpace(part)
		if segment == "" {
			return "", fmt.Errorf("invalid table name %q: empty identifier segment", name)
		}
		normalizedParts = append(normalizedParts, segment)
	}
	return strings.Join(normalizedParts, "."), nil
}
