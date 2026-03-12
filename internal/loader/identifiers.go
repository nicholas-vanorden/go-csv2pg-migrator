package loader

import (
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

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
