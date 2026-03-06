package transform

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type TransformFunc func(string) (any, error)

var Registry = map[string]TransformFunc{
	"date":    Date,
	"boolean": Boolean,
	"money":   Money,
}

func Date(input string) (any, error) {
	s := strings.TrimSpace(input)
	if s == "" {
		return nil, nil
	}

	layouts := []string{
		"2006-01-02",
		"01/02/2006",
		"2006/01/02",
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.999999",
	}

	for _, layout := range layouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}

	return nil, fmt.Errorf("unsupported date value %q", input)
}

func Boolean(input string) (any, error) {
	s := strings.ToLower(strings.TrimSpace(input))
	switch s {
	case "1", "t", "true", "y", "yes":
		return true, nil
	case "0", "f", "false", "n", "no":
		return false, nil
	case "":
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported boolean value %q", input)
	}
}

func Money(input string) (any, error) {
	s := strings.TrimSpace(input)
	if s == "" {
		return nil, nil
	}

	negative := false
	if strings.HasPrefix(s, "(") && strings.HasSuffix(s, ")") {
		negative = true
		s = strings.TrimSuffix(strings.TrimPrefix(s, "("), ")")
	}

	s = strings.ReplaceAll(s, "$", "")
	s = strings.ReplaceAll(s, ",", "")
	s = strings.TrimSpace(s)

	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, fmt.Errorf("unsupported money value %q: %w", input, err)
	}

	if negative {
		v = -v
	}

	return v, nil
}
