package transform

import (
	"fmt"
)

type TransformFunc func(string) (any, error)

var Registry = map[string]TransformFunc{
	"date":    Date,
	"boolean": Boolean,
}

func Date(input string) (any, error) {
	// TODO:Implement date transformation logic
	return nil, fmt.Errorf("date transform is not implemented")
}

func Boolean(input string) (any, error) {
	// TODO:Implement boolean transformation logic
	return nil, fmt.Errorf("boolean transform is not implemented")
}
