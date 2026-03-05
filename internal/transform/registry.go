package transform

import (
	"fmt"
)

type TransformFunc func(string) (any, error)

var Registry = map[string]TransformFunc{
	"oracle_date": OracleDate,
	"oracle_bool": OracleBool,
}

func OracleDate(input string) (any, error) {
	// TODO:Implement date transformation logic
	return nil, fmt.Errorf("oracle_date transform is not implemented")
}

func OracleBool(input string) (any, error) {
	// TODO:Implement boolean transformation logic
	return nil, fmt.Errorf("oracle_bool transform is not implemented")
}
