package transform

type TransformFunc func(string) (any, error)

var Registry = map[string]TransformFunc{
	"oracle_date": OracleDate,
	"oracle_bool": OracleBool,
}

func OracleDate(input string) (any, error) {
	// Implement your date transformation logic here
	return input, nil
}

func OracleBool(input string) (any, error) {
	// Implement your boolean transformation logic here
	return input, nil
}
