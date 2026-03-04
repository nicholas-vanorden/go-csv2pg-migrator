type TransformFunc func(string) (any, error)

var Registry = map[string]TransformFunc{
	"oracle_date": OracleDate,
	"oracle_bool": OracleBool,
}