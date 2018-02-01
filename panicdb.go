package dbutils

import "database/sql"

// PanicDB исполняет методы DB без возврата ошибки,
// вместо них выбрасываются паники.
type PanicDB struct {
	SafeDB DB
}

func (p PanicDB) NullSensitive() PanicDB {
	p.SafeDB = p.SafeDB.NullSensitive()
	return p
}

func (p *PanicDB) Close(commit bool) {
	if err := p.SafeDB.Close(commit); err != nil {
		panic(err)
	}
}
func (p *PanicDB) Exec(query string, args ...interface{}) sql.Result {
	res, err := p.SafeDB.Exec(query, args...)
	if err != nil {
		panic(err)
	}
	return res
}
func (p *PanicDB) Query(query string, args ...interface{}) *sql.Rows {
	res, err := p.SafeDB.Query(query, args...)
	if err != nil {
		panic(err)
	}
	return res
}

func (p *PanicDB) Select(i interface{}, query string, args ...interface{}) []RowValue {
	rows, err := p.SafeDB.Select(i, query, args...)
	if err != nil {
		panic(err)
	}
	return rows
}
func (p *PanicDB) Insert(i interface{}, tableName string) (res sql.Result) {
	res, err := p.SafeDB.Insert(i, tableName)
	if err != nil {
		panic(err)
	}
	return
}
func (p *PanicDB) Update(i interface{}, tableName string) sql.Result {
	res, err := p.SafeDB.Update(i, tableName)
	if err != nil {
		panic(err)
	}
	return res
}
func (p *PanicDB) Delete(i interface{}, tableName string) sql.Result {
	res, err := p.SafeDB.Delete(i, tableName)
	if err != nil {
		panic(err)
	}
	return res
}

func GetOraclePanicDB(sqlDB *sql.DB) *PanicDB {
	db, err := GetOracleDB(sqlDB)
	if err != nil {
		panic(err)
	}
	return &PanicDB{SafeDB: *db}
}
func GetPostgrePanicDB(sqlDB *sql.DB) *PanicDB {
	db, err := GetPostgreDB(sqlDB)
	if err != nil {
		panic(err)
	}
	return &PanicDB{SafeDB: *db}
}
