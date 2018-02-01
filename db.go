package dbutils

import (
	"database/sql"
	"github.com/pkg/errors"
)

// Драйвер БД. Используется для реализации
// специфичных для различных БД задач
type Driver string

const (
	Postgre Driver = "postgre"
	Oracle  Driver = "oracle"
)

type DB struct {
	sqlDB     *sql.DB // указатель на объект бд
	sqlTx     *sql.Tx // указатель на транзакцию
	driver    Driver  // тип базы данных
	argPrefix string  // префикс аргумента

	// чувствительность к nil значениям, используется при обновлении данных. Если false, то при
	// составлении запроса nil поля игнорируются. По умолчанию false. При необходимости
	// запроса чувствительного к nil значения используйте метод NullSensitive(), который вернет
	// копию объекта с полем nullSensitive равным true.
	nullSensitive bool
}

func (db *DB) tx() (tx *sql.Tx, err error) {
	if db.sqlTx == nil {
		if db.sqlTx, err = db.sqlDB.Begin(); err != nil {
			return
		}
	}
	return db.sqlTx, nil
}

// Возвращает копию объекта, у которого nullSensitive равен true.
func (db DB) NullSensitive() DB {
	db.nullSensitive = true
	return db
}

// Завершает транзакцию
func (db *DB) Close(commit bool) (err error) {
	if db.sqlTx == nil {
		return
	}
	if commit {
		return db.sqlTx.Commit()
	}
	return db.sqlTx.Rollback()
}

/////////////////////////////////////
// Методы для выполнения запросов, //
//   аналогичные методам sql.DB    //
/////////////////////////////////////

// Аналог метода Exec для sql.DB
func (db *DB) Exec(query string, args ...interface{}) (res sql.Result, err error) {
	if _, err = db.tx(); err != nil {
		return
	}
	return db.sqlTx.Exec(query, args...)
}

// Аналог метода Query для sql.DB
func (db *DB) Query(query string, args ...interface{}) (rows *sql.Rows, err error) {
	if _, err = db.tx(); err != nil {
		return
	}
	return db.sqlTx.Query(query, args...)
}

// Аналог метода QueryRow для sql.DB
func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	if _, err := db.tx(); err != nil {
		return db.sqlDB.QueryRow(query, args...)
	}
	return db.sqlTx.QueryRow(query, args...)
}

//////////////////////////////
// Обертки над функциями из //
//     файла utils.go       //
//////////////////////////////

// Выполняет select запрос и возвращает результат запроса как массив Map'ов.
func (db *DB) SelectMaps(query string, args ...interface{}) ([]RowValue, error) {
	return SelectMaps(db, query, args...)
}

// Считывает результат запроса в переданный массив структур.
func (db *DB) Select(i interface{}, query string, args ...interface{}) ([]RowValue, error) {
	return SelectStruct(db, i, query, args...)
}

// Выполняет select запрос и возвращает результат запроса как int64.
func (db *DB) SelectInt(query string, args ...interface{}) (res int64, err error) {
	return SelectInt(db, query, args...)
}

// Выполняет insert запрос по переданной структуре.
func (db *DB) Insert(i interface{}, tableName string) (res sql.Result, err error) {
	return InsertStruct(db, i, tableName, db.argPrefix, db.nullSensitive)
}

// Выполняет update запрос по переданной структуре.
func (db *DB) Update(i interface{}, tableName string) (sql.Result, error) {
	return UpdateStruct(db, i, tableName, db.argPrefix, db.nullSensitive)
}

// Выполняет delete запрос на основе переданной структуры.
func (db *DB) Delete(i interface{}, tableName string) (sql.Result, error) {
	return DeleteStruct(db, i, tableName, db.argPrefix)
}

///////////////////////////////////
// Конструкторы для структуры DB //
///////////////////////////////////

func GetOracleDB(db *sql.DB) (*DB, error) {
	if db == nil {
		return nil, errors.New("GetOracleDB: db равен nil")
	}
	return &DB{
		sqlDB:     db,
		driver:    Oracle,
		argPrefix: ":",
	}, nil
}
func GetPostgreDB(db *sql.DB) (*DB, error) {
	if db == nil {
		return nil, errors.New("GetPostgreDB: db равен nil")
	}
	return &DB{
		sqlDB:     db,
		driver:    Postgre,
		argPrefix: "$",
	}, nil
}
