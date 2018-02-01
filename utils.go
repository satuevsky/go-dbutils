// Содержит функции для выполнения запросов к БД
// на основе структур и Map'ов

package dbutils

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// Тип для хранения строки (map[столбец]значение)
type RowValue map[string]interface{}

// Интерфейс для всего, что может делать sql запросы.
// Используется в качестве аргумета для вызова последующих функций.
type ISqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type sqlVal struct {
	Value interface{}
}

func (v *sqlVal) Scan(src interface{}) error {
	v.Value = src
	return nil
}
func prepareSelectQuery(query string, value reflect.Value) string {
	query = strings.TrimSpace(query)
	prefix := strings.ToLower(query[:8])
	if prefix == "select *" {
		var columns []string
		var strData = getStructData(value)
		fields := strData.Fields
		for _, field := range fields {
			columns = append(columns, field.ColumnName)
		}
		colsQuery := strings.Join(columns, ", ")
		query = query[:7] + colsQuery + query[8:]
	}
	return query
}
func offsetArgNums(query string, startNum int, argPrefix string) (string, error) {
	var rx, err = regexp.Compile(argPrefix + `\d+`)
	if err != nil {
		return query, err
	}
	return rx.ReplaceAllStringFunc(query, func(oldArg string) string {
		var newArg = argPrefix + strconv.Itoa(startNum)
		startNum++
		return newArg
	}), nil
}

func SelectInt(executor ISqlExecutor, query string, args ...interface{}) (res int64, err error) {
	var i *int64
	err = executor.QueryRow(query, args...).Scan(&i)
	return *i, err
}
func SelectMaps(executor ISqlExecutor, query string, args ...interface{}) (scannedRows []RowValue, err error) {
	rows, err := executor.Query(query, args...)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var columns []*sql.ColumnType
		if columns, err = rows.ColumnTypes(); err != nil {
			return
		}

		var scanArgs []interface{}

		for i := 0; i < len(columns); i++ {
			scanArgs = append(scanArgs, &sqlVal{})
		}

		if err = rows.Scan(scanArgs...); err != nil {
			return
		}

		var row = map[string]interface{}{}
		for i, column := range columns {
			row[column.Name()] = scanArgs[i].(*sqlVal).Value
		}
		scannedRows = append(scannedRows, row)
	}
	return
}
func InsertMap(executor ISqlExecutor, values map[string]interface{}, tableName string, argPrefix string) (sql.Result, error) {
	var q = "INSERT INTO " + tableName
	var colNames []string
	var argNames []string
	var args []interface{}
	var i = 1

	for key, val := range values {
		colNames = append(colNames, key)
		argNames = append(argNames, argPrefix+strconv.Itoa(i))
		args = append(args, val)
		i++
	}

	q += "(" + strings.Join(colNames, ", ") + ")"
	q += "VALUES(" + strings.Join(argNames, ", ") + ")"
	return executor.Exec(q, args...)
}
func UpdateMap(executor ISqlExecutor, values map[string]interface{}, tableName string, argPrefix string, whereCondition string, args ...interface{}) (sql.Result, error) {
	var q = "UPDATE " + tableName + " SET "
	var sets []string
	var preparedArgs []interface{}
	var i = 1

	for key, val := range values {
		sets = append(sets, key+"="+argPrefix+strconv.Itoa(i))
		preparedArgs = append(preparedArgs, val)
		i++
	}

	preparedArgs = append(preparedArgs, args...)
	whereCondition, err := offsetArgNums(whereCondition, i, argPrefix)

	if err != nil {
		return nil, err
	}

	q += strings.Join(sets, ", ") + " " + whereCondition

	fmt.Printf("%s %+v", q, preparedArgs)

	return executor.Exec(q, preparedArgs...)
}

func SelectStruct(executor ISqlExecutor, arrPtr interface{}, query string, args ...interface{}) ([]RowValue, error) {
	// Проверки на указатель
	var ptrVal = reflect.ValueOf(arrPtr)
	if ptrVal.Kind() != reflect.Ptr {
		return nil, errors.New("Для выборки необходимо передать указатель на массив ")
	}
	if ptrVal.IsNil() {
		return nil, errors.New("Указатель равен nil ")
	}

	// Проверки на значение
	var sliceVal = ptrVal.Elem()
	if sliceVal.Kind() != reflect.Slice {
		return nil, errors.New("Переданный указатель указывает не на массив ")
	}

	var itemType = sliceVal.Type().Elem()
	var isPtrItem = itemType.Kind() == reflect.Ptr

	if isPtrItem {
		itemType = itemType.Elem()
	}

	query = prepareSelectQuery(query, reflect.New(itemType))

	scannedRows, err := SelectMaps(executor, query, args...)
	if err != nil {
		return nil, err
	}

	for _, row := range scannedRows {
		var rowVal = reflect.New(itemType)
		var structData = getStructData(rowVal)

		for colName, val := range row {
			colName = strings.ToLower(colName)
			field, ok := structData.Fields[colName]
			if !ok {
				continue
			}
			value := field.Value

			if value.Kind() == reflect.Ptr {
				if value.IsNil() {
					if val != nil {
						value.Set(reflect.New(field.Type.Elem()))
					} else {
						continue
					}
				}
				value = value.Elem()
			}
			convert(value, val)
		}
		if !isPtrItem {
			rowVal = rowVal.Elem()
		}
		sliceVal.Set(reflect.Append(sliceVal, rowVal))
	}
	return scannedRows, nil
}
func InsertStruct(executor ISqlExecutor, i interface{}, tableName string, argPrefix string, nullSensitive bool) (sql.Result, error) {
	var structData = getStructData(reflect.ValueOf(i))
	var values = map[string]interface{}{}
	for key, val := range structData.Fields {
		if val.IsPrimary {
			if val.SequenceName != "" {
				id, err := SelectInt(executor, "SELECT "+val.SequenceName+".nextval FROM dual")
				if err != nil {
					return nil, errors.New("Не удалось получить следующий идентификатор из " + val.SequenceName + ". (" + err.Error() + ")")
				}
				values[key] = id
			}
		} else {
			if !nullSensitive && val.Value.Kind() == reflect.Ptr && val.Value.IsNil() {
				continue
			}
			values[key] = val.Value.Interface()
		}
	}
	return InsertMap(executor, values, tableName, argPrefix)
}
func UpdateStruct(executor ISqlExecutor, i interface{}, tableName string, argPrefix string, nullSensitive bool) (sql.Result, error) {
	var structData = getStructData(reflect.ValueOf(i))

	//Проверка на первичный ключ
	if structData.PrimaryField == nil || (structData.PrimaryField.Value.Kind() == reflect.Ptr && structData.PrimaryField.Value.IsNil()) {
		return nil, errors.New("Ошибка обновления: Не найден первичный ключ на структуре " + structData.Name)
	}

	//Считывание полей структуры в мапу
	var values = map[string]interface{}{}
	for key, field := range structData.Fields {
		//Если первичный ключ или имеет nil значение пропускаем поле
		if field.IsPrimary || (!nullSensitive && field.Value.Kind() == reflect.Ptr && field.Value.IsNil()) {
			continue
		}
		values[key] = field.Value.Interface()
	}

	//Условие для обновления записи по первичному ключу
	var whereCondition = "WHERE " + structData.PrimaryField.ColumnName + "=" + argPrefix + strconv.Itoa(len(values)+1)

	return UpdateMap(executor, values, tableName, argPrefix, whereCondition, structData.PrimaryField.Value.Interface())
}
func DeleteStruct(executor ISqlExecutor, i interface{}, tableName string, argPrefix string) (sql.Result, error) {
	var structData = getStructData(reflect.ValueOf(i))
	//Проверка на первичный ключ
	if structData.PrimaryField == nil || (structData.PrimaryField.Value.Kind() == reflect.Ptr && structData.PrimaryField.Value.IsNil()) {
		return nil, errors.New("Ошибка удаления: Не найден первичный ключ на структуре " + structData.Name)
	}

	var query = `DELETE FROM ` + tableName + ` WHERE ` + structData.PrimaryField.ColumnName + `=:1`
	return executor.Exec(query, structData.PrimaryField.Value.Interface())
}
