package dbutils

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type StructField struct {
	Name         string
	ColumnName   string
	SequenceName string
	IsPrimary    bool
	Type         reflect.Type
	Value        reflect.Value
}

type StructData struct {
	Name         string
	PrimaryField *StructField
	Fields       map[string]StructField // ColumnName => structField
}

func getStructData(val reflect.Value) (res StructData) {
	var goDeep func(v reflect.Value, level int)
	res.Fields = map[string]StructField{}
	goDeep = func(v reflect.Value, level int) {
		//Если указатель, то берем значение на который указывает
		if v.Kind() == reflect.Ptr {
			goDeep(v.Elem(), level)
			return
		}
		if v.Kind() == reflect.Interface {
			goDeep(v.Elem(), level)
			return
		}
		if v.Kind() != reflect.Struct {
			return
		}

		t := v.Type()

		//Если структура корневая, то берем название структуры
		if level == 0 {
			res.Name = t.Name()
		}

		//Смотрим поля структуры
		for i := 0; i < t.NumField(); i++ {

			fv := v.Field(i)
			ft := t.Field(i)
			tag := ft.Tag

			//Если в теге поля есть параметр "db",
			//добавляем его в список
			if colName := tag.Get("db"); len(colName) > 0 {
				colName = strings.ToLower(colName)
				seq := tag.Get("seq")
				field := StructField{
					Name:         ft.Name,
					ColumnName:   colName,
					SequenceName: seq,
					IsPrimary:    len(seq) > 0,
					Type:         fv.Type(),
					Value:        fv,
				}
				if field.IsPrimary {
					res.PrimaryField = &field
				}
				res.Fields[colName] = field

			} else if fv.Kind() == reflect.Struct {
				//Если поле является структурой,
				//то берем ее поля рекрсивно
				goDeep(fv, level+1)
			}
		}
	}
	goDeep(val, 0)
	return
}

// Функция для конвертации прочитанного из бд значения src в dv
// (частично скопирована из пакета sql)
func convert(dv reflect.Value, src interface{}) error {
	sv := reflect.ValueOf(src)

	if sv.IsValid() && sv.Type().AssignableTo(dv.Type()) {
		switch b := src.(type) {
		case []byte:
			dv.Set(reflect.ValueOf(cloneBytes(b)))
		default:
			dv.Set(sv)
		}
		return nil
	}

	if dv.Kind() == sv.Kind() && sv.Type().ConvertibleTo(dv.Type()) {
		dv.Set(sv.Convert(dv.Type()))
		return nil
	}

	// The following conversions use a string value as an intermediate representation
	// to convert between various numeric types.
	//
	// This also allows scanning into user defined types such as "type Int int64".
	// For symmetry, also check for string destination types.
	switch dv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		s := asString(src)
		i64, err := strconv.ParseInt(s, 10, dv.Type().Bits())
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", src, s, dv.Kind(), err)
		}
		dv.SetInt(i64)
		return nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		s := asString(src)
		u64, err := strconv.ParseUint(s, 10, dv.Type().Bits())
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", src, s, dv.Kind(), err)
		}
		dv.SetUint(u64)
		return nil
	case reflect.Float32, reflect.Float64:
		s := asString(src)
		f64, err := strconv.ParseFloat(s, dv.Type().Bits())
		if err != nil {
			err = strconvErr(err)
			return fmt.Errorf("converting driver.Value type %T (%q) to a %s: %v", src, s, dv.Kind(), err)
		}
		dv.SetFloat(f64)
		return nil
	case reflect.String:
		switch v := src.(type) {
		case string:
			dv.SetString(v)
			return nil
		case []byte:
			dv.SetString(string(v))
			return nil
		}
	}

	return fmt.Errorf("unsupported Scan, storing driver.Value type %T into type %T", src, dv.Interface())
}
