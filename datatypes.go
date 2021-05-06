package qlx

import (
	"errors"
	"reflect"
)

func ToQlType(t reflect.Type, dbTag string) (string, error) {
	if dbTag == "id" {
		if t.Kind() != reflect.Int64 {
			return "", errors.New("id field's type can only be set to int64 type")
		}
		return "", nil
	}

	isPtr := false
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		isPtr = true
	}

	switch t.Kind() {
	case reflect.Int, reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Uint, reflect.Uint64, reflect.Uint32, reflect.Uint16, reflect.Uint8, reflect.Float32, reflect.Float64:
		name := t.String()
		if isPtr {
			return name, nil
		}
		return name + " not null default 0", nil
	case reflect.String:
		if isPtr {
			return "string", nil
		}
		return "string not null default ''", nil
	case reflect.Bool:
		if isPtr {
			return "bool", nil
		}
		return "bool not null default false", nil
	case reflect.Slice:
		switch t.Elem().Kind() {
		case reflect.Uint8:
			return "blob", nil
		}
	case reflect.Struct:
		switch t.String() {
		case "sql.NullString":
			return "string", nil
		case "sql.NullBool":
			return "bool", nil
		case "sql.NullInt32":
			return "int32", nil
		case "sql.NullInt64":
			return "int64", nil
		case "sql.NullFloat64":
			return "float64", nil
		}
	}
	return "", errors.New("unsupport field type:" + t.String() + ",kind=" + t.Kind().String())
}
