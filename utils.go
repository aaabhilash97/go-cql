package cql

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode"
)

func toSnakeCase(str string) string {
	var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func isExistsS(item string, items []string) bool {
	for _, key := range items {
		if key == item {
			return true
		}
	}
	return false
}

// BindStruct is used to bind map[string]interface{} to given struct
func BindStruct(model interface{}, m map[string]interface{}) error {
	sv := reflect.ValueOf(model).Elem()
	st := reflect.TypeOf(model).Elem()

	if sv.Kind() == reflect.Ptr && !sv.IsNil() {
		return fmt.Errorf("Unexpected Pointer")
	}
	if sv.Kind() != reflect.Struct && sv.Kind() != reflect.Interface {
		return fmt.Errorf("Expecting struct")
	}

	nFields := sv.NumField()
	for i := 0; i < nFields; i++ {
		fieldName := st.Field(i).Name

		if !unicode.IsUpper(rune(fieldName[0])) {
			continue
		}
		fieldValue := sv.Field(i)
		if !fieldValue.IsValid() {
			continue
		}

		if !fieldValue.CanSet() {
			continue
		}

		fieldName = toSnakeCase(fieldName)

		tag := st.Field(i).Tag.Get("cql")

		if tag == "-" {
			continue
		}

		tagList := strings.Split(tag, ",")
		for _, cqlTag := range tagList {
			cqlTagItem := strings.Split(cqlTag, "=")
			if len(cqlTagItem) == 2 && "column" == strings.ToLower(cqlTagItem[0]) {
				fieldName = cqlTagItem[1]
				break
			}

		}
		fieldType := fieldValue.Type()
		mapValue, ok := m[fieldName]
		if !ok {
			continue
		}
		value := reflect.ValueOf(mapValue)
		if fieldType == value.Type() {
			fieldValue.Set(value)
		} else if fieldType.Kind() == reflect.Ptr {
			var j interface{}
			if fieldType.String() == "*int" && value.Type().String() == "int" {
				var i = mapValue.(int)
				j = &i
			} else if fieldType.String() == "*int64" && value.Type().String() == "int64" {
				var i = mapValue.(int64)
				j = &i
			} else if fieldType.String() == "*bool" && value.Type().String() == "bool" {
				var i = mapValue.(bool)
				j = &i
			} else if fieldType.String() == "*string" && value.Type().String() == "string" {
				var i = mapValue.(int64)
				j = &i
			} else if fieldType.String() == "*time.Time" && value.Type().String() == "time.Time" {
				var i = mapValue.(time.Time)
				j = &i
			} else {
				continue
			}
			value = reflect.ValueOf(j)
			fieldValue.Set(value)
		}

	}
	return nil
}
