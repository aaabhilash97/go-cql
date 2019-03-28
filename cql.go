package cql

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/gocql/gocql"
)

// Table for table
type Table struct {
	Conn      *gocql.Session
	TableName string
	Model     interface{}
}

// FindOptions is the options for Find
type FindOptions struct {
	AllowFiltering bool
}

// InsertIfNotExistsResult is response for insert if not exists true
type InsertIfNotExistsResult struct {
	Applied bool
	Result  map[string]interface{}
}

// Q is Query type a shortcut for map[string]interface{}
type Q map[string]interface{}

// Find is used to perform different select queries
func (t *Table) Find(query Q, options FindOptions) ([]map[string]interface{}, error) {
	stmt := fmt.Sprintf(`SELECT * FROM "%s"`, t.TableName)
	values := make([]interface{}, 0)
	whereCondition := parseQuery(query, &values)
	if len(values) > 0 {
		stmt += fmt.Sprintf(" WHERE %s", whereCondition)
	}
	fmt.Println(stmt, values)
	iter := t.Conn.Query(stmt, values...).Iter()

	result := make([]map[string]interface{}, 0)
	for {
		// New map each iteration
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}
		result = append(result, row)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return result, nil
}

// BindStruct is
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

var operators = map[string]string{
	"$eq":  "=",
	"$gte": ">=",
	"$gt":  ">",
	"$lt":  "<",
	"$lte": "<=",
}

func parseQuery(q Q, values *[]interface{}) string {
	cur := ""
	for key, value := range q {
		if key == "$eq" || key == "$gt" || key == "$gte" || key == "$lt" || key == "$lte" {
			*values = append(*values, value)
			cur += fmt.Sprintf("%s?", operators[key])
		} else if newQ, ok := value.(Q); ok {
			next := parseQuery(newQ, values)
			next = strings.Trim(next, " AND")
			cur += fmt.Sprintf(" %s %s %s", "AND", key, next)
		} else {
			*values = append(*values, value)
			cur += fmt.Sprintf(" %s %s=?", "AND", key)
		}
	}
	cur = strings.Trim(cur, " AND")
	return cur
}

func toSnakeCase(str string) string {
	var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}
