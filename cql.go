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

// Views is for materialized views
type Views struct {
	Name         string
	PartitionKey []string
	ClusterKey   []string
	Select       []string
}

// Table for table
type Table struct {
	Conn             *gocql.Session
	TableName        string
	Model            interface{}
	PartitionKey     []string
	ClusterKey       []string
	MaterializedView []Views
}

// QOpt is the options for Find
type QOpt struct {
	AllowFiltering bool
	Consistency    string
	Limit          int
	View           string
	ViewID         int
}

// InsertIfNotExistsResult is response for insert if not exists true
type InsertIfNotExistsResult struct {
	Applied bool
	Result  map[string]interface{}
}

// Q is Query type a shortcut for map[string]interface{}
type Q map[string]interface{}

// Find is used to perform different select queries
func (t *Table) Find(query Q, options QOpt) ([]map[string]interface{}, error) {

	selectedCol := t.getSelectedColumns(query, options)

	tableName := t.TableName

	if options.View != "" {
		for _, view := range t.MaterializedView {
			if options.View == view.Name {
				tableName = options.View
			}
		}
	} else if len(t.MaterializedView) >= options.ViewID && options.ViewID > 0 {
		tableName = t.MaterializedView[options.ViewID-1].Name
	}

	stmt := fmt.Sprintf(`SELECT %s FROM "%s"`, selectedCol, tableName)
	values := make([]interface{}, 0)
	whereCondition := parseQuery(query["where"].(Q), &values)
	if len(values) > 0 {
		stmt += fmt.Sprintf(" WHERE %s", whereCondition)
	}
	if options.Limit > 0 {
		stmt += fmt.Sprintf(" %d LIMIT", options.Limit)
	}
	if options.AllowFiltering {
		stmt += " ALLOW FILTERING"
	}
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

func (t *Table) getSelectedColumns(query Q, options QOpt) string {
	selects, ok := query["select"].([]string)
	if !ok {
		selects = make([]string, 0)
	}
	var err error
	var columns []string
	if options.View != "" {
		for _, view := range t.MaterializedView {
			if options.View == view.Name {
				columns = view.Select
			}
		}
	} else if len(t.MaterializedView) >= options.ViewID && options.ViewID > 0 {
		columns = t.MaterializedView[options.ViewID-1].Select
	}
	if len(columns) == 0 {
		columns, err = t.getColumns()
	}
	selectedColumns := make([]string, 0)
	ignoredColumns := make([]string, 0)
	if len(selects) > 0 {
		for _, key := range selects {
			if string(key[0]) == "-" || err != nil {
				ignoredColumns = append(ignoredColumns, string(key[1:]))
			} else {
				selectedColumns = append(selectedColumns, key)
			}
		}
	} else {
		return "*"
	}
	if len(columns) > 0 && len(ignoredColumns) > 0 && len(selectedColumns) == 0 {
		for _, key := range columns {
			if !isExistsS(key, ignoredColumns) {
				selectedColumns = append(selectedColumns, key)
			}
		}
		return strings.Join(selectedColumns, ",")
	} else if len(columns) > 0 && len(selectedColumns) == 0 {
		return strings.Join(columns, ",")
	} else if len(selectedColumns) > 0 {
		return strings.Join(selectedColumns, ",")
	}
	return "*"
}

func isExistsS(item string, items []string) bool {
	for _, key := range items {
		if key == item {
			return true
		}
	}
	return false
}

func (t *Table) getColumns() ([]string, error) {
	sv := reflect.ValueOf(t.Model).Elem()
	st := reflect.TypeOf(t.Model).Elem()
	if sv.Kind() == reflect.Ptr && !sv.IsNil() {
		return nil, fmt.Errorf("Invalid model")
	}
	if sv.Kind() != reflect.Struct && sv.Kind() != reflect.Interface {
		return nil, fmt.Errorf("Invalid string")
	}
	result := make([]string, 0)
	nFields := st.NumField()
	for i := 0; i < nFields; i++ {
		fieldName := st.Field(i).Name

		if !unicode.IsUpper(rune(fieldName[0])) {
			continue
		}
		result = append(result, toSnakeCase(fieldName))
	}

	return result, nil
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
