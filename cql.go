package cql

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
)

var operators = map[string]string{
	"$eq":  "=",
	"$gte": ">=",
	"$gt":  ">",
	"$lt":  "<",
	"$lte": "<=",
}

// Error is error from CQL
type Error struct {
	Msg  string
	Code int
}

func (e *Error) Error() string {
	return fmt.Sprintf("CQL ERROR: %s", e.Msg)
}

// Views is for materialized views
type Views struct {
	Name         string
	PartitionKey []string
	ClusterKey   []string
	Select       []string
}

// Table for cassandra table
//
// Example
//  userTable := &cql.Table{
//		Conn:      session,
//		TableName: "USERS",
//		Model:     &PartnerAPIAuth{},
//		MaterializedView: []cql.Views{
//			cql.Views{
//				Name:   "user_view1",
//				Select: []string{"phone"},
//			},
//		},
//	}
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
	BindTo         interface{}
}

// InsertIfNotExistsResult is response for insert if not exists true
type InsertIfNotExistsResult struct {
	Applied bool
	Result  map[string]interface{}
}

// Q is Query type a shortcut for map[string]interface{}
type Q map[string]interface{}

// Find is used to perform select queries
//	result, err := userTable.Find(cql.Q{
//		"where": cql.Q{
//			"phone": "9895774319",
//		},
//	}, cql.QOpt{
//		AllowFiltering: true,
//		ViewID:         1,
//})
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
		stmt += fmt.Sprintf(" LIMIT %d", options.Limit)
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
		return nil, &Error{err.Error(), UnknownError}
	}
	return result, nil
}

// FindOne is used to perform get one result
//	result, err := userTable.Find(cql.Q{
//		"where": cql.Q{
//			"phone": "9895774319",
//		},
//	}, cql.QOpt{
//		AllowFiltering: true,
//		ViewID: 1,
//	})
func (t *Table) FindOne(query Q, options QOpt) (map[string]interface{}, error) {
	options.Limit = 1
	result, err := t.Find(query, options)
	if err != nil {
		return nil, err
	} else if len(result) == 0 {
		return nil, &Error{"No Matching Row", NoMatchingRow}
	}
	BindStruct(options.BindTo, result[0])
	return result[0], nil
}

// InsertIfNotExists is used to to insert row if not exists
func (t *Table) InsertIfNotExists(input map[string]interface{}, tableName string) (*InsertIfNotExistsResult, error) {
	stmt := fmt.Sprintf(`INSERT INTO "%s"`, tableName)
	columns := ""
	values := []interface{}{}
	for key, value := range input {
		values = append(values, value)
		columns += `"` + key + `"` + ","
	}
	columns = strings.Trim(columns, ",")
	stmt += " (" + columns + ") "
	stmt += "VALUES("
	for range values {
		stmt += "?,"
	}
	stmt = strings.Trim(stmt, ",")
	stmt += ") IF NOT EXISTS"
	result := make(map[string]interface{})

	applied, err := t.Conn.Query(
		stmt,
		values...).MapScanCAS(result)
	if err != nil {
		return nil, err
	}
	return &InsertIfNotExistsResult{
		Applied: applied,
		Result:  result,
	}, nil
}
