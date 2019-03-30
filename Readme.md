# go-cql

Easy to use wrapper for gocql cassandra driver. Focussing on minimal and easy usages.

## Installation

```sh
go get -u github.com/aaabhilash97/go-cql
```

## Features

- Building query using map
- Binding result to struct

## Example

```go
package main

import (
	"fmt"
	"time"

	"github.com/aaabhilash97/go-cql"
	"github.com/gocql/gocql"
)

type User struct {
	Username string
	Email      string `cql:"column=email"`
	password   string
	DeletedAt   *time.Time
}

func main() {
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "db_local"
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: "db_user",
		Password: "db_password",
	}
	cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()
	defer session.Close()

	userTable := &cql.Table{
		Conn:      session,
		TableName: "USERS",
		Model:     &PartnerAPIAuth{},
		MaterializedView: []cql.Views{
			cql.Views{
				Name:   "user_view1",
				Select: []string{"phone"},
			},
		},
	}
	result, err := userTable.Find(cql.Q{
		"where": cql.Q{
			"phone": "9895774319",
		},
	}, cql.QOpt{
		AllowFiltering: true,
		ViewID:         1,
	})
	p := User{}
	cql.BindStruct(&p, result[0])
	fmt.Println(p, result[0])
}

```