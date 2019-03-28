# go-cql

Easy to use wrapper for gocql cassandra driver. Focussing on minimal and easy usages.

## Installation

go get -u github.com/aaabhilash97/go-cql

## Features

Building query using map
Binding result to struct

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
	Email      string
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

	UserTable := &cql.Table{
		Conn:      session,
		TableName: "USERS",
	}
	result, _ := UserTable.Find(cql.Q{
		"username": "ok"
	}, cql.FindOptions{})
	p := User{}
	cql.BindStruct(&p, result[0])
	fmt.Println(p, result[0])
}

```