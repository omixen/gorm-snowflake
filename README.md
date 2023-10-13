# gorm-snowflake
Snowflake driver for [gorm](https://gorm.io/)

## Snowflake Features

Notable Snowflake (SF) features that affect decisions in this driver

- Use of quotes in SF enforces case-sensitivity which requires string conditions to match. Right now, we are removing all quotes in the internals to make the driver case-insensitive and only uppercase when working with internal tables (INFORMATION_SCHEMA)
- SF does not support INDEX, it does micro-partitioning automatically in all tables for optimizations. Therefore all Index related functions are nil-returned.
- Transactions in SF do not support SAVEPOINT (https://docs.snowflake.com/en/sql-reference/transactions.html)
- GORM rely on being able to query back inserted rows in every transaction in order to get default values back. There is no easy way to do this ala SQL Server (`OUTPUT INSERTED`) or Postgres (`RETURNING`). Instead, we automatically turn on SF `CHANGE_TRACKING` feature on for all tables. This allows us to run `CHANGES` query on the table after running any DML. However due to non-deterministic nature of return from `MERGE`, it doesn't support updates.
- The `SELECT...CHANGES` feature of SF does not return unchanged rows from `MERGE` statement, therefore we can only rely on the `APPEND_ONLY` option and only support returning fields from inserted rows in the same order.
- SF does not enforce any constraint other than NOT NULL. This driver expect all tests and features related to enforcing constraint to be disabled. (https://docs.snowflake.com/en/user-guide/table-considerations.html#referential-integrity-constraints)

## How To

How to use this project

```go
package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/snowflakedb/gosnowflake"
	snowflake "github.com/omixen/gorm-snowflake"
	"github.com/tillfinancial/goutil/errorss"
	"gorm.io/gorm"
)

type User struct {
	ID        int64
	FirstName string
	LastName  string
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Optional if you specify the Schema in the Config
func (r *User) TableName() string {
	return "MY_SCHEMA.USERS"
}

func main() {

	config := gosnowflake.Config{
		Account:   "12345678.us-east-1",
		User:      "<SNOWFLAKE USER>",
		Password:  "<PASSWORD>",
		Database:  "MY_DATABASE",
		Schema:    "MY_SCHEMA",    // Optional if your models have a TableName method
		Warehouse: "MY_WAREHOUSE", // Optional
	}

	connStr, err := gosnowflake.DSN(&config)
	if err != nil {
		panic(err)
	}

	db, err := gorm.Open(snowflake.Open(connStr), &gorm.Config{
		NamingStrategy: snowflake.NewNamingStrategy(),
	})
	if err != nil {
		panic(err)
	}

	as := []User{}
	tx := db.Where("status = ?", "active").Find(&as)
	if tx.Error != nil {
		fmt.Println(tx.Error)
	}

	bytes, err := json.MarshalIndent(as, "", "\t")
	if err !nil {
		panic(err)
	}
	fmt.Print(string(bytes))
}
```