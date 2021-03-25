# gorm-snowflake
Snowflake driver for gorm

## Snowflake Features

Notable Snowflake (SF) features that affect decisions in this driver

- Use of quotes in SF enforces case-sensitivity which requires string conditions to match. Right now, we are removing all quotes in the internals to make the driver case-insensitive and only uppercase when working with internal tables (INFORMATION_SCHEMA)
- SF does not support INDEX, it does micro-partitioning automatically in all tables for optimizations. Therefore all Index related functions are nil-returned.
- Transactions in SF do not support SAVEPOINT (https://docs.snowflake.com/en/sql-reference/transactions.html)
- GORM rely on being able to query back inserted/updated rows in every transaction in order to get default values back, there is no easy way to do this ala SQL Server (`OUTPUT INSERTED`) or Postgres (`RETURNING`) instead, we automatically set CHANGE_TRACKING on for all tables for now. THis allows us to run CHANGES query on the table after running any DML.