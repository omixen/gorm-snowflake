package snowflake

import (
	"strings"

	"gorm.io/gorm/schema"
)

// NamingStrategy for snowflake (always uppercase)
type NamingStrategy struct {
	defaultNS schema.Namer
}

// NewNamingStrategy create new instance of snowflake naming strat
func NewNamingStrategy() schema.Namer {
	return &NamingStrategy{
		defaultNS: schema.NamingStrategy{},
	}
}

// ColumnName snowflake edition
func (sns NamingStrategy) ColumnName(table, column string) string {
	return strings.ToUpper(sns.defaultNS.ColumnName(table, column))
}

// TableName snowflake edition
func (sns NamingStrategy) TableName(table string) string {
	return sns.defaultNS.TableName(table)
}

// SchemaName snowflake edition
func (sns NamingStrategy) SchemaName(table string) string {
	return sns.defaultNS.SchemaName(table)
}

// JoinTableName snowflake edition
func (sns NamingStrategy) JoinTableName(joinTable string) string {
	return sns.defaultNS.JoinTableName(joinTable)
}

// RelationshipFKName snowflake edition
func (sns NamingStrategy) RelationshipFKName(rel schema.Relationship) string {
	return sns.defaultNS.RelationshipFKName(rel)
}

// CheckerName snowflake edition
func (sns NamingStrategy) CheckerName(table, column string) string {
	return sns.defaultNS.CheckerName(table, column)
}

// IndexName snowflake edition
func (sns NamingStrategy) IndexName(table, column string) string {
	return sns.defaultNS.IndexName(table, column)
}
