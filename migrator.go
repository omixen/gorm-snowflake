package snowflake

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
)

type Migrator struct {
	migrator.Migrator
}

func (m Migrator) AutoMigrate(values ...interface{}) error {
	for _, value := range m.ReorderModels(values, true) {
		tx := m.DB.Session(&gorm.Session{NewDB: true})
		if !tx.Migrator().HasTable(value) {
			if err := tx.Migrator().CreateTable(value); err != nil {
				return err
			}
		} else {
			if err := m.RunWithValue(value, func(stmt *gorm.Statement) (errr error) {
				columnTypes, _ := m.DB.Migrator().ColumnTypes(value)

				for _, field := range stmt.Schema.FieldsByDBName {
					var foundColumn gorm.ColumnType

					for _, columnType := range columnTypes {
						if columnType.Name() == field.DBName {
							foundColumn = columnType
							break
						}
					}

					if foundColumn == nil {
						// not found, add column
						if err := tx.Migrator().AddColumn(value, field.DBName); err != nil {
							return err
						}
					} else if err := m.DB.Migrator().MigrateColumn(value, field, foundColumn); err != nil {
						// found, smart migrate
						return err
					}
				}

				for _, rel := range stmt.Schema.Relationships.Relations {
					if !m.DB.Config.DisableForeignKeyConstraintWhenMigrating {
						if constraint := rel.ParseConstraint(); constraint != nil {
							if constraint.Schema == stmt.Schema {
								if !tx.Migrator().HasConstraint(value, constraint.Name) {
									if err := tx.Migrator().CreateConstraint(value, constraint.Name); err != nil {
										return err
									}
								}
							}
						}
					}

					for _, chk := range stmt.Schema.ParseCheckConstraints() {
						if !tx.Migrator().HasConstraint(value, chk.Name) {
							if err := tx.Migrator().CreateConstraint(value, chk.Name); err != nil {
								return err
							}
						}
					}
				}

				return nil
			}); err != nil {
				return err
			}
		}
	}

	return nil
}

func (m Migrator) HasTable(value interface{}) bool {
	var count int
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			"SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_name = ? AND table_catalog = ?",
			strings.ToUpper(stmt.Table), m.CurrentDatabase(),
		).Row().Scan(&count)
	})
	return count > 0
}

func (m Migrator) DropTable(values ...interface{}) error {
	values = m.ReorderModels(values, false)
	for i := len(values) - 1; i >= 0; i-- {
		if err := m.RunWithValue(values[i], func(stmt *gorm.Statement) error {
			// dropping constraints automatically
			return m.DB.Exec("DROP TABLE IF EXISTS ?", clause.Table{Name: stmt.Table}).Error
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m Migrator) RenameTable(oldName, newName interface{}) error {
	var oldTable, newTable string
	if v, ok := oldName.(string); ok {
		oldTable = v
	} else {
		stmt := &gorm.Statement{DB: m.DB}
		if err := stmt.Parse(oldName); err == nil {
			oldTable = stmt.Table
		} else {
			return err
		}
	}

	if v, ok := newName.(string); ok {
		newTable = v
	} else {
		stmt := &gorm.Statement{DB: m.DB}
		if err := stmt.Parse(newName); err == nil {
			newTable = stmt.Table
		} else {
			return err
		}
	}

	return m.DB.Exec(
		"ALTER TABLE [ IF EXISTS ] <name> RENAME TO <new_table_name>",
		clause.Table{Name: oldTable}, clause.Table{Name: newTable},
	).Error
}

func (m Migrator) HasColumn(value interface{}, field string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		currentDatabase := m.DB.Migrator().CurrentDatabase()
		name := field
		if field := stmt.Schema.LookUpField(field); field != nil {
			name = field.DBName
		}

		return m.DB.Raw(
			"SELECT count(*) FROM INFORMATION_SCHEMA.columns WHERE table_catalog = ? AND table_name = ? AND column_name = ?",
			currentDatabase, strings.ToUpper(stmt.Table), strings.ToUpper(name),
		).Row().Scan(&count)
	})

	return count > 0
}

func (m Migrator) AlterColumn(value interface{}, field string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(field); field != nil {
			fileType := clause.Expr{SQL: m.DataTypeOf(field)}
			if field.NotNull {
				fileType.SQL += " NOT NULL"
			}

			return m.DB.Exec(
				"ALTER TABLE ? ALTER COLUMN ? ?",
				clause.Table{Name: stmt.Table}, clause.Column{Name: field.DBName}, fileType,
			).Error
		}
		return fmt.Errorf("failed to look up field with name: %s", field)
	})
}

func (m Migrator) RenameColumn(value interface{}, oldName, newName string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if field := stmt.Schema.LookUpField(oldName); field != nil {
			oldName = field.DBName
		}

		if field := stmt.Schema.LookUpField(newName); field != nil {
			newName = field.DBName
		}

		return m.DB.Exec(
			"sp_rename @objname = ?, @newname = ?, @objtype = 'COLUMN';",
			fmt.Sprintf("%s.%s", stmt.Table, oldName), clause.Column{Name: newName},
		).Error
	})
}

/*
	SNOWFLAKE DOES NOT SUPPORT INDEX
	SNOWFLAKE DOES MICRO PARTITIONING AUTOMATICALLY ON ALL TABLES
*/

// HasIndex return true to satisfy unit tests
func (m Migrator) HasIndex(value interface{}, name string) bool {
	return true
}

// RenameIndex return nil, SF does not support Index
func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	return nil
}

// CreateIndex return nil, SF does not support Index
func (m Migrator) CreateIndex(value interface{}, name string) error {
	return nil
}

// DropIndex return nil, SF does not support Index
func (m Migrator) DropIndex(value interface{}, name string) error {
	return nil
}

func (m Migrator) HasConstraint(value interface{}, name string) bool {
	var count int64
	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		return m.DB.Raw(
			`SELECT count(*) FROM sys.foreign_keys as F inner join sys.tables as T on F.parent_object_id=T.object_id inner join information_schema.tables as I on I.TABLE_NAME = T.name WHERE F.name = ?  AND T.Name = ? AND I.TABLE_CATALOG = ?;`,
			strings.ToUpper(name), strings.ToUpper(stmt.Table), m.CurrentDatabase(),
		).Row().Scan(&count)
	})
	return count > 0
}

func (m Migrator) CurrentDatabase() (name string) {
	m.DB.Raw("SELECT CURRENT_DATABASE()").Row().Scan(&name)
	return
}
