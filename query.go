package cassago

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

func (tx *db) resetDB() {
	tx.statement = nil
	tx.err = nil
}

func (tx *db) buildQeury() error {
	switch tx.statement.operation {
	case operationFindOne, operationFindAll:
		// read operation
		var selects string
		var wheres string

		if len(tx.statement.selects) > 0 {
			selects = strings.Join(tx.statement.selects, ", ")
		} else {
			selects = "*"
		}

		conditions := make([]string, 0)
		for _, where := range tx.statement.wheres {
			conditions = append(conditions, where.condition)

			tx.statement.values = append(tx.statement.values, where.values...)
		}

		wheres = strings.Join(conditions, " AND ")

		tx.statement.query = fmt.Sprintf("SELECT %s FROM %s WHERE %s", selects, tx.statement.tableName, wheres)

		if len(tx.statement.groups) > 0 {
			tx.statement.query += ` GROUP BY ` + strings.Join(tx.statement.groups, ", ")
		}

		if tx.statement.operation == operationFindOne {
			tx.statement.query += ` LIMIT 1`
		}

		if tx.statement.operation == operationFindAll {
			limit := defaultLimit
			if tx.statement.limit > 0 {
				limit = tx.statement.limit
			}
			tx.statement.query += fmt.Sprintf(" LIMIT %d", limit)
		}
	case operationCreateOne:
		// create one operation

		var inserts string
		var binds string
		var insertsArray = []string{}
		var bindsArray = []string{}

		columns := tables[tx.statement.tableName].columnsByColumnName

		entityValue := reflect.ValueOf(tx.model.entity).Elem()

		for _, column := range columns {
			value := reflect.Indirect(entityValue).FieldByName(column.fieldName)

			// if value is pointer and field is also pointer, skip
			// TODO: Fix this
			if value.Kind() == reflect.Ptr && column.fieldIsPointer {
				// continue
			}

			tx.statement.values = append(tx.statement.values, reflect.Indirect(entityValue).FieldByName(column.fieldName).Interface())

			insertsArray = append(insertsArray, column.columnName)
			bindsArray = append(bindsArray, "?")
		}
		inserts = strings.Join(insertsArray, ", ")
		binds = strings.Join(bindsArray, ", ")

		tx.statement.query = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", tx.statement.tableName, inserts, binds)
	case operationPatchOne:
		// patch operation
		var sets string
		var updates = []string{}
		for key, value := range *tx.statement.updates {
			update := ` ` + key + ` = ? `
			updates = append(updates, update)

			tx.statement.values = append(tx.statement.values, value)
		}
		sets = strings.Join(updates, ", ")

		var wheres string

		conditions := make([]string, 0)
		for _, where := range tx.statement.wheres {
			conditions = append(conditions, where.condition)

			tx.statement.values = append(tx.statement.values, where.values...)
		}

		wheres = strings.Join(conditions, " AND ")

		tx.statement.query = fmt.Sprintf("Update %s SET %s WHERE %s", tx.statement.tableName, sets, wheres)
	case operationIncreaseOne:
		// increase operation
		var sets string
		var updates = []string{}
		for key, value := range *tx.statement.updates {
			update := ` ` + key + ` += ? `
			updates = append(updates, update)

			tx.statement.values = append(tx.statement.values, value)
		}
		sets = strings.Join(updates, ", ")

		var wheres string

		conditions := make([]string, 0)
		for _, where := range tx.statement.wheres {
			conditions = append(conditions, where.condition)

			tx.statement.values = append(tx.statement.values, where.values...)
		}

		wheres = strings.Join(conditions, " AND ")

		tx.statement.query = fmt.Sprintf("Update %s SET %s WHERE %s", tx.statement.tableName, sets, wheres)
	case operationDeleteOne, operationDeleteAll:
		// delete operation
		var wheres string

		conditions := make([]string, 0)
		for _, where := range tx.statement.wheres {
			conditions = append(conditions, where.condition)

			tx.statement.values = append(tx.statement.values, where.values...)
		}

		wheres = strings.Join(conditions, " AND ")

		tx.statement.query = fmt.Sprintf("DELETE FROM %s WHERE %s", tx.statement.tableName, wheres)
	case operationCount:
		// count operation
		var wheres string

		conditions := make([]string, 0)
		for _, where := range tx.statement.wheres {
			conditions = append(conditions, where.condition)

			tx.statement.values = append(tx.statement.values, where.values...)
		}

		wheres = strings.Join(conditions, " AND ")

		tx.statement.query = fmt.Sprintf("SELECT COUNT("+tx.statement.countColumn+") as count FROM %s WHERE %s", tx.statement.tableName, wheres)
	}

	return nil
}

func (tx *db) buildScans() (*map[string]interface{}, error) {
	scans := map[string]interface{}{}

	selects := []string{}
	if len(tx.statement.selects) > 0 {
		selects = tx.statement.selects
	} else {
		for _, column := range tables[tx.statement.tableName].columnsByColumnName {
			selects = append(selects, column.columnName)
		}
	}

	for _, s := range selects {
		columnName := tables[tx.statement.tableName].columnsByColumnName[s].columnName

		switch tables[tx.statement.tableName].columnsByColumnName[s].fieldType {
		case "string":
			value := new(string)
			if tables[tx.statement.tableName].columnsByColumnName[s].fieldIsPointer {
				scans[columnName] = &value
			} else {
				scans[columnName] = value
			}
		case "int":
			value := new(int)
			if tables[tx.statement.tableName].columnsByColumnName[s].fieldIsPointer {
				scans[columnName] = &value
			} else {
				scans[columnName] = value
			}
		case "int32":
			value := new(int32)
			if tables[tx.statement.tableName].columnsByColumnName[s].fieldIsPointer {
				scans[columnName] = &value
			} else {
				scans[columnName] = value
			}
		case "int64":
			value := new(int64)
			if tables[tx.statement.tableName].columnsByColumnName[s].fieldIsPointer {
				scans[columnName] = &value
			} else {
				scans[columnName] = value
			}
		case "uint":
			value := new(uint)
			if tables[tx.statement.tableName].columnsByColumnName[s].fieldIsPointer {
				scans[columnName] = &value
			} else {
				scans[columnName] = value
			}
		case "uint32":
			value := new(uint32)
			if tables[tx.statement.tableName].columnsByColumnName[s].fieldIsPointer {
				scans[columnName] = &value
			} else {
				scans[columnName] = value
			}
		case "uint64":
			value := new(uint64)
			if tables[tx.statement.tableName].columnsByColumnName[s].fieldIsPointer {
				scans[columnName] = &value
			} else {
				scans[columnName] = value
			}
		case "map[int]string":
			value := new(map[int]string)
			if tables[tx.statement.tableName].columnsByColumnName[s].fieldIsPointer {
				scans[columnName] = &value
			} else {
				scans[columnName] = value
			}
		case "map[string]string":
			value := new(map[string]string)
			if tables[tx.statement.tableName].columnsByColumnName[s].fieldIsPointer {
				scans[columnName] = &value
			} else {
				scans[columnName] = value
			}
		default:
		}
	}

	return &scans, nil
}

func (tx *db) execFindOne() error {
	defer debug(&tx.statement.query, &tx.statement.values, time.Now())

	//timezone := new(int)
	//status := new(int)
	scans, err := tx.buildScans()
	if err != nil {
		return err
	}

	/*
		resultMap := map[string]interface{}{}

		for _, s := range tx.statement.selects {
			columnName := tables[tx.statement.tableName].columnsByColumnName[s].columnName
			if tables[tx.statement.tableName].columnsByColumnName[s].fieldType == "string" {
				value := new(string)
				if tables[tx.statement.tableName].columnsByColumnName[s].fieldIsPointer {
					resultMap[columnName] = &value
				} else {
					resultMap[columnName] = value
				}
			}

			if tables[tx.statement.tableName].columnsByColumnName[s].fieldType == "int" {
				value := new(int)
				if tables[tx.statement.tableName].columnsByColumnName[s].fieldIsPointer {
					resultMap[columnName] = &value
				} else {
					resultMap[columnName] = value
				}
			}
		}
	*/

	iter := tx.session.
		Query(tx.statement.query, tx.statement.values...).
		Consistency(gocql.All).
		Iter()

	if iter.NumRows() > 0 {

		for iter.MapScan(*scans) {
			for columnName, columnValue := range *scans {
				// if column is not defined in struct, skip
				if _, ok := tables[tx.statement.tableName].columnsByColumnName[columnName]; !ok {
					continue
				}

				fieldName := tables[tx.statement.tableName].columnsByColumnName[columnName].fieldName
				fieldValue := reflect.ValueOf(tx.model.entity).Elem().FieldByName(fieldName)

				var value interface{}

				if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldType == "string" {
					if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldIsPointer {
						value = columnValue.(*string)
					} else {
						value = columnValue.(string)
					}
				}

				if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldType == "int" {
					if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldIsPointer {
						value = columnValue.(*int)
					} else {
						value = columnValue.(int)
					}
				}

				if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldType == "uint" {
					if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldIsPointer {
						value = columnValue.(*uint)
					} else {
						value = columnValue.(uint)
					}
				}

				if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldType == "int64" {
					if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldIsPointer {
						value = columnValue.(*int64)
					} else {
						value = columnValue.(int64)
					}
				}

				if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldType == "uint64" {
					if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldIsPointer {
						value = columnValue.(*uint64)
					} else {
						value = columnValue.(uint64)
					}
				}

				if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldType == "map[int]string" {
					if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldIsPointer {
						value = columnValue.(*map[int]string)
					} else {
						value = columnValue.(map[int]string)
					}
				}

				if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldType == "map[string]string" {
					if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldIsPointer {
						value = columnValue.(*map[string]string)
					} else {
						value = columnValue.(map[string]string)
					}
				}

				fieldValue.Set(reflect.ValueOf(value))
			}
		}
	}

	if err := iter.Close(); err != nil {
		tx.resetDB()
		return err
	}

	if iter.NumRows() == 0 {
		tx.resetDB()
		return ErrRecordNotFound
	}

	tx.resetDB()

	return nil
}

func (tx *db) execFindAll() error {
	defer debug(&tx.statement.query, &tx.statement.values, time.Now())

	/*
		scans, err := tx.buildScans()
		if err != nil {
			return err
		}
	*/

	iter := tx.session.
		Query(tx.statement.query, tx.statement.values...).
		Consistency(gocql.All).
		Iter()

	count := iter.NumRows()

	sliceValue := reflect.ValueOf(tx.model.entity).Elem()
	slice := reflect.MakeSlice(sliceValue.Type(), count, count)

	index := 0
	for {
		scans, err := tx.buildScans()
		if err != nil {
			return err
		}
		if !iter.MapScan(*scans) {
			break
		}

		for columnName, columnValue := range *scans {
			fieldName := tables[tx.statement.tableName].columnsByColumnName[columnName].fieldName
			fieldValue := slice.Index(index).FieldByName(fieldName)

			if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldType == "string" {
				if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldIsPointer {
					value := columnValue.(*string)
					fieldValue.Set(reflect.ValueOf(value))
				} else {
					value := columnValue.(string)
					fieldValue.Set(reflect.ValueOf(value))
				}
			}

			if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldType == "int" {
				if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldIsPointer {
					value := columnValue.(*int)
					fieldValue.Set(reflect.ValueOf(value))
				} else {
					value := columnValue.(int)
					fieldValue.Set(reflect.ValueOf(value))
				}
			}

			if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldType == "int64" {
				if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldIsPointer {
					value := columnValue.(*int64)
					fieldValue.Set(reflect.ValueOf(value))
				} else {
					value := columnValue.(int64)
					fieldValue.Set(reflect.ValueOf(value))
				}
			}

			if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldType == "map[int]string" {
				if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldIsPointer {
					value := columnValue.(*map[int]string)
					fieldValue.Set(reflect.ValueOf(value))
				} else {
					value := columnValue.(map[int]string)
					fieldValue.Set(reflect.ValueOf(value))
				}
			}

			if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldType == "map[string]string" {
				if tables[tx.statement.tableName].columnsByColumnName[columnName].fieldIsPointer {
					value := columnValue.(*map[string]string)
					fieldValue.Set(reflect.ValueOf(value))
				} else {
					value := columnValue.(map[string]string)
					fieldValue.Set(reflect.ValueOf(value))
				}
			}
		}

		index += 1
	}

	if err := iter.Close(); err != nil {
		tx.resetDB()
		return err
	}

	sliceValue.Set(slice)

	tx.resetDB()

	return nil
}

func (tx *db) execWriteOne() error {
	defer debug(&tx.statement.query, &tx.statement.values, time.Now())

	err := tx.session.
		Query(tx.statement.query, tx.statement.values...).
		Exec()

	if err != nil {
		tx.resetDB()
		return err
	}

	tx.resetDB()

	return nil
}

func (tx *db) execCount() error {
	defer debug(&tx.statement.query, &tx.statement.values, time.Now())

	tx.resetDB()

	return nil
}

func debug(query *string, binds *[]interface{}, startTime time.Time) {
	if query == nil || binds == nil {
		return
	}

	excuteTime := time.Since(startTime)

	fmt.Println("cassago debug info")
	fmt.Println("query:", *query)
	fmt.Println("bind:", *binds)
	fmt.Println("execute time:", excuteTime.Milliseconds())
}
