package cassago

import (
	"reflect"
)

const TagName = "cassago"

type model struct {
	entity          interface{}
	entityType      reflect.Kind // struct or slice
	entityIsPointer bool
}

func (tx *db) buildModel(entity interface{}) error {
	if tx.err != nil {
		return tx.err
	}

	if tx.statement == nil {
		tx.statement = &dbStatement{}
	}

	tx.model = &model{
		entity: entity,
	}

	entityValue := reflect.ValueOf(entity)
	if entityValue.Kind() != reflect.Ptr {
		return ErrModelNotPointer
	}

	entityValue = entityValue.Elem()

	switch entityValue.Kind() {
	case reflect.Slice:
		// if model entity is slice, get slice element type

		tx.model.entityType = reflect.Slice

		if err := tx.buildTableFromStructSlice(entityValue); err != nil {
			return err
		}

		// db.statement = statement

		return nil
	case reflect.Struct:
		// if model entity is struct, get struct element type

		tx.model.entityType = reflect.Struct
		tx.model.entityIsPointer = true // if model entity is struct, it must be pointer

		if err := tx.buildTableFromStruct(entity); err != nil {
			return err
		}

		// db.statement = statement

		return nil
	default:
		return ErrEntityKindNotFound
	}

	/*
		if modelValue.Kind() == reflect.Slice {

			newSlice := reflect.MakeSlice(modelValue.Type(), 1, 1)

			fmt.Println("##### kind:", newSlice.Len())
			fmt.Println("###### kind:", newSlice.Kind())

			for i := 0; i < newSlice.Len(); i++ {
				item := newSlice.Index(i)
				fmt.Println("#", item.Kind())
				if item.Kind() == reflect.Struct {
					tableName, err := getTableName(item.Interface())
					fmt.Println("##", err)
					fmt.Println("##", tableName)
					fmt.Println("##", item.Type().Name())
					//	v := reflect.Indirect(item)
					//	for j := 0; j < v.NumField(); j++ {
					//		fmt.Println(v.Type().Field(j).Name, v.Field(j).Interface())
					//	}
				}
			}
			return nil

			//modelValue := reflect.TypeOf(modelValue)

			fmt.Println("##### kind:", reflect.TypeOf(modelValue).Kind())

			return db.buildModel(reflect.TypeOf(modelValue))

			//tableName, err := getTableName(model)
			//fmt.Println()

		}

		fmt.Println("kind: ", modelValue.Kind())

		if modelValue.Kind() != reflect.Struct {
			return ErrNotStructKind
		}

		tableName, err := getTableName(entity)
		if err != nil {
			return err
		}

		fmt.Println("table name: ", tableName)

		db.statement.tableName = tableName

		db.statement, err = db.statement.buildTableFromStruct(entity)

		if err != nil {
			return err
		}

		return nil

		modelType := modelValue.Type()
		for i := 0; i < modelValue.NumField(); i++ {
			_column := column{}

			field := modelValue.Field(i)
			_column.fieldType = field.Type().String()

			if field.Kind() == reflect.Ptr {
				_column.fieldIsPointer = true
				_column.fieldType = strings.TrimPrefix(_column.fieldType, "*")
			}

			_column.fieldName = modelType.Field(i).Name

			tag := modelType.Field(i).Tag.Get(TagName)
			if tag != "" {
				if tag == "-" {
					continue
				}

				tagDetails := strings.Split(tag, ";")
				for _, tagDetail := range tagDetails {
					tagKV := strings.Split(tagDetail, ":")
					if len(tagKV) == 2 {
						if tagKV[0] == "column" {
							if tagKV[1] == "" || tagKV[1] == "-" {
								continue
							}

							_column.columnName = tagKV[1]
						}
					}

					if tagDetail == "pk" {
						_column.fieldIsPK = true
						continue
					}

					if tagDetail == "ck" {
						_column.fieldIsCK = true
						continue
					}
				}
			}

			if _column.columnName == "" {
				_column.columnName = snaker.CamelToSnake(_column.fieldName)
			}

			if tables[tableName] == nil {
				tables[tableName] = &table{}
			}

			if tables[tableName].columnsByColumnName == nil {
				tables[tableName].columnsByColumnName = make(map[string]column)
			}

			tables[tableName].columnsByColumnName[_column.columnName] = _column
		}

		// db.model = model
	*/

	return nil
}
