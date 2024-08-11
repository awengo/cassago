package cassago

import (
	"reflect"
	"strings"

	"github.com/serenize/snaker"
)

type column struct {
	fieldName      string // field name in struct
	columnName     string // column name in database
	fieldType      string // field type in struct
	columnType     string // column type in database
	fieldIsPointer bool   // field is pointer
	fieldIsPK      bool   // field is partion key
	fieldIsCK      bool   // field is cluster key
}

type table struct {
	isPointer           bool              // entity is pointer
	structName          string            // entity struct name
	tableName           string            // table name in database
	columnsByFieldName  map[string]column // columns in table
	columnsByColumnName map[string]column // columns in table
	hasCount            bool              // has count column
}

// table by tableName
var tables = make(map[string]*table)

func getTableName(model interface{}) (string, error) {

	// if struct has method TableName() string, use it
	value := reflect.ValueOf(model)

	entityType := reflect.TypeOf(model)
	if entityType.Kind() == reflect.Ptr {
		value = value.Elem()
		//return "", ErrModelNotPointer
	}

	method := value.MethodByName("TableName")
	if method.IsValid() {
		return method.Call([]reflect.Value{})[0].String(), nil
	}

	var structName string
	if entityType.Kind() == reflect.Ptr {
		structName = entityType.Elem().Name()
	} else {
		structName = entityType.Name()
	}

	if structName == "" {
		return "", ErrTableNameFound
	}

	return snaker.CamelToSnake(structName), nil
}

func (tx *db) buildTableFromStructSlice(sliceValue reflect.Value) error {
	Debug("tableStructSlice:", sliceValue)

	slice := reflect.MakeSlice(sliceValue.Type(), 1, 1)

	entity := slice.Index(0)
	if entity.Kind() == reflect.Struct {
		tx.model.entityIsPointer = false
		if err := tx.buildTableFromStruct(entity.Interface()); err != nil {
			return err
		}

		return nil
	}

	return ErrNotStructKind
}

func (tx *db) buildTableFromStruct(tableStruct interface{}) error {
	entityValue := reflect.ValueOf(tableStruct)
	if entityValue.Kind() == reflect.Ptr {
		// return ErrModelNotPointer
		entityValue = entityValue.Elem()
	}

	if entityValue.Kind() != reflect.Struct {
		return ErrNotStructKind
	}

	tableName, err := getTableName(tableStruct)
	if err != nil {
		return err
	}

	tx.statement.tableName = tableName

	if _, ok := tables[tableName]; ok {
		Debug("hit table from cache. table name:", tableName)
		return nil
	}

	entityType := entityValue.Type()

	if tables[tableName] == nil {
		tables[tableName] = &table{}
	}

	for i := 0; i < entityValue.NumField(); i++ {
		if entityType.Field(i).Name == "Count" {
			tables[tableName].hasCount = true
			continue
		}

		_column := column{}

		field := entityValue.Field(i)
		_column.fieldType = field.Type().String()

		if field.Kind() == reflect.Ptr {
			_column.fieldIsPointer = true
			_column.fieldType = strings.TrimPrefix(_column.fieldType, "*")
		}

		_column.fieldName = entityType.Field(i).Name

		tag := entityType.Field(i).Tag.Get(TagName)
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

		if tables[tableName].columnsByColumnName == nil {
			tables[tableName].columnsByColumnName = make(map[string]column)
		}

		tables[tableName].columnsByColumnName[_column.columnName] = _column
	}

	return nil
}
