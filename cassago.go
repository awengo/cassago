package cassago

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/gocql/gocql"
)

type dbStatement struct {
	operation   int
	selects     []string
	tableName   string
	limit       int
	wheres      []where
	groups      []string
	query       string
	values      []interface{}
	updates     *map[string]interface{}
	countColumn string
}

var dbs = make(map[string]*db)

const (
	operationFindOne = iota
	operationFindAll
	operationCreateOne
	operationCreateAll
	operationUpdateOne
	operationUpdateAll
	operationDeleteOne
	operationDeleteAll
	operationPatchOne
	operationPatchAll
	operationIncreaseOne
	operationIncreaseAll
	operationCount
)

type condition struct {
	eq  interface{}
	gte interface{}
	gt  interface{}
	lte interface{}
	lt  interface{}
	in  []interface{}
}

var session *gocql.Session

func connect() error {
	if session != nil {
		return nil
	}

	if Config == nil {
		return ErrConfigNotCorrect
	}

	if len(Config.Hosts) == 0 || Config.Name == "" {
		return ErrConfigNotCorrect
	}

	cluster := gocql.NewCluster(Config.Hosts...)
	cluster.Keyspace = Config.Name

	cassandraSession, err := cluster.CreateSession()

	if err != nil {
		return ErrDatabaseConnect
	}

	session = cassandraSession

	return nil
}

func (tx *db) Error() error {
	return tx.err
}

func Begin() dbInterface {
	tx := &db{}

	if session == nil {
		if err := connect(); err != nil {
			tx.err = err
			return tx
		}
	}

	tx.session = session
	tx.statement = &dbStatement{}
	tx.model = nil
	tx.err = nil

	return tx
}

func Commit() {

}

func (tx *db) Exec() error {
	if tx.err != nil {
		return tx.err
	}

	if tx.statement == nil {
		return ErrTXNotBegin
	}

	if err := tx.buildQeury(); err != nil {
		return err
	}

	switch tx.statement.operation {
	case operationFindOne:
		return tx.execFindOne()
	case operationFindAll:
		return tx.execFindAll()
	case operationCreateOne,
		operationPatchOne,
		operationDeleteOne,
		operationIncreaseOne:
		return tx.execWriteOne()
	case operationCount:
		return tx.execCount()
	default:
		return ErrOperationNotSupported
	}

	var querySelect string
	if len(tx.statement.selects) > 0 {
		querySelect = strings.Join(tx.statement.selects, ",")
	} else {
		querySelect = "*"
	}

	conditions := make([]string, 0)
	values := make([]interface{}, 0)
	for _, where := range tx.statement.wheres {
		conditions = append(conditions, where.condition)
		values = append(values, where.values...)
	}

	queryConditions := strings.Join(conditions, " AND ")

	fmt.Println("joinWhere: ", queryConditions)
	fmt.Println("values: ", values)

	var area string
	var id string
	if err := tx.session.Query(`SELECT `+querySelect+` FROM members WHERE `+queryConditions+` LIMIT 1`,
		values...).Consistency(gocql.One).Scan(&area, &id); err != nil {
		Error(err)
	}

	return nil
}

func (tx *db) Find(model interface{}) dbInterface {
	if tx.err != nil {
		return tx
	}

	if tx.statement == nil {
		tx.err = ErrTXNotBegin
		return tx
	}

	if err := tx.buildModel(model); err != nil {
		tx.err = err
		return tx
	}

	switch tx.model.entityType {
	case reflect.Struct:
		tx.statement.operation = operationFindOne
	case reflect.Slice:
		tx.statement.operation = operationFindAll
	default:
		tx.err = ErrEntityKindNotFound
		return tx
	}

	//if db.statement == nil {
	//	db.statement = &dbStatement{}
	//}

	// db.model = entity
	// db.statement.operation = operationFindOne

	return tx
}

func (tx *db) Create(model interface{}) dbInterface {
	if tx.err != nil {
		return tx
	}

	if tx.statement == nil {
		tx.err = ErrTXNotBegin
		return tx
	}

	if err := tx.buildModel(model); err != nil {
		tx.err = err
		return tx
	}

	switch tx.model.entityType {
	case reflect.Struct:
		tx.statement.operation = operationCreateOne
	case reflect.Slice:
		tx.statement.operation = operationCreateAll
	default:
		tx.err = ErrEntityKindNotFound
		return tx
	}

	return tx
}

func (tx *db) Update(model interface{}, updates *map[string]interface{}) dbInterface {
	if tx.err != nil {
		return tx
	}

	if tx.statement == nil {
		tx.err = ErrTXNotBegin
		return tx
	}

	if err := tx.buildModel(model); err != nil {
		tx.err = err
		return tx
	}

	switch tx.model.entityType {
	case reflect.Struct:
		tx.statement.operation = operationUpdateOne
	case reflect.Slice:
		tx.statement.operation = operationUpdateAll
	default:
		tx.err = ErrEntityKindNotFound
		return tx
	}

	return tx
}

func (tx *db) Patch(model interface{}, updates *map[string]interface{}) dbInterface {
	if tx.err != nil {
		return tx
	}

	if tx.statement == nil {
		tx.err = ErrTXNotBegin
		return tx
	}

	if updates == nil || len(*updates) == 0 {
		tx.err = ErrUpdateNotFound
		return tx
	}

	if err := tx.buildModel(model); err != nil {
		tx.err = err
		return tx
	}

	tx.statement.updates = updates

	switch tx.model.entityType {
	case reflect.Struct:
		tx.statement.operation = operationPatchOne
	case reflect.Slice:
		tx.statement.operation = operationPatchAll
	default:
		tx.err = ErrEntityKindNotFound
		return tx
	}

	return tx
}

func (tx *db) Delete(model interface{}) dbInterface {
	if tx.err != nil {
		return tx
	}

	if tx.statement == nil {
		tx.err = ErrTXNotBegin
		return tx
	}

	if err := tx.buildModel(model); err != nil {
		tx.err = err
		return tx
	}

	switch tx.model.entityType {
	case reflect.Struct:
		tx.statement.operation = operationDeleteOne
	case reflect.Slice:
		tx.statement.operation = operationDeleteAll
	default:
		tx.err = ErrEntityKindNotFound
		return tx
	}

	return tx
}

func (tx *db) Increase(model interface{}, updates *map[string]interface{}) dbInterface {
	if tx.err != nil {
		return tx
	}

	if tx.statement == nil {
		tx.err = ErrTXNotBegin
		return tx
	}

	if updates == nil || len(*updates) == 0 {
		tx.err = ErrUpdateNotFound
		return tx
	}

	if err := tx.buildModel(model); err != nil {
		tx.err = err
		return tx
	}

	tx.statement.updates = updates

	switch tx.model.entityType {
	case reflect.Struct:
		tx.statement.operation = operationIncreaseOne
	case reflect.Slice:
		tx.statement.operation = operationIncreaseAll
	default:
		tx.err = ErrEntityKindNotFound
		return tx
	}

	return tx
}

func (tx *db) Count(model interface{}, column string, count *int64) dbInterface {
	if tx.err != nil {
		return tx
	}

	if tx.statement == nil {
		tx.err = ErrTXNotBegin
		return tx
	}

	if err := tx.buildModel(model); err != nil {
		tx.err = err
		return tx
	}

	tx.statement.countColumn = column
	tx.statement.operation = operationCount

	return tx
}
