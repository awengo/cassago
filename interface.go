package cassago

import "github.com/gocql/gocql"

type dbInterface interface {
	Find(interface{}) dbInterface
	Create(interface{}) dbInterface
	Patch(interface{}, *map[string]interface{}) dbInterface
	Increase(interface{}, *map[string]interface{}) dbInterface
	Delete(interface{}) dbInterface
	Count(interface{}, string, *int64) dbInterface
	Select(...string) dbInterface
	Group(...string) dbInterface
	Where(string, ...interface{}) dbInterface
	Limit(int) dbInterface
	Exec() error
}

type db struct {
	session   *gocql.Session
	statement *dbStatement
	model     *model
	err       error
}
