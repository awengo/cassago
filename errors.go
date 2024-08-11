package cassago

import "errors"

var ErrConfigNotCorrect = errors.New("config  is not correct")
var ErrModelNotPointer = errors.New("model must be a pointer")
var ErrDatabaseConnect = errors.New("database connect error")
var ErrRecordNotFound = errors.New("record not found")
var ErrNotStructKind = errors.New("model must be a struct")
var ErrEntityKindNotFound = errors.New("model must be a struct or slice")
var ErrOperationNotSupported = errors.New("db operation not supported")
var ErrUpdateNotFound = errors.New("update not found")
var ErrTableNameFound = errors.New("table name not found")
var ErrTXNotBegin = errors.New("tx not begin")
