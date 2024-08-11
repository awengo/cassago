package cassago

const defaultLimit = 100

type where struct {
	condition string
	values    []interface{}
}

func EQ(value interface{}) condition {
	return condition{eq: value}
}

func IN(values ...interface{}) condition {
	return condition{in: values}
}

func (tx *db) Select(selects ...string) dbInterface {
	if tx.err != nil {
		return tx
	}

	if tx.statement == nil {
		tx.err = ErrTXNotBegin
		return tx
	}

	tx.statement.selects = selects

	return tx
}

func (tx *db) Where(condition string, values ...interface{}) dbInterface {
	if tx.err != nil {
		return tx
	}

	if tx.statement == nil {
		tx.err = ErrTXNotBegin
		return tx
	}

	where := where{
		condition: condition,
		values:    values,
	}

	if tx.statement == nil {
		tx.statement = &dbStatement{}
	}

	tx.statement.wheres = append(tx.statement.wheres, where)

	return tx
}

func (tx *db) Limit(limit int) dbInterface {
	if tx.err != nil {
		return tx
	}

	if tx.statement == nil {
		tx.err = ErrTXNotBegin
		return tx
	}

	if limit < 0 {
		limit = defaultLimit
	}

	if limit > 0 {
		tx.statement.limit = limit
	}

	return tx
}

func (tx *db) Group(groups ...string) dbInterface {
	if tx.err != nil {
		return tx
	}

	if tx.statement == nil {
		tx.err = ErrTXNotBegin
		return tx
	}

	tx.statement.groups = groups

	return tx
}
