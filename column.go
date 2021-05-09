package qlx

import (
	"fmt"
	"log"

	"modernc.org/ql"
)

type Column struct {
	TableName string `ql:"TableName"`
	Ordinal   int64  `ql:"ordinal"`
	Name      string `ql:"name"`
	Type      string `ql:"type"`
}

func DescTable(pool *ql.DB, tableName string) ([]Column, error) {
	rss, _, e := pool.Run(ql.NewRWCtx(), `select * from __Column where TableName=$1`, tableName)
	if e != nil {
		log.Println(e)
		return nil, e
	}
	if len(rss) == 0 {
		return nil, nil
	}

	out := []Column{}
	e = rss[0].Do(false, func(data []interface{}) (more bool, err error) {
		v := &Column{}
		e := ql.Unmarshal(v, data)
		if e != nil {
			log.Println(e)
			return false, e
		}

		out = append(out, *v)
		return true, nil
	})
	if e != nil {
		log.Println(e)
		return nil, e
	}

	return out, nil
}

func (b *BaseModel) addColumn(name, typ string) error {
	query := `BEGIN TRANSACTION;alter table ` + b.TableName + ` add ` + name + ` ` + typ + `;COMMIT;`
	_, _, e := b.Pool.Execute(ql.NewRWCtx(), ql.MustCompile(query))
	if e != nil {
		return fmt.Errorf("%w:%s", e, query)
	}
	return nil
}

func (b *BaseModel) dropColumn(name string) error {
	query := `BEGIN TRANSACTION;alter table ` + b.TableName + ` drop column ` + name + ";COMMIT;"
	_, _, e := b.Pool.Execute(ql.NewRWCtx(), ql.MustCompile(query))
	if e != nil {
		return fmt.Errorf("%w:%s", e, query)
	}
	return nil
}
