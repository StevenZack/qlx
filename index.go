package qlx

import (
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"

	"modernc.org/ql"
)

type Index struct {
	TableName  string `ql:"TableName"`
	ColumnName string `ql:"ColumnName"`
	Name       string `ql:"Name"`
	IsUnique   bool   `ql:"isUnique"`
}

func (i Index) ToIndexName() string {
	cn := i.ColumnName
	if cn == "id()" {
		cn = "id_pkey"
	}
	return i.TableName + "_" + cn + "_idx"
}

func toIndexList(indexes map[string]string, tableName string) ([]Index, error) {
	indexList := []Index{}
	for key, idxStr := range indexes {
		vs, e := url.ParseQuery(strings.ReplaceAll(idxStr, ",", "&"))
		if e != nil {
			return nil, errors.New("field '" + key + "', invalid index tag format:" + idxStr)
		}

		index := Index{}
		for k := range vs {
			switch k {
			case "unique", "uniq":
				index.IsUnique = true
			default:
				return nil, errors.New("field '" + key + "', unsupported key:" + k)
			}
		}

		//normal index
		index.ColumnName = key
		index.TableName = tableName
		indexList = append(indexList, index)
	}

	return indexList, nil
}

func (b *BaseModel) createIndexFromField(indexList []Index) error {
	if len(indexList) == 0 {
		return nil
	}

	for _, idx := range indexList {
		e := b.createIndex(idx)
		if e != nil {
			log.Println(e)
			return e
		}
	}
	return nil
}

func (b *BaseModel) createIndex(idx Index) error {
	idx.TableName = b.TableName

	builder := new(strings.Builder)
	builder.WriteString("BEGIN TRANSACTION;")
	builder.WriteString("create ")
	if idx.IsUnique {
		builder.WriteString("unique ")
	}
	builder.WriteString("index ")
	builder.WriteString(idx.ToIndexName())
	builder.WriteString(" on " + b.TableName + " (")
	builder.WriteString(idx.ColumnName)
	builder.WriteString(");")
	builder.WriteString("COMMIT;")

	query := builder.String()
	_, _, e := b.Pool.Run(ql.NewRWCtx(), query)
	if e != nil {
		return fmt.Errorf("%w:%s", e, query)
	}
	return nil
}

func (b *BaseModel) GetIndexes() ([]Index, error) {
	rss, _, e := b.Pool.Run(ql.NewRWCtx(), `select * from __Index where TableName=$1`, b.TableName)
	if e != nil {
		log.Println(e)
		return nil, e
	}
	if len(rss) == 0 {
		return nil, nil
	}

	out := []Index{}
	e = rss[0].Do(false, func(data []interface{}) (more bool, err error) {
		v := Index{}
		e := ql.Unmarshal(&v, data)
		if e != nil {
			log.Println(e)
			return false, e
		}

		out = append(out, v)
		return true, nil
	})
	if e != nil {
		log.Println(e)
		return nil, e
	}
	return out, nil
}

func (b *BaseModel) dropIndex(name string) error {
	query := `BEGIN TRANSACTION;drop index ` + name + ";COMMIT;"
	_, _, e := b.Pool.Run(ql.NewRWCtx(), query)
	if e != nil {
		log.Println(e)
		return fmt.Errorf("%w:`%s`", e, query)
	}
	return nil
}
