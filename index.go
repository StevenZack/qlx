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

func toIndexList(indexes map[string]string) ([]Index, error) {
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
	builder := new(strings.Builder)
	builder.WriteString("create ")
	if idx.IsUnique {
		builder.WriteString("unique ")
	}
	builder.WriteString("index on " + b.TableName + " (")
	builder.WriteString(idx.ColumnName)
	builder.WriteString(")")

	query := builder.String()
	_, _, e := b.Pool.Run(ql.NewRWCtx(), query)
	if e != nil {
		return fmt.Errorf("%w:%s", e, query)
	}
	return nil
}
