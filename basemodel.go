package qlx

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/iancoleman/strcase"
	"modernc.org/ql"
)

type BaseModel struct {
	Type      reflect.Type
	Dsn       string //file path,like : ./app.db or [empty]
	Pool      *ql.DB
	TableName string

	dbTags  []string
	qlTypes []string
}

func NewBaseModel(dsn string, data interface{}) (*BaseModel, error) {
	model, _, e := NewBaseModelWithCreated(dsn, data)
	return model, e
}

func NewBaseModelWithCreated(dsn string, data interface{}) (*BaseModel, bool, error) {
	created := false
	t := reflect.TypeOf(data)
	model := &BaseModel{
		Type:      t,
		Dsn:       dsn,
		TableName: ToTableName(t.Name()),
	}
	var e error
	if dsn == "" {
		model.Pool, e = ql.OpenMem()
		if e != nil {
			log.Println(e)
			return nil, false, e
		}
	} else {
		model.Pool, e = ql.OpenFile(dsn, &ql.Options{CanCreate: true})
		if e != nil {
			log.Println(e)
			return nil, false, e
		}
	}

	//check data
	if t.Kind() == reflect.Ptr {
		return nil, false, errors.New("data must be struct type")
	}

	indexes := make(map[string]string)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		//dbTag
		dbTag, ok := field.Tag.Lookup("db")
		if !ok {
			return nil, false, errors.New("field " + field.Name + " has no `db` tag specified")
		}
		if i == 0 {
			if dbTag != "id" || field.Type.Kind() != reflect.Int64 {
				return nil, false, errors.New("The first field must be id int64")
			}
			continue
		}
		if dbTag != strcase.ToSnake(dbTag) {
			return nil, false, errors.New("Field '" + field.Name + "'s `db` tag is not in snake case")
		}

		//index
		if index, ok := field.Tag.Lookup("index"); ok {
			indexes[dbTag] = index
		}

		qlType, e := ToQlType(field.Type, dbTag)
		if e != nil {
			log.Println(e)
			return nil, false, fmt.Errorf("Field %s:%w", field.Name, e)
		}

		model.dbTags = append(model.dbTags, dbTag)
		model.qlTypes = append(model.qlTypes, qlType)
	}

	//desc
	remoteColumnList, e := DescTable(model.Pool, model.TableName)
	if e != nil {
		log.Println(e)
		return nil, false, e
	}

	//create table
	if len(remoteColumnList) == 0 {
		e = model.createTable()
		if e != nil {
			log.Println(e)
			return nil, false, e
		}
		//create index
		e = model.createIndexFromField(localIndexList)
		if e != nil {
			log.Println(e)
			return nil, false, e
		}
		return model, true, nil
	}
	return model, false, nil
}

func (b *BaseModel) GetCreateTableSQL() string {
	builder := new(strings.Builder)
	builder.WriteString(`create table ` + b.TableName + ` (`)
	for i, dbTag := range b.dbTags {
		if i == 0 {
			continue
		}
		builder.WriteString(dbTag + " ")
		builder.WriteString(b.qlTypes[i])
		if i < len(b.dbTags)-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString(")")
	return builder.String()
}

func (b *BaseModel) createTable() error {
	query := b.GetCreateTableSQL()
	_, _, e := b.Pool.Run(ql.NewRWCtx(), query)
	if e != nil {
		return fmt.Errorf("%w: %s", e, query)
	}
	return nil
}

func (b *BaseModel) addColumn(name, typ string) error {
	_, _, e := b.Pool.Run(ql.NewRWCtx(), `alter table `+b.TableName+` add column `+name+` `+typ)
	if e != nil {
		log.Println(e)
		return e
	}
	return nil
}

func (b *BaseModel) dropColumn(name string) error {
	_, _, e := b.Pool.Run(ql.NewRWCtx(), `alter table `+b.TableName+` drop column `+name)
	if e != nil {
		log.Println(e)
		return e
	}
	return nil
}
