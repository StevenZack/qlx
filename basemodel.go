package qlx

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
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

/** NewBaseModel creates a *BaseModel.
1.dsn: file path or empty string. File path like "./app.db"; Empty string for memory database.
2.data: Struct{}
*/
func NewBaseModel(dsn string, data interface{}) (*BaseModel, error) {
	model, _, e := NewBaseModelWithCreated(dsn, data)
	return model, e
}

func NewBaseModelWithCreated(dsn string, data interface{}) (*BaseModel, bool, error) {
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
		dbTag, ok := field.Tag.Lookup("ql")
		if !ok {
			return nil, false, errors.New("field " + field.Name + " has no `ql` tag specified")
		}
		if i == 0 {
			if dbTag != "id" || field.Type.Kind() != reflect.Int64 {
				return nil, false, errors.New("The first field must be id int64")
			}
			continue
		}
		if dbTag != strcase.ToSnake(dbTag) {
			return nil, false, errors.New("Field '" + field.Name + "'s `ql` tag is not in snake case")
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
	localIndexList, e := toIndexList(indexes, model.TableName)
	if e != nil {
		log.Println(e)
		return nil, false, e
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
		e = model.createIndexFromField(append([]Index{
			{
				ColumnName: "id()",
				IsUnique:   true,
			},
		}, localIndexList...))
		if e != nil {
			log.Println(e)
			return nil, false, e
		}
		return model, true, nil
	}

	//column check
	remoteColumns := make(map[string]Column)
	for _, c := range remoteColumnList {
		remoteColumns[c.Name] = c
	}

	//local columns to be created
	localColumns := make(map[string]string)
	for i, db := range model.dbTags {
		localColumns[db] = model.qlTypes[i]

		remote, ok := remoteColumns[db]
		if !ok {
			// auto-create field on remote database
			log.Println("Remote column '" + db + "' to be created")
			e = model.addColumn(db, model.qlTypes[i])
			if e != nil {
				log.Println(e)
				return nil, false, e
			}
			continue
		}

		//type check
		dbType := strings.Split(model.qlTypes[i], " ")[0]
		if dbType != remote.Type {
			return nil, false, errors.New("Found local field " + db + "'s type '" + dbType + "' doesn't match remote column type:" + remote.Type)
		}
	}

	//remote columns to be dropped
	for _, remote := range remoteColumnList {
		_, ok := localColumns[remote.Name]
		if !ok {
			//auto-drop remote column
			log.Println("Remote column '" + remote.Name + "' to be dropped")
			e = model.dropColumn(remote.Name)
			if e != nil {
				log.Println(e)
				return nil, false, e
			}
			continue
		}
	}

	//index check
	remoteIndexList, e := model.GetIndexes()
	if e != nil {
		log.Println(e)
		return nil, false, e
	}
	remoteIndexes := make(map[string]Index)
	for _, remote := range remoteIndexList {
		remoteIndexes[remote.Name] = remote
	}

	//indexes to be created
	localIndexes := make(map[string]Index)
	for _, local := range localIndexList {
		localIndexes[local.ToIndexName()] = local
		remote, ok := remoteIndexes[local.ToIndexName()]
		if !ok {
			//auto-create index on remote database
			log.Println("Remote index '" + local.ToIndexName() + "' to be created")
			e = model.createIndex(local)
			if e != nil {
				log.Println(e)
				return nil, false, e
			}
			continue
		}

		//unique check
		if local.IsUnique != remote.IsUnique {
			return nil, false, errors.New("Index '" + local.ToIndexName() + "' unique option is inconsistant with remote database: " + strconv.FormatBool(local.IsUnique) + " vs " + strconv.FormatBool(remote.IsUnique))
		}
	}

	//indexes to be dropped
	for _, remote := range remoteIndexList {
		if strings.Contains(remote.Name, "_pkey") {
			continue
		}
		_, ok := localIndexes[remote.Name]
		if !ok {
			log.Println("Remote index '" + remote.Name + "' to be dropped")
			e = model.dropIndex(remote.Name)
			if e != nil {
				log.Println(e)
				return nil, false, e
			}
			continue
		}
	}

	return model, false, nil
}

func (b *BaseModel) GetCreateTableSQL() string {
	builder := new(strings.Builder)
	builder.WriteString("BEGIN TRANSACTION;")
	builder.WriteString(`create table ` + b.TableName + ` (`)
	for i, dbTag := range b.dbTags {
		builder.WriteString(dbTag + " ")
		builder.WriteString(b.qlTypes[i])
		if i < len(b.dbTags)-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString(");")
	builder.WriteString("COMMIT;")
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

func (b *BaseModel) GetInsertSQL() ([]int, string) {
	builder := new(strings.Builder)
	builder.WriteString("BEGIN TRANSACTION;insert into " + b.TableName + ` (`)

	values := new(strings.Builder)
	values.WriteString("values (")

	argsIndex := []int{}
	for i, dbTag := range b.dbTags {
		argsIndex = append(argsIndex, i+1)

		builder.WriteString(dbTag)
		values.WriteString("$" + strconv.Itoa(len(argsIndex)))

		if i < len(b.dbTags)-1 {
			builder.WriteString(",")
			values.WriteString(",")
		}
	}

	builder.WriteString(")")
	values.WriteString(")")

	builder.WriteString(values.String())
	builder.WriteString(";COMMIT;")
	return argsIndex, builder.String()
}

func (b *BaseModel) GetSelectSQL() string {
	builder := new(strings.Builder)
	builder.WriteString("select id(),")
	for i, dbTag := range b.dbTags {
		builder.WriteString(dbTag)
		if i < len(b.dbTags)-1 {
			builder.WriteString(",")
		}
	}
	builder.WriteString(" from " + b.TableName)
	return builder.String()
}

func (b *BaseModel) Insert(v interface{}) error {
	//validate
	value := reflect.ValueOf(v)
	t := value.Type()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		value = value.Elem()
	}
	if t.String() != b.Type.String() {
		return errors.New("Wrong insert type:" + t.String() + " for table " + b.TableName)
	}

	//args
	argsIndex, query := b.GetInsertSQL()
	args := []interface{}{}
	for _, i := range argsIndex {
		field := value.Field(i)
		args = append(args, field.Interface())
	}

	//exec
	rss, _, e := b.Pool.Run(ql.NewRWCtx(), query, args...)
	if e != nil {
		log.Println(e)
		return fmt.Errorf("%w:`%s`", e, query)
	}
	for _, rs := range rss {
		rs.Do(false, func(data []interface{}) (more bool, err error) {
			fmt.Println(data)
			return true, nil
		})
	}
	return nil
}

func (b *BaseModel) InsertAll(vs interface{}) error {
	//validate
	sliceValue := reflect.ValueOf(vs)
	t := sliceValue.Type()
	if t.Kind() != reflect.Slice {
		return errors.New("Insert value is not an slice type:" + t.String())
	}
	t = t.Elem()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.String() != b.Type.String() {
		return errors.New("Wrong insert type:" + t.String() + " for table " + b.TableName)
	}

	for i := 0; i < sliceValue.Len(); i++ {
		value := sliceValue.Index(i)
		if value.Kind() == reflect.Ptr {
			value = value.Elem()
		}

		e := b.Insert(value.Interface())
		if e != nil {
			log.Println(e)
			return e
		}
	}
	return nil
}

func (b *BaseModel) Find(id int64) (interface{}, error) {
	//scan
	v := reflect.New(b.Type)
	query := b.GetSelectSQL()

	query = query + ` where id()=$1`
	rss, _, e := b.Pool.Run(ql.NewRWCtx(), query, id)
	if e != nil {
		return nil, fmt.Errorf("%w:%s", e, query)
	}

	if len(rss) == 0 {
		return nil, sql.ErrNoRows
	}

	row, e := rss[0].FirstRow()
	if e != nil {
		return nil, e
	}
	if len(row) == 0 {
		return nil, sql.ErrNoRows
	}
	e = ql.Unmarshal(v.Interface(), row)
	if e != nil {
		return nil, e
	}

	return v.Interface(), nil
}

func (b *BaseModel) FindWhere(where string, args ...interface{}) (interface{}, error) {
	//where
	where = toWhere(where)
	query := b.GetSelectSQL()
	query = query + where

	//scan
	v := reflect.New(b.Type)
	rss, _, e := b.Pool.Run(ql.NewRWCtx(), query, args...)
	if e != nil {
		return nil, e
	}

	if len(rss) == 0 {
		return nil, sql.ErrNoRows
	}
	row, e := rss[0].FirstRow()
	if e != nil {
		return nil, e
	}
	if len(row) == 0 {
		return nil, sql.ErrNoRows
	}

	e = ql.Unmarshal(v.Interface(), row)
	if e != nil {
		return nil, e
	}
	return v.Interface(), nil
}

func (b *BaseModel) QueryWhere(where string, args ...interface{}) (interface{}, error) {
	where = toWhere(where)

	query := b.GetSelectSQL()

	//query
	query = query + where
	rss, _, e := b.Pool.Run(ql.NewRWCtx(), query, args...)
	if e != nil {
		return nil, e
	}
	if len(rss) == 0 {
		return nil, sql.ErrNoRows
	}

	vs := reflect.MakeSlice(reflect.SliceOf(reflect.PtrTo(b.Type)), 0, 2)
	e = rss[0].Do(false, func(data []interface{}) (more bool, err error) {
		v := reflect.New(b.Type)
		e = ql.Unmarshal(v.Interface(), data)
		if e != nil {
			return false, e
		}

		vs = reflect.Append(vs, v)
		return true, nil
	})
	if e != nil {
		return nil, e
	}

	return vs.Interface(), nil
}

func (b *BaseModel) Exists(id int64) (bool, error) {
	query := `select 1 from ` + b.TableName + ` where id()=$1 limit 1`
	rss, _, e := b.Pool.Run(ql.NewRWCtx(), query, id)
	if e != nil {
		return false, e
	}

	if len(rss) == 0 {
		return false, sql.ErrNoRows
	}
	row, e := rss[0].FirstRow()
	if e != nil {
		return false, e
	}
	if len(row) == 0 {
		return false, nil
	}
	return true, nil
}

func (b *BaseModel) ExistsWhere(where string, args ...interface{}) (bool, error) {
	//where
	where = toWhere(where)

	//scan
	query := `select 1 from ` + b.TableName + where + ` limit 1`
	rss, _, e := b.Pool.Run(ql.NewRWCtx(), query, args...)
	if e != nil {
		return false, e
	}
	if len(rss) == 0 {
		return false, sql.ErrNoRows
	}
	row, e := rss[0].FirstRow()
	if e != nil {
		return false, e
	}

	if len(row) == 0 {
		return false, nil
	}
	return true, nil
}

func (b *BaseModel) CountWhere(where string, args ...interface{}) (int64, error) {
	where = toWhere(where)
	//scan
	query := `select count() from ` + b.TableName + where
	rss, _, e := b.Pool.Run(ql.NewRWCtx(), query, args...)
	if e != nil {
		return 0, e
	}
	if len(rss) == 0 {
		return 0, sql.ErrNoRows
	}

	row, e := rss[0].FirstRow()
	if e != nil {
		return 0, e
	}
	if len(row) == 0 {
		return 0, sql.ErrNoRows
	}
	return row[0].(int64), nil
}

func (b *BaseModel) UpdateSet(sets string, where string, args ...interface{}) error {
	where = toWhere(where)

	query := `BEGIN TRANSACTION;update ` + b.TableName + ` set ` + sets + where + ";COMMIT;"
	_, _, e := b.Pool.Run(ql.NewRWCtx(), query, args...)
	return e
}

func (b *BaseModel) Clear() error {
	_, _, e := b.Pool.Run(ql.NewRWCtx(), `BEGIN TRANSACTION;truncate table `+b.TableName+";COMMIT;")
	return e
}

func (b *BaseModel) Truncate() error {
	return b.Clear()
}

func (b *BaseModel) Delete(id int64) error {
	query := `BEGIN TRANSACTION;delete from ` + b.TableName + ` where id()=$1`
	_, _, e := b.Pool.Run(ql.NewRWCtx(), query, id)
	return e
}

func (b *BaseModel) DeleteWhere(where string, args ...interface{}) error {
	where = toWhere(where)

	query := `BEGIN TRANSACTION; delete from ` + b.TableName + where + `;COMMIT;`
	_, _, e := b.Pool.Run(ql.NewRWCtx(), query)
	return e
}
