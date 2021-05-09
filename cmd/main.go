package main

import (
	"fmt"
	"log"

	"github.com/StevenZack/qlx"
)

type Student struct {
	Id         int64 `ql:"id"`
	Age        int64 `ql:"age" index:"unique"`
	UpdateTime int64 `ql:"update_time" index:""`
	CreateTime int64 `ql:"create_time"`
}

func init() {
	log.SetFlags(log.Lshortfile)
}

func main() {
	b, e := qlx.NewBaseModel("./app.db", Student{})
	if e != nil {
		log.Fatal(e)
	}

	v, e := b.ExistsWhere("")
	if e != nil {
		log.Fatal(e)
	}

	fmt.Println(v)
}
