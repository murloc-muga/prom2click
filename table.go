package main

import (
	"database/sql"
	"fmt"

	"github.com/prometheus/common/log"
)

var createDB = `CREATE DATABASE IF NOT EXISTS %s`
var createTable = `CREATE TABLE IF NOT EXISTS %s.%s
(
      date Date DEFAULT toDate(0),
      name String,
      tags Array(String),
      val Float64,
      ts DateTime,
      updated DateTime DEFAULT now()
) ENGINE = GraphiteMergeTree('graphite_rollup') 
PARTITION BY date 
ORDER BY (name, tags, ts) 
PRIMARY KEY (name, tags, ts)`

func CreateDBTable(db *sql.DB, database, table string) error {
	tx, err := db.Begin()
	if err != nil {
		log.Errorln(err)
		return err
	}
	_, err = tx.Exec(fmt.Sprintf(createDB, database))
	if err != nil {
		log.Errorln(err)
		tx.Rollback()
		return err
	}
	_, err = tx.Exec(fmt.Sprintf(createTable, database, table))
	if err != nil {
		log.Errorln(err)
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		log.Errorf("commit error, %s", err.Error())
		return err
	}
	return nil
}
