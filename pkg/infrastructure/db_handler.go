package infrastructure

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/itsubaki/hermes-lambda/pkg/interface/database"
)

type Handler struct {
	// sql.DB use connection pooling
	DB *sql.DB
}

func New(driver, datasource, database string) (database.Handler, error) {
	db, err := sql.Open(driver, datasource)
	if err != nil {
		return nil, fmt.Errorf("open: %v", err)
	}

	start := time.Now()
	for {
		if time.Since(start) > 10*time.Minute {
			return nil, fmt.Errorf("db ping time over")
		}

		if err := db.Ping(); err != nil {
			time.Sleep(5 * time.Second)
			continue
		}

		break
	}

	q := fmt.Sprintf("create database if not exists %s", database)
	if _, err := db.Exec(q); err != nil {
		return nil, fmt.Errorf("create database if not exists: %v", err)
	}

	if err := db.Close(); err != nil {
		return nil, fmt.Errorf("close: %v", err)
	}

	source := fmt.Sprintf("%s%s", datasource, database)
	db2, err := sql.Open(driver, source)
	if err != nil {
		return nil, fmt.Errorf("open: %v", err)
	}

	return &Handler{
		DB: db2,
	}, nil
}

func (h *Handler) Query(query string, args ...interface{}) (database.Rows, error) {
	rows, err := h.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return &Rows{rows}, nil
}

func (h *Handler) QueryRow(query string, args ...interface{}) database.Row {
	return h.DB.QueryRow(query, args...)
}

func (h *Handler) Transact(txFunc func(tx database.Tx) error) (err error) {
	tx, err := h.DB.Begin()
	if err != nil {
		return
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}

		if err != nil {
			tx.Rollback()
			return
		}

		err = tx.Commit()
	}()

	return txFunc(&Tx{tx})
}

func (h *Handler) Close() error {
	return h.DB.Close()
}

func (h *Handler) Begin() (database.Tx, error) {
	tx, err := h.DB.Begin()
	if err != nil {
		return nil, err
	}

	return &Tx{tx}, nil
}

type Tx struct {
	Tx *sql.Tx
}

func (tx *Tx) Commit() error {
	return tx.Tx.Commit()
}

func (tx *Tx) Exec(statement string, args ...interface{}) (database.Result, error) {
	result, err := tx.Tx.Exec(statement, args...)
	if err != nil {
		return nil, err
	}

	return &Result{result}, nil
}

func (tx *Tx) Query(statement string, args ...interface{}) (database.Rows, error) {
	rows, err := tx.Tx.Query(statement, args...)
	if err != nil {
		return nil, err
	}

	return &Rows{rows}, nil
}

func (tx *Tx) QueryRow(query string, args ...interface{}) database.Row {
	row := tx.Tx.QueryRow(query, args...)
	return &Row{row}
}

func (tx *Tx) Rollback() error {
	return tx.Tx.Rollback()
}

type Result struct {
	Result sql.Result
}

func (r *Result) LastInsertId() (int64, error) {
	return r.Result.LastInsertId()
}

func (r *Result) RowsAffected() (int64, error) {
	return r.Result.RowsAffected()
}

type Rows struct {
	Rows *sql.Rows
}

func (r *Rows) Close() error {
	return r.Rows.Close()
}

func (r *Rows) Columns() ([]string, error) {
	return r.Rows.Columns()
}

func (r *Rows) Err() error {
	return r.Rows.Err()
}

func (r *Rows) Next() bool {
	return r.Rows.Next()
}

func (r *Rows) NextResultSet() bool {
	return r.Rows.NextResultSet()
}

func (r *Rows) Scan(dest ...interface{}) error {
	return r.Rows.Scan(dest...)
}

type Row struct {
	Row *sql.Row
}

func (r *Row) Scan(dest ...interface{}) error {
	return r.Row.Scan(dest...)
}
