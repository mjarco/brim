package main

import (
	"bytes"
	"fmt"
	"database/sql"
	"text/template"

	_ "github.com/lib/pq"
)

type MigrationTask struct {
	From, To, Key, Env string
}

type DBConfig struct {
	User       string `yaml:"user"`
	Password   string `yaml:"password"`
	DBName     string `yaml:"dbname"`
	Host       string `yaml:"host"`
	InsertTmpl string `yaml:"inserttmpl"`
}

type dbStorage struct {
	config DBConfig
	db *sql.DB
	tmpl *template.Template
}

func (s *dbStorage) conn() (db *sql.DB, err error) {
	if s.db != nil {
		return s.db, nil
	}
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s",
		s.config.User,
		s.config.Password,
		s.config.Host,
		s.config.DBName)
	s.db, err = sql.Open("postgres", connStr)
	return s.db, err
}

func (s *dbStorage) renderQuery (classKey classifiedKey) (string, error) {
	var err error

	task := MigrationTask{
		Key: classKey.path,
		Env: "none",
		To: classKey.targetCluster,
		From: classKey.sourceCluster,
	}

	if s.tmpl == nil {
		s.tmpl, err = template.New("insert").Parse(s.config.InsertTmpl)
		if err != nil {
			fmt.Println(err.Error())
			return "", err
		}
	}
	queryBuf := &bytes.Buffer{}
	s.tmpl.Execute(queryBuf, task)
	return queryBuf.String(), nil
}

func (s *dbStorage) store(task classifiedKey) error {
	db, err := s.conn()
	if err != nil {
		return err
	}
	q, err := s.renderQuery(task)
	if err != nil {
		return err
	}
	fmt.Printf("Query:\n%s\n", q)
	rows, err := db.Query(q)

	if err != nil {
		return err
	}
	rows.Close()
	return nil
}
