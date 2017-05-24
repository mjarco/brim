package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"text/template"

	"github.com/allegro/akubra/log"
	_ "github.com/lib/pq"
)

type MigrationTask struct {
	From, To, Key, Env string

}

type DBConfig struct {
	User       string `yaml:"user" validate:"nonzero"`
	Password   string `yaml:"password" validate:"min=0,max=32"`
	DBName     string `yaml:"dbname" validate:"min=4,max=12"`
	Host       string `yaml:"host" validate:"min=4,max=48"`
	InsertTmpl string `yaml:"inserttmpl" validate:"min=10"`
	SelectTmpl string `yaml:"selecttmpl" validate:"min=10"`
}

type dbStorage struct {
	config DBConfig
	db     *sql.DB
	tmpl   *template.Template
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

func (s *dbStorage) renderQuery(classKey classifiedKey) (string, error) {
	var err error

	task := MigrationTask{
		Key:  classKey.path,
		Env:  "none",
		To:   classKey.targetRegion,
		From: classKey.sourceRegion,
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
	log.Debug("Query:\n%s\n", q)
	rows, err := db.Query(q)

	if err != nil {
		return err
	}
	rows.Close()
	return nil
}
