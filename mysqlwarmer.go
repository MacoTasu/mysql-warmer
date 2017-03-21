package mysqlwarmer

import (
	"database/sql"
	"fmt"
	"log"
	"sync"

	// mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/howeyc/gopass"
	flags "github.com/jessevdk/go-flags"
)

// Options is commandline args
type Options struct {
	Host     string `short:"h" long:"host" required:"true" description:"db host"`
	Port     int    `short:"P" long:"port" required:"true" description:"db port"`
	User     string `short:"u" long:"user" required:"true" description:"db login user"`
	Password string `short:"p" long:"password" description:"login user password"`
	DataBase string `short:"d" long:"database" required:"true" description:"database name"`
}

// Err is for multiple err in goroutine
type Err struct {
	Err1 error
	Err2 error
}

func (opts *Options) getDSN() string {
	return fmt.Sprintf("%s:%s@(%s:%d)/%s", opts.User, opts.Password, opts.Host, opts.Port, opts.DataBase)
}

func (opts *Options) getTables(engine string) ([]string, error) {
	db, err := sql.Open("mysql", opts.getDSN())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT table_name FROM information_schema.tables WHERE engine='%s' AND table_schema='%s'", engine, opts.DataBase))
	if err != nil {
		return nil, err
	}

	var tableName string
	tableNames := make([]string, 0)
	for rows.Next() {
		err = rows.Scan(&tableName)
		if err != nil {
			return nil, err
		}
		tableNames = append(tableNames, tableName)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}

	return tableNames, nil
}

func (opts *Options) preload(tables []string) error {
	db, err := sql.Open("mysql", opts.getDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxIdleConns(60)

	var wg sync.WaitGroup
	errChan := make(chan Err, 1)
	for _, table := range tables {
		wg.Add(1)
		go func(targetTable string) {
			var e Err
			defer wg.Done()

			log.Printf("start: %s \n", targetTable)
			_, err1 := db.Exec(fmt.Sprintf("LOAD INDEX INTO CACHE %s", targetTable))
			_, err2 := db.Exec(fmt.Sprintf("SELECT * FROM %s", targetTable))
			log.Printf("end: %s \n", targetTable)

			e.Err1 = err1
			e.Err2 = err2

			errChan <- e
		}(table)
	}
	e := <-errChan
	if e.Err1 != nil {
		return e.Err1
	}
	if e.Err2 != nil {
		return e.Err2
	}
	wg.Wait()

	return nil
}

func (opts *Options) setPass(args []string) error {
	for _, element := range args {
		if element == "--password=" || element == "-p=" {
			fmt.Printf("Password: ")

			pass, err := gopass.GetPasswd()
			if err != nil {
				return err
			}
			opts.Password = fmt.Sprintf("%s", pass)
			return nil
		}
	}
	return nil
}

// Run is executing precache query
func Run(args []string) error {
	opts := &Options{}
	_, err := flags.ParseArgs(opts, args)
	if err != nil {
		return err
	}

	err = opts.setPass(args)
	if err != nil {
		return err
	}

	myisamables, err := opts.getTables("myisam")
	if err != nil {
		return err
	}

	err = opts.preload(myisamables)
	if err != nil {
		return err
	}

	return nil
}
