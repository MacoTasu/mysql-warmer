package mysqlwarmer

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/Songmu/prompter"
	"golang.org/x/sync/errgroup"
	// mysql driver
	_ "github.com/go-sql-driver/mysql"
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

func (opts *Options) getDSN() string {
	return fmt.Sprintf("%s:%s@(%s:%d)/%s", opts.User, opts.Password, opts.Host, opts.Port, opts.DataBase)
}

func (opts *Options) getTables(engine string) ([]string, error) {
	db, err := sql.Open("mysql", opts.getDSN())
	if err != nil {
		return nil, err
	}
	defer db.Close()

	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE engine=? AND table_schema=?", engine, opts.DataBase)
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

	eg := errgroup.Group{}
	for _, table := range tables {
		eg.Go(func() error {
			log.Printf("start: %s \n", table)
			_, err := db.Exec(fmt.Sprintf("LOAD INDEX INTO CACHE %s", table))
			if err != nil {
				return err
			}

			_, err = db.Exec(fmt.Sprintf("SELECT * FROM %s", table))
			if err != nil {
				return err
			}
			log.Printf("end: %s \n", table)
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		log.Fatal(err)
	}

	return nil
}

func (opts *Options) setPass(args []string) error {
	for _, element := range args {
		if element == "--password=" || element == "-p=" {
			pass := prompter.Prompt("Password: ", "")
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
