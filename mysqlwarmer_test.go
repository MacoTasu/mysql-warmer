package mysqlwarmer

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"testing"

	"github.com/lestrrat/go-test-mysqld"
	"github.com/stretchr/testify/assert"
)

var (
	testMysqld *mysqltest.TestMysqld
	opts       = Options{
		DataBase: "test",
		Host:     "localhost",
		User:     "root",
		Password: "",
		Port:     13306,
	}
)

func TestMain(m *testing.M) {
	os.Exit(runTests(m))
}

func createTestDataBase() {
	db, err := sql.Open("mysql", fmt.Sprintf("root:@tcp(%s:%d)/", opts.Host, opts.Port))
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS  `test`"); err != nil {
		log.Fatalln(err)
	}

	if _, err := db.Exec("USE `test`"); err != nil {
		log.Fatalln(err)
	}
}

func runTests(m *testing.M) int {
	config := mysqltest.NewConfig()
	config.SkipNetworking = false
	config.Port = opts.Port
	config.BindAddress = opts.Host
	mysqld, err := mysqltest.NewMysqld(config)
	if err != nil {
		log.Fatalln(err)
	}
	defer mysqld.Stop()

	testMysqld = mysqld

	createTestDataBase()

	return m.Run()
}

func TestGetDSN(t *testing.T) {
	assert.Equal(
		t,
		opts.getDSN(),
		fmt.Sprintf("%s:%s@(%s:%d)/%s", opts.User, opts.Password, opts.Host, opts.Port, opts.DataBase),
		"should be equal",
	)
}

func createTestTable() {
	db, err := sql.Open("mysql", testMysqld.DSN())
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS `myisam_table` (`id` INTEGER, INDEX(`id`)) ENGINE=MyISAM"); err != nil {
		log.Fatalln(err)
	}

	if _, err = db.Exec("CREATE TABLE IF NOT EXISTS `innodb_table` (`id` INTEGER) ENGINE=InnoDB"); err != nil {
		log.Fatalln(err)
	}
}

func TestGetTables(t *testing.T) {
	createTestTable()

	var myisamTables []string
	myisamTables = append(myisamTables, "myisam_table")
	tables, err := opts.getTables("myisam")
	if err != nil {
		log.Fatalln(err)
	}

	assert.Equal(
		t,
		tables,
		myisamTables,
		"should be equal",
	)
}

func checkVersion() bool {
	out, err := exec.Command("mysql", "--version").Output()
	if err != nil {
		log.Fatalln(err)
	}

	r := regexp.MustCompile("5.7")
	return r.MatchString(fmt.Sprintf("%s", out))
}

func TestPreload(t *testing.T) {
	myisamtables := []string{"myisam_table"}

	db, err := sql.Open("mysql", testMysqld.DSN())
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	if checkVersion() {
		if _, err = db.Exec("SET GLOBAL show_compatibility_56=ON"); err != nil {
			log.Fatalln(err)
		}
	}

	var keysCount int
	err = db.QueryRow("SELECT `VARIABLE_VALUE` FROM information_schema.GLOBAL_STATUS  WHERE VARIABLE_NAME = 'com_preload_keys'").Scan(&keysCount)
	if err != nil {
		log.Fatalln(err)
	}

	assert.Equal(
		t,
		keysCount,
		0,
		"has not cache",
	)

	if err = opts.preload(myisamtables); err != nil {
		log.Fatalln(err)
	}

	err = db.QueryRow("SELECT `VARIABLE_VALUE` FROM information_schema.GLOBAL_STATUS  WHERE VARIABLE_NAME = 'com_preload_keys'").Scan(&keysCount)
	if err != nil {
		log.Fatalln(err)
	}

	assert.Equal(
		t,
		keysCount,
		1,
		"has cache",
	)
}

func TestRun(t *testing.T) {
	args := []string{
		"--host",
		opts.Host,
		"--user",
		opts.User,
		"--port",
		fmt.Sprintf("%d", opts.Port),
		"--database",
		opts.DataBase,
	}
	assert.Nil(t, Run(args), "success")
}
