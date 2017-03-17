package main

import (
	"log"
	"os"

	mysqlwarmer "github.com/MacoTasu/mysql-warmer"
)

func main() {
	if err := mysqlwarmer.Run(os.Args[1:]); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}
