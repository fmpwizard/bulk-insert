package main

import (
	"errors"
	"flag"
	"log"
	"strings"
	"sync"
)

var testType string
var connStr DatabaseURI

/*
bulk-insert \
-type=cockroachdb-setup \
--db-uri=postgresql://root@192.168.2.102:26257?sslmode=disable \
--db-uri=postgresql://root@192.168.2.103:26257?sslmode=disable \
--db-uri=postgresql://root@192.168.2.104:26257?sslmode=disable
*/
// bulk-insert  -type=cockroachdb  --db-uri=postgresql://root@192.168.2.102:26257?sslmode=disable
// bulk-insert  -type=mongodb  --db-uri=mongodb://192.168.1.11:2700/testing

func main() {
	flags()
	flag.Parse()
	if len(connStr.Paths) == 0 {
		flag.Usage()
	}
	var wg sync.WaitGroup
	if testType == "mongodb" {
		wg.Add(1)
		go mongodbRun(connStr.Paths[0], &wg)
		wg.Wait()
	}
	if testType == "cockroachdb-setup" {
		cockroachdbSetup(connStr.Paths[0])
	}
	if testType == "cockroachdb" {
		for {
			wg.Add(3)
			go cockroachdbRun(connStr.Paths[0], &wg)
			go cockroachdbRun(connStr.Paths[1], &wg)
			go cockroachdbRun(connStr.Paths[2], &wg)
			log.Println("waiting ...")
			wg.Wait()
			log.Println("about to truncate ...")
			cockroachdbDelete(connStr.Paths[0])
			log.Println("truncated ...")
		}
	}
}

func flags() {
	flag.StringVar(&testType, "type", "mongodb", "enter test type: mongodb | cockroachdb")
	flag.Var(&connStr, "db-uri", "enter connection uri for database, can be passed several times for multiple uris")
}

//"postgresql://192.168.2.102:26257/bank"

// DatabaseURI holds a slice of uri to connect to the database
type DatabaseURI struct {
	Paths []string
}

func (r *DatabaseURI) String() string {
	s := ""
	for _, v := range r.Paths {
		s = s + v
	}
	return s
}

// Set is the method to set the flag value, part of the flag.Value interface.
// Set's argument is a string to be parsed to set the flag.
func (r *DatabaseURI) Set(value string) error {
	cleanString := strings.TrimSpace(value)

	r.Paths = append(r.Paths, cleanString)

	if len(r.Paths) == 0 {
		return errors.New("missing db-uri parameter, you can pass it many times for multiple uris (cockroachdb supports this)")
	}
	return nil
}
