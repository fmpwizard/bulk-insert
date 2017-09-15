package main

import (
	"log"
	"sync"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func mongodbRun(url string, wg *sync.WaitGroup) {
	sess, err := mgo.DialWithTimeout(url, 3*time.Second)
	sess.SetCursorTimeout(0)
	sess.SetSyncTimeout(0)
	sess.SetSocketTimeout(0)
	sess.SetMode(mgo.Primary, false)
	if err != nil {
		log.Fatalln("failed to connect to mongodb ", err)
	}
	for x := 0; x < 3; x++ {
		if err := mongodbLoad(sess); err != nil {
			log.Fatalln("failed to insert docs into mongodb ", err)
		}
	}
	wg.Done()
}

func mongodbLoad(s *mgo.Session) error {
	var payload []interface{}
	ret := GenRecord()
	for x := 0; x <= 999; x++ {
		ret.ID = bson.NewObjectId()
		payload = append(payload, ret)
	}
	return s.DB("testing").C("trades").Insert(payload...)
}
