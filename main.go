package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	uuid "github.com/nu7hatch/gouuid"
)

func main() {
	var success uint64
	var failed uint64
	defer func() {
		fmt.Printf("Success: %d\n", atomic.LoadUint64(&success))
		fmt.Printf("Failed: %d\n", atomic.LoadUint64(&failed))
	}()

	delay, err := time.ParseDuration(os.Getenv("delay"))
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1; i++ {
		go func() {
			session := connect()
			defer session.Close()

			guid := guid()
			t := time.NewTicker(delay)
			for range t.C {
				err := insert(guid, session)
				if err != nil {
					atomic.AddUint64(&failed, 1)
					log.Println(err)
					continue
				}

				atomic.AddUint64(&success, 1)
			}
		}()
	}

	wait()
}

func wait() {
	var end_waiter sync.WaitGroup
	end_waiter.Add(1)
	var signal_channel chan os.Signal
	signal_channel = make(chan os.Signal, 1)
	signal.Notify(signal_channel, os.Interrupt)
	go func() {
		<-signal_channel
		end_waiter.Done()
	}()
	end_waiter.Wait()
}

func connect() *gocql.Session {
	cluster := gocql.NewCluster()
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: os.Getenv("USER"), Password: os.Getenv("PASSWORD")}
	cluster.Keyspace = "gocqlwrite"
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	fmt.Println("Create Table")
	if err := session.Query(`
		CREATE TABLE IF NOT EXISTS logs (
		   source_id varchar,
		   ts timeuuid,
		   log varchar,
		PRIMARY KEY (source_id, ts);`).Exec(); err != nil {
		log.Fatal(err)
	}
	return session
}

func insert(guid string, s *gocql.Session) error {
	now := time.Now()
	return s.Query(`
				INSERT into logs (source_id, ts, log) VALUES(?, ?, ?)`,
		guid,
		now,
		"sample log message "+guid,
	).Exec()
}

func guid() string {
	uuid, _ := uuid.NewV4()
	return uuid.String()
}
