package main

import (
	"fmt"
	"log"
	"os"
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
<<<<<<< HEAD

	var sessions []*gocql.Session
	sessionsChan := make(chan *gocql.Session, 500)

	wg := sync.WaitGroup{}
	wg.Add(500)
	for i := 0; i < 500; i++ {
		go func() {
			session := connect()
			sessionsChan <- session
			wg.Done()
		}()
	}
	wg.Wait()

	for i := 0; i < 500; i++ {
		session := <-sessionsChan
		sessions = append(sessions, session)
		defer session.Close()
	}

	for i := 0; i < 20000; i++ {
		go func() {
			guid := guid()
			t := time.NewTicker(delay)
			for range t.C {
				err := insert(guid, sessions[i%len(sessions)])
				if err != nil {
					atomic.AddUint64(&failed, 1)
					log.Println(err)
					continue
				}
=======

	fmt.Println("Create Table")
	c := setupConnction()

	fmt.Println("Create Sessions")
	ps := 125
	sessions := make(chan *gocql.Session, 500)
	createSessions(c, sessions, ps)

	fmt.Println("Write")
	doneChan := make(chan struct{}, 1)
	for i := 0; i < ps; i++ {
		guid := guid()
		fmt.Println(guid)

		go func() {
			session := <-sessions
			t := time.NewTicker(delay)
			for {
				select {
				case <-t.C:
					for i := 0; i < 20000/ps; i++ {
						err := insert(guid, session)
						if err != nil {
							atomic.AddUint64(&failed, 1)
							log.Println(err)
							continue
						}
>>>>>>> test works

						atomic.AddUint64(&success, 1)
					}
				case <-doneChan:
					return
				}
			}
		}()
	}

	testDuration := time.NewTimer(time.Minute)
	<-testDuration.C
	close(doneChan)
}

func createSessions(c *gocql.ClusterConfig, sessions chan *gocql.Session, poolSize int) {
	for i := 0; i < poolSize; i++ {

		session, err := c.CreateSession()
		if err != nil {
			panic(err)
		}
		fmt.Print(i)
		sessions <- session

	}
	fmt.Println()
}

func setupConnction() *gocql.ClusterConfig {
	cluster := gocql.NewCluster() //TODO: What about the hostname of a load balancer?

	cluster.Authenticator = gocql.PasswordAuthenticator{Username: os.Getenv("USER"), Password: os.Getenv("PASSWORD")}
	cluster.Keyspace = "gocqlwrite"
	cluster.Consistency = gocql.Quorum
	cluster.ConnectTimeout = time.Second * 10

	//TODO: Know more about how this works
	cluster.DisableInitialHostLookup = true

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	if err := session.Query(`
		CREATE TABLE IF NOT EXISTS logs (
		   source_id varchar,
		   ts timestamp,
		   log varchar,
		PRIMARY KEY (source_id, ts));`).Exec(); err != nil {
		log.Fatal(err)
	}
	session.Close()

	return cluster
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
