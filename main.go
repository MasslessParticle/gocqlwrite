package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	uuid "github.com/nu7hatch/gouuid"
)

func main() {
	http.HandleFunc("/run_test", func(w http.ResponseWriter, r *http.Request) {
		go runTest()
		w.Write([]byte("Test Started!"))
	})
	http.ListenAndServe(":"+os.Getenv("PORT"), nil)
}

func runTest() {
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
					logMessage := sampleLogMessage()

					batch := session.NewBatch(gocql.LoggedBatch)
					for i := 0; i < 20000/ps; i++ {
						batch.Query(`INSERT into logs (source_id, ts_nanos, log) VALUES(?, ?, ?)`, guid, time.Now().UnixNano(), logMessage)
					}

					err = session.ExecuteBatch(batch)
					if err != nil {
						atomic.AddUint64(&failed, 20000/uint64(ps))
						log.Println(err)
					}

					atomic.AddUint64(&success, 20000/uint64(ps))
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

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func sampleLogMessage() string {
	b := make([]byte, 200)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
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
	cluster := gocql.NewCluster()

	cluster.Authenticator = gocql.PasswordAuthenticator{Username: os.Getenv("USER"), Password: os.Getenv("PASSWORD")}
	cluster.Keyspace = "gocqlwrite"
	cluster.Consistency = gocql.Quorum
	cluster.ConnectTimeout = time.Second * 10

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	if err := session.Query(`CREATE TABLE IF NOT EXISTS logs (source_id varchar, ts_nanos bigint, log varchar, PRIMARY KEY (source_id, ts_nanos));`).Exec(); err != nil {
		log.Fatal(err)
	}
	session.Close()

	return cluster
}

func guid() string {
	uuid, _ := uuid.NewV4()
	return uuid.String()
}
