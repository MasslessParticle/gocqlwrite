package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/gocql/gocql"
	uuid "github.com/nu7hatch/gouuid"
)

var (
	success uint64
	failed  uint64
)

func main() {
	fmt.Println("Create Table")
	c := setupConnction()

	fmt.Println("Create Sessions")
	session, err := c.CreateSession()
	if err != nil {
		panic(err)
	}

	fmt.Println("Write")
	guid := guid()
	fmt.Println(guid)

	go serveStats(guid)

	t := time.NewTicker(1 * time.Millisecond)
	for range t.C {
		batch := session.NewBatch(gocql.LoggedBatch)
		for i := 0; i < 5; i++ {
			now := time.Now()
			nowMin := now.Truncate(time.Hour)

			batch.Query(`INSERT into logs (source_id, ts_hour, ts, log) VALUES(?, ?, ?, ?)`, guid, nowMin, now.UnixNano(), gofakeit.HackerPhrase())
		}

		err = session.ExecuteBatch(batch)
		if err != nil {
			atomic.AddUint64(&failed, 5)
			log.Println(err)
			continue
		}

		atomic.AddUint64(&success, 5)
	}
}

func serveStats(appId string) {
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		stats := fmt.Sprintf(
			`{"id": "%s", "success": "%d", "failed": "%d"}`,
			appId,
			atomic.LoadUint64(&success),
			atomic.LoadUint64(&failed),
		)

		w.Write([]byte(stats))
	})

	http.ListenAndServe(":"+os.Getenv("PORT"), nil)
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

	if err := session.Query(`
		CREATE TABLE IF NOT EXISTS logs (
		   source_id varchar,
		   ts_hour varchar,
		   ts bigint,
		   log varchar,
		PRIMARY KEY ((source_id, ts_min), ts));`).Exec(); err != nil {
		log.Fatal(err)
	}
	session.Close()

	return cluster
}

func guid() string {
	uuid, _ := uuid.NewV4()
	return uuid.String()
}
