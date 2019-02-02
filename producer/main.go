package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/gocql/gocql"
	uuid "github.com/nu7hatch/gouuid"
)

var (
	success       uint64
	failed        uint64
	started       time.Time
	lastOperation string
)

func main() {
	//Introduce some jitter among the emitters
	time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)

	lo := make(chan string, 1000)
	go setLastOperation(lo)

	lo <- "Create Table"
	c := setupConnction()

	lo <- "Create Session"
	session, err := c.CreateSession()
	if err != nil {
		panic(err)
	}

	lo <- "Write"
	guid := guid()
	fmt.Println(guid)

	go serveStats(guid)

	started = time.Now()
	t := time.NewTicker(10 * time.Millisecond)
	for range t.C {
		go func() {
			batch := session.NewBatch(gocql.LoggedBatch)
			lo <- "New Batch"

			logs := rand.Intn(5) + 2
			fmt.Printf("%s: Sending %d logs\n", guid, logs)
			for i := 0; i < logs; i++ {
				now := time.Now()
				nowMin := now.Truncate(time.Hour)

				lo <- "Send Batch"
				batch.Query(`INSERT into logs (source_id, ts_hour, ts, log) VALUES(?, ?, ?, ?)`, guid, nowMin.Format("2006010215"), now.UnixNano(), gofakeit.HipsterSentence(21))
			}
			lo <- "Logs send"

			lo <- "Executing Batch"
			err = session.ExecuteBatch(batch)
			lo <- "Execued Batch"
			if err != nil {
				atomic.AddUint64(&failed, uint64(logs))
				log.Println(err)
				return
			}

			atomic.AddUint64(&success, uint64(logs))
		}()
	}
}

func setLastOperation(o chan string) {
	for {
		select {
		case operation := <-o:
			lastOperation = operation
		}
	}
}

func serveStats(appId string) {
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s := atomic.LoadUint64(&success)
		f := atomic.LoadUint64(&failed)
		dur := time.Since(started)
		seconds := float64(dur) / float64(time.Second)

		stats := fmt.Sprintf(
			`{"id": "%s", "last_op": "%s", "success": %d, "failed": %d, "runtime": "%s", "avg_rate": %.2f}`,
			appId,
			lastOperation,
			s,
			f,
			dur,
			float64(s+f)/seconds,
		)

		w.Write([]byte(stats))
	})

	http.ListenAndServe(":"+os.Getenv("PORT"), nil)
}

func setupConnction() *gocql.ClusterConfig {
	hosts := strings.Split(os.Getenv("HOSTS"), ",")
	cluster := gocql.NewCluster(hosts...)

	cluster.Authenticator = gocql.PasswordAuthenticator{Username: os.Getenv("USER"), Password: os.Getenv("PASSWORD")}
	cluster.Keyspace = "gocqlwrite"
	cluster.Consistency = gocql.Quorum
	cluster.ConnectTimeout = time.Second * 10
	cluster.Timeout = 10 * time.Second

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
		PRIMARY KEY ((source_id, ts_hour), ts));`).Exec(); err != nil {
		log.Fatal(err)
	}
	session.Close()

	return cluster
}

func guid() string {
	uuid, _ := uuid.NewV4()
	return uuid.String()
}
