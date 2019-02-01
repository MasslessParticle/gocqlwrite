package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/brianvoe/gofakeit"
	"github.com/gocql/gocql"
	uuid "github.com/nu7hatch/gouuid"
)

var (
	success uint64
	failed  uint64
	started time.Time
	now     int64
)

func main() {
	guid := guid()
	setupConnction()
	go generate(guid)
	serveStats(guid)
}

func generate(guid string) {
	rate, _ := strconv.Atoi(os.Getenv("LOGS_PER_SECOND"))
	fmt.Println("rate: ", rate)
	dur, _ := strconv.Atoi(os.Getenv("LOGGING_HOURS"))
	fmt.Println("dur: ", dur)
	seconds := dur * 60 * 60
	fmt.Println("seconds: ", seconds)
	millisPerLog := time.Duration(1000/rate) * time.Millisecond
	fmt.Println("millis per log: ", millisPerLog)
	session := startSession()

	fmt.Println("Generating logs...")
	t := time.Now()
	started = t
	for i := 0; i < seconds; i++ {
		batch := session.NewBatch(gocql.LoggedBatch)
		for j := 0; j < rate; j++ {
			batch.Query(`INSERT into logs (source_id, ts_min, ts, log) VALUES(?, ?, ?, ?)`, guid, t.Format("200601021504"), t.UnixNano(), gofakeit.HipsterSentence(21))
			t = t.Add(millisPerLog)
			atomic.StoreInt64(&now, t.UnixNano())
		}

		t = t.Truncate(time.Second).Add(time.Second)

		err := session.ExecuteBatch(batch)
		if err != nil {
			atomic.AddUint64(&failed, uint64(rate))
			log.Println(err)
			continue
		}

		atomic.AddUint64(&success, uint64(rate))
	}
	fmt.Println("Done!")
}

func serveStats(appId string) {
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s := atomic.LoadUint64(&success)
		f := atomic.LoadUint64(&failed)

		n := atomic.LoadInt64(&now)
		currentTime := time.Unix(0, n)
		dur := currentTime.Sub(started)

		seconds := float64(dur) / float64(time.Second)

		stats := fmt.Sprintf(
			`{"id": "%s", "success": %d, "failed": %d, "runtime": "%s", "avg_rate": %.2f}`,
			appId,
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
		   ts_min varchar,
		   ts bigint,
		   log varchar,
		PRIMARY KEY ((source_id, ts_min), ts));`).Exec(); err != nil {
		log.Fatal(err)
	}
	session.Close()

	return cluster
}

func startSession() *gocql.Session {

	cluster.Authenticator = gocql.PasswordAuthenticator{Username: os.Getenv("USER"), Password: os.Getenv("PASSWORD")}
	cluster.Keyspace = "gocqlwrite"
	cluster.Consistency = gocql.Quorum
	cluster.ConnectTimeout = 30 * time.Second
	cluster.Compressor = gocql.SnappyCompressor{}
	cluster.Timeout = 30 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	return session
}

func guid() string {
	uuid, _ := uuid.NewV4()
	return uuid.String()
}
