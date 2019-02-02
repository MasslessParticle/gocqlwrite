package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

var (
	connectionPool   = 35
	minutesPerBucket = 15
	sessions         chan *gocql.Session
)

func main() {
	startSessions()
	fmt.Println(len(sessions))
	defer stopSessions()

	http.HandleFunc("/query", handleQuery)
	http.ListenAndServe(":"+os.Getenv("PORT"), nil)
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	sourceID := r.URL.Query().Get("source_id")
	start, err := time.Parse("200601021504", r.URL.Query().Get("start"))
	if err != nil {
		w.Write([]byte("Cannot parse start value"))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	end, err := time.Parse("200601021504", r.URL.Query().Get("end"))
	if err != nil {
		w.Write([]byte("Cannot parse end value"))
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	times := getTimes(start, end)
	var tp [][]time.Time
	var lastIndex int
	for i := range times {
		if i%minutesPerBucket == 0 && i != lastIndex {
			tp = append(tp, times[lastIndex:i])
			lastIndex = i
		}
	}
	if lastIndex != len(times) {
		tp = append(tp, times[lastIndex:])
	}

	counts := make(chan int, len(tp))

	var wg2 sync.WaitGroup
	wg2.Add(1)
	var total int
	go func() {
		defer wg2.Done()
		for {
			select {
			case c, ok := <-counts:
				if ok {
					total += c
				} else {
					return
				}
			}
		}
	}()

	var wg sync.WaitGroup
	for _, ts := range tp {
		wg.Add(1)
		s := <-sessions
		go func(s *gocql.Session, t []time.Time) {
			counts <- countLogs(s, sourceID, t)
			sessions <- s
			wg.Done()
		}(s, ts)
	}

	wg.Wait()
	close(counts)
	wg2.Wait()

	w.Write([]byte(fmt.Sprintf("Num Logs: %d\n", total)))
}

func countLogs(s *gocql.Session, sourceID string, ts []time.Time) int {
	q := s.Query(
		`SELECT log FROM logs WHERE source_id = ? and ts_min IN ?`,
		sourceID,
		timesToString(ts),
	).Consistency(gocql.One).Iter()

	fmt.Println(q)

	var numLogs int
	var log string
	for q.Scan(&log) {
		numLogs++
	}

	if err := q.Close(); err != nil {
		fmt.Println(err)
	}

	return numLogs
}

func getTimes(start, end time.Time) []time.Time {
	start = start.Truncate(time.Minute)
	end = end.Truncate(time.Minute)

	var times []time.Time
	for t := start; t == end || end.After(t); t = t.Add(time.Minute) {
		times = append(times, t)
	}

	fmt.Println(times)

	return times
}

func timesToString(times []time.Time) []string {
	var ts []string
	for _, t := range times {
		ts = append(ts, t.Format("200601021504"))
	}

	return ts
}

func stopSessions() {
	fmt.Println("Stopping Sessions")
	close(sessions)
	for s := range sessions {
		s.Close()
	}
}

func startSessions() {
	sessions = make(chan *gocql.Session, connectionPool)

	hosts := strings.Split(os.Getenv("HOSTS"), ",")
	cluster := gocql.NewCluster(hosts...)

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

	for i := 0; i < connectionPool; i++ {
		s, err := cluster.CreateSession()
		if err != nil {
			fmt.Println(err)
		}
		sessions <- s
	}

	session.Close()
}
