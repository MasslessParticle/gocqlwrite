package main

import (
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
)

var (
	connectionPool   = 1
	minutesPerBucket = 60
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
	//Query params setup
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

	q := r.URL.Query().Get("q")
	var re *regexp.Regexp
	if q != "" {
		re, err = regexp.Compile(q)
		if err != nil {
			w.Write([]byte("Bad Regex"))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
	}

	//Do Query Query params setup

	times := getTimes(start, end)
	counts := make(chan int, len(times))

	var wg sync.WaitGroup
	wg.Add(1)
	var total int
	go func() {
		defer wg.Done()
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

	s := <-sessions
	batchQuery(sourceID, re, times, s, counts)
	sessions <- s

	wg.Wait()

	w.Write([]byte(fmt.Sprintf("Num Logs: %d\n", total)))
}

func batchQuery(sourceID string, re *regexp.Regexp, times []time.Time, s *gocql.Session, counts chan int) {
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

	var wg sync.WaitGroup
	for _, t := range tp {
		wg.Add(1)
		go func(s *gocql.Session, t []time.Time) {
			counts <- batchCount(s, sourceID, t, re)
			wg.Done()
		}(s, t)
	}
	wg.Wait()
	close(counts)
}

func batchCount(s *gocql.Session, sourceID string, t []time.Time, re *regexp.Regexp) int {
	var q *gocql.Query

	q = s.Query(
		`SELECT log FROM logs WHERE source_id = ? and ts_min IN ?`,
		sourceID,
		timesToString(t),
	).Consistency(gocql.One)

	iter := q.Iter()
	var numLogs int
	var log []byte
	for iter.Scan(&log) {
		if re == nil || re.Match(log) {
			numLogs++
		}
	}

	if err := iter.Close(); err != nil {
		fmt.Println(err)
	}

	return numLogs
}

func getTimes(start, end time.Time) []time.Time {
	start = start.Truncate(time.Minute)
	end = end.Truncate(time.Minute)

	var times []time.Time
	for t := start; t.Equal(end) || end.After(t); t = t.Add(time.Minute) {
		times = append(times, t)
	}

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

	rp := gocql.SimpleRetryPolicy{
		NumRetries: 3,
	}
	hosts := strings.Split(os.Getenv("HOSTS"), ",")
	cluster := gocql.NewCluster(hosts...)

	cluster.Authenticator = gocql.PasswordAuthenticator{Username: os.Getenv("USER"), Password: os.Getenv("PASSWORD")}
	cluster.Keyspace = "logstore"
	cluster.Consistency = gocql.Quorum
	cluster.ConnectTimeout = 30 * time.Second
	cluster.Compressor = gocql.SnappyCompressor{}
	cluster.Timeout = 30 * time.Second
	cluster.RetryPolicy = &rp

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
