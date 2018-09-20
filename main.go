package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gocql/gocql"
	uuid "github.com/nu7hatch/gouuid"
)

func main() {
	//Create the table
	cluster := gocql.NewCluster()
	cluster.Authenticator = gocql.PasswordAuthenticator{Username: os.Getenv("USER"), Password: os.Getenv("PASSWORD")}
	cluster.Keyspace = "gocqlwrite"
	cluster.Consistency = gocql.Quorum

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	fmt.Println("Create Table")
	if err := session.Query(`
		CREATE TABLE IF NOT EXISTS logs (
		   source_id varchar,
		   timestamp_minute varchar,
		   ts timeuuid,
		   log varchar,
		PRIMARY KEY (source_id, timestamp_minute));`).Exec(); err != nil {
		log.Fatal(err)
	}

	sourceID := sourceID()
	fmt.Println("***********************************")
	fmt.Println(sourceID)
	fmt.Println("***********************************")

	now := time.Now()

	fmt.Println("Write Data")
	if err := session.Query(`
				INSERT into logs (source_id, timestamp_minute, ts, log) VALUES(?, ?, ?, ?)`,
		sourceID,
		fmt.Sprintf("%s", now.Truncate(time.Minute).UnixNano()),
		now,
		"sample log message "+sourceID,
	).Exec(); err != nil {
		log.Fatal(err)
	}

}

func sourceID() string {
	uuid, _ := uuid.NewV4()
	return uuid.String()
}
