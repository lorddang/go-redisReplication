package main

import (
	"fmt"
	"github.com/lorddang/go-redisReplication/replication"
	"github.com/ngaut/log"
	"io"
	"os"
	"os/signal"
	"time"
)

type replicator struct {
}

func (r *replicator) ProcessRdb(br io.Reader, n int64) (err error) {
	file, _ := os.OpenFile("dump.rdb", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	defer file.Close()
	_, err = io.CopyN(file, br, n)
	return err
}

func (r *replicator) ProcessMasterRepl(cmd string) error {
	fmt.Println(cmd)
	return nil
}
func main() {

	replConfig := replication.RedisReplicationConf{
		MasterHost:     "127.0.0.1",
		MasterPort:     6328,
		MockServerPort: 8081,
		MasterAuth:     "",
	}
	repl := &replicator{}
	replicator := replication.NewReplication(replConfig, repl)
	defer replicator.StopReplication()
	go func() {
		err := replicator.StartReplication()
		if err != nil {
			log.Error(err)
			os.Exit(-1)
		}
	}()
	time.Sleep(time.Second * 4)

	replicator.StopReplication()
	time.Sleep(time.Second * 4)

	replicator.StartReplication()
	handleSignal()
}

func handleSignal() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs)
	func() {
		sig := <-sigs
		log.Info("receive signal: ", sig)

		os.Exit(0)
	}()
}

func init() {
	log.SetLevel(log.LOG_LEVEL_INFO)
	logFile, _ := os.OpenFile("replication.log", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	log.SetOutput(logFile)
}
