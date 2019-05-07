package main

import (
	"fmt"
	"io"
	"os"
	"github.com/ngaut/log"
	"github.com/lorddang/go-redisReplication/replication"
	"os/signal"
	"time"
)

type replicator struct {
}

func (r *replicator) ProcessRdb(br io.Reader, n int64) error {
	file, _ := os.OpenFile("dump.rdb", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0755)
	defer file.Close()
	io.CopyN(file, br, n)
	return nil
}

func (r *replicator) ProcessMasterRepl(cmd string) error {
	fmt.Println(cmd)
	return nil
}
func main() {

	slaveCfg := replication.RedisReplicationConf{
		MasterHost:     "127.0.0.1",
		MasterPort:     7777,
		MockServerPort: 8081,
		MasterAuth:     "",
	}
	repl := &replicator{}
	replicator := replication.NewReplication(slaveCfg, repl)
	go func() {
		err := replicator.StartReplication()
		if err != nil {
			log.Error(err)
			os.Exit(-1)
		}
	}()
	time.Sleep(time.Second*3)
	//replicator.StopReplication()
	handleSignal()
}

func handleSignal()  {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs)
	func() {
		sig := <-sigs
		log.Info("receive signal: ", sig)

		os.Exit(0)
	}()
}