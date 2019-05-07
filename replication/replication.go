package replication

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"github.com/pkg/errors"
)

type mockServer struct {
	conn        Conn
	masterHost  string
	masterPort  int
	offset      int64
	masterRunId string
	mockServerPort   int
	mu          sync.RWMutex
	RdbBytes    int64
	replicator  Replicator
	masterauth  string
	ackCtlChan chan int
}

type Replicator interface {
	ProcessRdb(br io.Reader, totalBytes int64) error

	ProcessMasterRepl(repl string) error
}

type RedisReplicationConf struct {
	MasterHost     string
	MasterPort     int
	Offset         int64
	MasterRunId    string
	MockServerPort int
	MasterAuth     string
}

func NewReplication(conf RedisReplicationConf, repl Replicator) *mockServer {
	var masterRunId string
	var offset int64
	if conf.MasterRunId == "" {
		masterRunId = "?"
	}
	if conf.Offset == 0 {
		offset = int64(-1)
	}
	return &mockServer{
		masterHost:  conf.MasterHost,
		masterPort:  conf.MasterPort,
		masterRunId: masterRunId,
		mockServerPort:   conf.MockServerPort,
		offset:      offset,
		replicator:  repl,
		masterauth:  conf.MasterAuth,
		ackCtlChan: make(chan int, 1),
	}
}

func (s *mockServer) StartReplication() error {
	connAddr := fmt.Sprintf("%s:%d", s.masterHost, s.masterPort)
	c, err := net.Dial("tcp", connAddr)
	if err != nil {
		return err
	}
	s.conn = NewConn(c, 0, 0)
	if s.masterauth != "" {
		s.conn.Send("AUTH", s.masterauth)
		s.conn.Flush()
		retVal, err := s.conn.readReply()
		retValString := fmt.Sprintf("%s", retVal)
		if err !=nil {
			errors.New(fmt.Sprintf("error: %s, %s", err, retValString))
		}
		if !strings.HasPrefix(retValString, "+"){
			errors.New(fmt.Sprintf("error: %s, %s", err, retValString))
		}

	}
	s.sendMockServerPort()

	s.conn.Send("PSYNC", s.masterRunId, s.offset)
	s.conn.Flush()
	line, err := s.conn.readLine()
	if line[0] == '+' {
		lineSplit := strings.Split(string(line[1:]), " ")
		if lineSplit[0] == "FULLRESYNC" {
			s.masterRunId = lineSplit[1]
			s.offset, _ = strconv.ParseInt(string(lineSplit[2]), 10, 64)

		}
	}else {
		return errors.New(string(line[1:]))
	}
	for{
		line, err = s.conn.readLine()
		if strings.HasPrefix(string(line), "$"){
			s.RdbBytes, err = strconv.ParseInt(string(line[1:]), 10, 64)
			break
		}
	}

	s.replicator.ProcessRdb(s.conn.GetBr(), s.RdbBytes)

	go s.conn.ReadPSyncResult()
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				s.sendAck()
			case <-s.ackCtlChan:
				ticker.Stop()
				return

			}

		}
	}()
	channel := s.conn.GetResultChannel()
	for rpd := range channel {
		cmd := rpd.Data
		s.setOffset(int64(rpd.Bytes))
		s.replicator.ProcessMasterRepl(cmd)
	}
	fmt.Println("over")
	return nil
}

func (s *mockServer) sendAck() error {
	err := s.conn.Send("REPLCONF", "ACK", s.offset)
	s.conn.Flush()
	return err
}

func (s *mockServer) setOffset(offset int64) {
	s.offset += offset

}

func (s *mockServer) sendMockServerPort() error {
	err := s.conn.Send("REPLCONF", "listening-port", s.mockServerPort)
	s.conn.Flush()
	ret, err := s.conn.readReply()
	fmt.Println("send mockServer port")
	retValString := fmt.Sprintf("%s", ret)

	if err !=nil {
		errors.New(fmt.Sprintf("error: %s, %s", err, retValString))
	}
	if !strings.HasPrefix(retValString, "+"){
		errors.New(fmt.Sprintf("error: %s, %s", err, retValString))
	}
	return err
}

func (s *mockServer) StopReplication()  {
	s.ackCtlChan <- 1

	s.conn.sendCloseSignal()
	fmt.Println("stop")
}


