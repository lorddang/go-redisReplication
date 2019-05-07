package replication

import (
	"fmt"
	"github.com/ngaut/log"
	"github.com/pkg/errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type mockServer struct {
	conn           Conn
	masterHost     string
	masterPort     int
	offset         int64
	masterRunId    string
	mockServerPort int
	mu             sync.RWMutex
	rdbBytes       int64
	replicator     Replicator
	masterAuth     string
	ackCtlChan     chan int
}

type Replicator interface {
	ProcessRdb(br io.Reader, rdbBytes int64) error

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
		masterHost:     conf.MasterHost,
		masterPort:     conf.MasterPort,
		masterRunId:    masterRunId,
		mockServerPort: conf.MockServerPort,
		offset:         offset,
		replicator:     repl,
		masterAuth:     conf.MasterAuth,
		ackCtlChan:     make(chan int, 1),
	}
}

func (s *mockServer) StartReplication() error {
	connAddr := fmt.Sprintf("%s:%d", s.masterHost, s.masterPort)
	c, err := net.Dial("tcp", connAddr)
	log.Infof("Conn Redis Server %s", connAddr)
	if err != nil {
		return err
	}
	s.conn = NewConn(c, 0, 0)
	if s.masterAuth != "" {
		log.Infof("masterAuth is %s, send Auth command")
		s.conn.Send("AUTH", s.masterAuth)
		s.conn.Flush()
		retVal, err := s.conn.readReply()
		retValString := fmt.Sprintf("%s", retVal)
		if err != nil {
			errors.New(fmt.Sprintf("error: %s, %s", err, retValString))
		}
		if !strings.HasPrefix(retValString, "+") {
			errors.New(fmt.Sprintf("error: %s, %s", err, retValString))
		}

	} else {
		log.Info("masterAuth is null, no need send auth command")
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
	} else {
		return errors.New(string(line[1:]))
	}
	for {
		line, err = s.conn.readLine()
		if strings.HasPrefix(string(line), "$") {
			s.rdbBytes, err = strconv.ParseInt(string(line[1:]), 10, 64)
			break
		}
	}
	log.Info("Start Process RDB Stream")
	err = s.replicator.ProcessRdb(s.conn.GetBr(), s.rdbBytes)
	if err == nil {
		log.Info("Process RDB Finished")
	} else {
		log.Errorf("Process RDB Error: %v", err)
	}

	go s.conn.ReadPSyncResult()
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				s.sendAck()
			case <-s.ackCtlChan:
				log.Debug("Cancel Send Ack Goroutine")
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
	log.Debugf("Replication Over !!!")
	return nil
}

func (s *mockServer) sendAck() error {
	err := s.conn.Send("REPLCONF", "ACK", s.offset)
	s.conn.Flush()
	log.Debugf("Send ACK to Master: REPLCONF ACK %d", s.offset)
	return err
}

func (s *mockServer) setOffset(offset int64) {
	s.offset += offset

}

func (s *mockServer) sendMockServerPort() error {
	log.Debugf("Send MockServer Port: REPLCONF listening-port %d", s.mockServerPort)
	err := s.conn.Send("REPLCONF", "listening-port", s.mockServerPort)
	s.conn.Flush()
	ret, err := s.conn.readReply()
	retValString := fmt.Sprintf("%s", ret)

	if err != nil {
		errors.New(fmt.Sprintf("error: %s, %s", err, retValString))
	}
	if !strings.HasPrefix(retValString, "+") {
		errors.New(fmt.Sprintf("error: %s, %s", err, retValString))
	}
	return err
}

func (s *mockServer) StopReplication() {
	log.Info("User Request Stop Replication, Stop Replication")
	s.ackCtlChan <- 1

	s.conn.sendCloseSignal()
	log.Info("Replication was Stopped, Bye Bye !!!")
}
