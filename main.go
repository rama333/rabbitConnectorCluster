package main

import (
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"log"
	"os"
	"os/signal"
	"rabbitConnectorCluster/rmq"
	"syscall"
	"time"
)

func main() {
	st := time.Now()

	defer func() {
		logrus.Infof("stopped in %s second", time.Now().Sub(st))
	}()

	connect := rmq.NewConnect("all-pgw-cdrs", "amqp://guest:guest@rabbit_url_node_1/", "amqp://guest:guest@rabbit_url_node_2/", "amqp://guest:guest@rabbit_url_node_2/")

	defer connect.Close()

	logrus.SetLevel(logrus.DebugLevel)

	connect.Consume(func(messages []byte) {
		user := &user.USER{}

		err := proto.Unmarshal(messages, user)
		if err != nil {
			log.Println("proto unmarshal", user)
		}

		time.Sleep(time.Second * 1)
	})

	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	logrus.Infof("captured %v signal, stopping", <-signals)

}
