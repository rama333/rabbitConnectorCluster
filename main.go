package main

import (
	"fmt"
	_ "github.com/rama333/rabbitConnectorCluster/rmq"
	"github.com/sirupsen/logrus"
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

	connect := rmq.NewConnect("detecteded_faces", "amqp://admin:test@192.168.1.1/", "amqp://guest:guest@rabbit_url_node_2/", "amqp://guest:guest@rabbit_url_node_2/")

	defer connect.Close()

	logrus.SetLevel(logrus.DebugLevel)

	connect.Consume(func(messages []byte) {

		logrus.Info(string(messages))

		time.Sleep(time.Second * 1)
	})

	for i := 0; i < 100; i++ {
		connect.Publish(fmt.Sprint(i, "qwe"))
	}

	signals := make(chan os.Signal)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	logrus.Infof("captured %v signal, stopping", <-signals)

}
