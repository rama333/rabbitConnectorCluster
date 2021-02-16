package rmq

import (
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"time"
)

var (
	errorCloseConnection chan *amqp.Error
	rabbitError          error
	connect              *amqp.Connection
)

type connection struct {
	connection           *amqp.Connection
	errorCloseConnection chan *amqp.Error
	err                  chan error
	channel              *amqp.Channel
	urls                 []string
	nameQueue            string
	consumer             []Consume
	close                bool
}

type Consume func(mes []byte)

func NewConnect(name string, urls ...string) *connection {
	c := new(connection)

	c.urls = urls
	c.nameQueue = name
	c.err = make(chan error)

	c.connect()

	go c.rabbitMQReConnector()

	return c
}

func (c *connection) Close() {
	logrus.Info("close connection")
	c.close = true
	c.connection.Close()
	c.channel.Close()
}

func (c *connection) connect() {

LOOP:
	for {
		for _, url := range c.urls {
			logrus.Info("connect to", url)

			conn, err := amqp.Dial(url)

			if err == nil {
				logrus.Info("successful connect", url)
				c.connection = conn
				c.errorCloseConnection = make(chan *amqp.Error)
				c.connection.NotifyClose(c.errorCloseConnection)
				c.openChannel()

				break LOOP
			}
		}

		logrus.Info("retry connection 1 sec")

		time.Sleep(time.Second * 1)

	}
}

func (c *connection) openChannel() {
	channel, err := c.connection.Channel()

	if err != nil {
		logrus.Info("could not open channel")
	}

	logrus.Info("successful open channel")

	c.channel = channel
}

func (c *connection) rabbitMQReConnector() {

	for {
		<-c.errorCloseConnection
		logrus.Info("lost connection")
		c.connect()
		c.recoveryConsumer()
	}
}

func (c *connection) registerConsumer() <-chan amqp.Delivery {

	deliveries, err := c.channel.Consume(
		c.nameQueue, // queue
		"",          // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)

	if err != nil {
		logrus.Info(err)
	}

	return deliveries

}

func (c *connection) Consume(co Consume) {

	deliveries := c.registerConsumer()

	c.handlerMessagesConsumer(deliveries, co, false)

}

func (c *connection) handlerMessagesConsumer(deliveries <-chan amqp.Delivery, co Consume, recovery bool) {

	if !recovery {
		c.consumer = append(c.consumer, co)
	}
	go func() {
		for deliverer := range deliveries {
			co(deliverer.Body)
		}
	}()
}

func (c *connection) recoveryConsumer() {

	deliveries := c.registerConsumer()

	for _, con := range c.consumer {
		c.handlerMessagesConsumer(deliveries, con, true)
	}
}
