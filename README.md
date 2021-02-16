# rabbitMQ CLUSTER RECONNECT

_This project knows how to work with a rabbit cluster, and also knows how to reconnect_

```
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
	
	for i := 0; i < 100; i++ {
		connect.Publish(fmt.Sprint(i, " - test"))
	}
```