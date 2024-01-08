# RabbitMQ cluster with HA proxy

how to run:

add the following to your `/etc/hosts`

```
127.0.0.1       node0
127.0.0.1       node1
127.0.0.1       node2
```

set the following values in your environment if you want to run the tests

```
RABBITMQ_USER="rabbit"
RABBITMQ_PASSWORD="rabbit"
RABBIT_MQ_MANAGEMENT_PORT=15673
RABBIT_MQ_AMQP_PORT=5555
RABBIT_MQ_TEST_NODES="node0:5562;node1:5572;node2:5582"
RABBIT_MQ_TEST_ADDRESS_BALANCER="localhost:5553"
```

then run the following

```bash
git clone git@github.com:rabbitmq/rabbitmq-stream-js-client.git .
make rabbitmq-cluster
```

ports:

```
 - localhost:5553 #standard stream port
 - localhost:5554 #TLS stream port
 - http://localhost:15673 #management port
```
