RabbitMQ cluster with HA proxy 
===

how to run:

add the following to your `/etc/hosts`
```
127.0.0.1       node0
127.0.0.1       node1
127.0.0.1       node2
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
