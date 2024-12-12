rabbitmq-cluster:
	cd cluster; rm -rf tls-gen;
	cd cluster; git clone https://github.com/michaelklishin/tls-gen tls-gen; cd tls-gen/basic; make
	mv cluster/tls-gen/basic/result/server_*_certificate.pem cluster/tls-gen/basic/result/server_certificate.pem
	mv cluster/tls-gen/basic/result/server_*key.pem cluster/tls-gen/basic/result/server_key.pem
	cd cluster; docker build -t haproxy-rabbitmq-cluster  .
	cd cluster; chmod -R 755 tls-gen 
	cd cluster; docker compose down
	cd cluster; docker compose up -d

rabbitmq-test:
	rm -rf tls-gen;
	git clone https://github.com/rabbitmq/tls-gen tls-gen; cd tls-gen/basic; CN=rabbitmq make
	chmod -R 755 tls-gen
	docker compose down
	docker compose up -d
	sleep 5
	docker exec rabbitmq-stream rabbitmqctl await_startup
	docker exec rabbitmq-stream rabbitmqctl add_user 'O=client,CN=rabbitmq' ''
	docker exec rabbitmq-stream rabbitmqctl clear_password 'O=client,CN=rabbitmq'
	docker exec rabbitmq-stream rabbitmqctl set_permissions 'O=client,CN=rabbitmq' '.*' '.*' '.*'
