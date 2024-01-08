rabbitmq-cluster:
	cd cluster; rm -rf tls-gen;
	cd cluster; git clone https://github.com/michaelklishin/tls-gen tls-gen; cd tls-gen/basic; make
	mv cluster/tls-gen/basic/result/server_*_certificate.pem cluster/tls-gen/basic/result/server_certificate.pem
	mv cluster/tls-gen/basic/result/server_*key.pem cluster/tls-gen/basic/result/server_key.pem
	cd cluster; docker build -t haproxy-rabbitmq-cluster  .
	cd cluster; chmod 755 -R tls-gen 
	cd cluster; docker compose down
	cd cluster; docker compose up -d