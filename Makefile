
deps:
	@go mod tidy
.PHONY: setup

build:
	@curl https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem --silent --output ./dist/rds-combined-ca-bundle.pem
	@go build -o dist/docdb-repro cmd/main.go

run-producer: build
	@./dist/docdb-repro run-producer --debug \
		--inserts 1 --updates 5 --delay-between-inserts 10 --delay-between-updates 10 \
		--connection-string "mongodb://<user>:<password>@<host>:27017/?ssl=true&sslInsecure=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
.PHONY: run-producer

run-consumer: build
	@./dist/docdb-repro run-consumer --debug \
		--connection-string "mongodb://<user>:<password>@<host>:27017/?ssl=true&sslInsecure=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
.PHONY: run-consumer

