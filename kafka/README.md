To build the final docker image use the makefile

Run `make build` to compile and build the docker image.

Create the kafka topic
```
./kafka-topics.sh --create --topic tmgo --replication-factor 1  --partitions 16 --zookeeper localhost:2181
```
