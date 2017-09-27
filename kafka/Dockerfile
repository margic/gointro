FROM debian:jessie

RUN apt-get update && apt-get -y install librdkafka-dev

COPY kafka /kafka
COPY config.yml .
ENTRYPOINT ["./kafka"]
