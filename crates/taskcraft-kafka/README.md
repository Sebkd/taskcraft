# taskcraft-kafka

A Kafka source for [taskcraft](https://github.com/Sebkd/taskcraft): one topic
read as a member of a consumer group.

- Auto commit is always off. An ack commits a partition's offset by the
  commit boundary: an unfinished task holds back the commit of every later
  task of its partition.
- Task ids come from message keys by default; metadata from headers named in
  the metadata registry.
- Partitions are shared between the processes of a group by Kafka.

Built on [`rdkafka`](https://crates.io/crates/rdkafka): building it compiles
librdkafka and needs a C compiler and `make`.

The integration tests need a broker: set `TASKCRAFT_KAFKA_BROKERS`
(for example `localhost:9092`), or they are skipped.
