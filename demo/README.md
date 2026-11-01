# Komiter demo

A single-broker Kafka, a Kafka UI to look at it with, and two Spark queries that use Komiter.
This is what produced the screenshots in the top-level README.

## Run it

```bash
docker-compose up -d                       # Kafka on :9092, Kafka UI on :8081
mvn -f .. install -DskipTests              # publish com.komiter:komiter_2.13:4.1.3 locally

docker exec komiter-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic orders --partitions 6 --replication-factor 1

docker exec komiter-kafka /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic orders --num-records 300000 --record-size 220 --throughput -1 \
  --producer-props bootstrap.servers=localhost:9092 acks=1

./run.sh
```

Then open <http://localhost:8081> and go to **Consumers**. The `komiter-demo` group is there
with its lag falling, even though Spark never joined a consumer group. Or from the shell:

```bash
docker exec komiter-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group komiter-demo
```

`maxOffsetsPerTrigger` in the demo throttles the query on purpose, so the backlog drains over
a couple of minutes and you can watch the lag fall instead of it vanishing in one batch.

## The multi-source variant

`KomiterMultiDemo` runs one query over two Kafka sources, where the first is subscribed to two
topics at once — the case that needs Komiter to match plan leaves to progress entries by
stream identity. It commits to two separate groups, `komiter-sales` and `komiter-logistics`.

```bash
for t in payments shipments; do
  docker exec komiter-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --topic $t --partitions 3 --replication-factor 1
done
# produce into payments and shipments as above, then:
MAIN_CLASS=KomiterMultiDemo ./run.sh
```

## Tear down

```bash
docker-compose down -v
```

## Notes

- `run.sh` runs the job in a plain JVM rather than through `spark-submit`, so it adds the
  `--add-opens` flags that `spark-submit` would normally supply on JDK 17.
- Kafka UI is on 8081 rather than the usual 8080 to stay out of the way of whatever else you
  have running.
- The broker advertises two listeners: `kafka:29092` for the UI over the compose network, and
  `localhost:9092` for the Spark job on the host.
