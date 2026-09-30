# Local Kafka Setup: Step by Step

> Companion to [Kafka-Engineering-Guide.md](Kafka-Engineering-Guide.md). This walks through the "Further learning path" at the end of that guide: run Kafka locally, create a topic, produce and consume, watch rebalancing, break a broker, and replay.

Everything here uses a single-broker **KRaft** cluster (no ZooKeeper) and the CLI tools that ship with Kafka. No application code is needed until the last section.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Option A: Run Kafka with Docker (recommended)](#2-option-a-run-kafka-with-docker-recommended)
3. [Option B: Run Kafka from the binary tarball](#3-option-b-run-kafka-from-the-binary-tarball)
4. [Create and inspect a topic](#4-create-and-inspect-a-topic)
5. [Produce and consume from the console](#5-produce-and-consume-from-the-console)
6. [Consumer groups, lag, and rebalancing](#6-consumer-groups-lag-and-rebalancing)
7. [Reset offsets and replay](#7-reset-offsets-and-replay)
8. [Peek inside `__consumer_offsets`](#8-peek-inside-__consumer_offsets)
9. [Log compaction](#9-log-compaction)
10. [Three brokers: replication, `acks`, and killing a broker](#10-three-brokers-replication-acks-and-killing-a-broker)
11. [Optional: a web UI](#11-optional-a-web-ui)
12. [Write your own producer and consumer (.NET)](#12-write-your-own-producer-and-consumer-net)
13. [Tear down](#13-tear-down)
14. [Troubleshooting](#14-troubleshooting)

---

## 1. Prerequisites

Pick one of the two run options below. You only need the tools for the option you choose.

| Option | Needs | Notes |
|---|---|---|
| **A. Docker** | Docker Engine or Docker Desktop with Compose | On WSL2, install Docker Desktop on Windows and enable **Settings > Resources > WSL integration** for your distro, or install Docker Engine directly inside WSL. Verify with `docker compose version`. |
| **B. Tarball** | Java 17 or newer | Kafka 4.x brokers require Java 17+. Verify with `java -version`. |

Check what you have:

```bash
docker compose version     # option A
java -version              # option B
```

All commands below assume you are in this repo's root directory. The `.gitignore` already excludes `data/`, `kafka-logs/`, and `docker-compose.override.yml`.

---

## 2. Option A: Run Kafka with Docker (recommended)

### 2.1 Create the compose file

Save this as `docker-compose.yml` in the repo root:

```yaml
services:
  kafka:
    image: apache/kafka:latest
    container_name: kafka
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      # Three listeners: one for other containers, one for your host, one for the controller.
      KAFKA_LISTENERS: INTERNAL://:19092,EXTERNAL://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:19092,EXTERNAL://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      # Single broker, so internal topics can only have one replica.
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      # Sensible defaults for experiments.
      KAFKA_NUM_PARTITIONS: 3
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      KAFKA_LOG_DIRS: /var/lib/kafka/data
    volumes:
      - kafka-data:/var/lib/kafka/data

volumes:
  kafka-data:
```

Pin `apache/kafka:latest` to a specific tag (for example `apache/kafka:4.1.0`) once you know which version you want to learn against.

### 2.2 Start it

```bash
docker compose up -d
docker compose logs -f kafka     # Ctrl+C to stop following
```

You are ready when the log shows a line like `Kafka Server started`.

### 2.3 Make the CLI tools easy to call

The Kafka CLI scripts live inside the container at `/opt/kafka/bin/`. Define a helper so you do not have to type `docker exec` every time. In **fish** (this repo's shell):

```fish
function kf
    docker exec -it kafka /opt/kafka/bin/$argv[1] --bootstrap-server localhost:9092 $argv[2..]
end
funcsave kf
```

In **bash/zsh**:

```bash
kf() { docker exec -it kafka /opt/kafka/bin/"$1" --bootstrap-server localhost:9092 "${@:2}"; }
```

From here on, `kf kafka-topics.sh --list` means `docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`.

Smoke test:

```bash
kf kafka-topics.sh --list
kf kafka-broker-api-versions.sh | head -3
```

The first command returns nothing (no topics yet) with no error. The second prints the broker's address and supported API versions.

---

## 3. Option B: Run Kafka from the binary tarball

Skip this section if you used Docker.

### 3.1 Download and unpack

Get the latest binary release from <https://kafka.apache.org/downloads> (the file named `kafka_2.13-<version>.tgz`, not the `-src` one).

```bash
mkdir -p ~/kafka && cd ~/kafka
curl -LO https://downloads.apache.org/kafka/<VERSION>/kafka_2.13-<VERSION>.tgz
tar -xzf kafka_2.13-<VERSION>.tgz
cd kafka_2.13-<VERSION>
```

### 3.2 Format the storage directory (one time only)

KRaft needs a cluster ID and a formatted metadata log before the first start.

```bash
KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format --standalone -t "$KAFKA_CLUSTER_ID" -c config/server.properties
```

`--standalone` sets this node up as the single controller and broker.

### 3.3 Start the broker

```bash
bin/kafka-server-start.sh config/server.properties
```

Leave that terminal open. Open a second terminal for everything else.

### 3.4 Make the CLI tools easy to call

```fish
# fish
function kf
    ~/kafka/kafka_2.13-<VERSION>/bin/$argv[1] --bootstrap-server localhost:9092 $argv[2..]
end
funcsave kf
```

```bash
# bash/zsh
kf() { ~/kafka/kafka_2.13-<VERSION>/bin/"$1" --bootstrap-server localhost:9092 "${@:2}"; }
```

Smoke test:

```bash
kf kafka-topics.sh --list
```

---

## 4. Create and inspect a topic

Create the `orders` topic from the guide with three partitions:

```bash
kf kafka-topics.sh --create --topic orders --partitions 3 --replication-factor 1
kf kafka-topics.sh --describe --topic orders
```

You should see one line per partition, each with `Leader: 1`, `Replicas: 1`, `Isr: 1`. That is Section 4.3 and 4.7 of the guide made concrete: three independent logs, all on the only broker.

Useful topic commands:

```bash
kf kafka-topics.sh --list
kf kafka-topics.sh --describe --topic orders
kf kafka-topics.sh --alter --topic orders --partitions 6      # can only grow, never shrink
kf kafka-topics.sh --delete --topic orders
```

Do not grow the partition count yet. Section 7 of the guide explains why: it changes `hash(key) % n` and moves existing keys.

---

## 5. Produce and consume from the console

Open **two terminals**.

**Terminal 1: consumer.** Start it first so it is waiting. The extra properties print the partition, offset, and key alongside each value:

```bash
kf kafka-console-consumer.sh --topic orders --from-beginning \
  --property print.partition=true \
  --property print.offset=true \
  --property print.key=true \
  --property key.separator=" | "
```

**Terminal 2: producer.** Send keyed records as `key:value`:

```bash
kf kafka-console-producer.sh --topic orders \
  --property parse.key=true \
  --property key.separator=:
```

Type a few lines, pressing Enter after each:

```
customer-42:login
customer-42:add-to-cart
customer-7:login
customer-42:checkout
customer-7:logout
```

Watch terminal 1. Things to notice:

- All `customer-42` records land in the **same partition**, in the order you typed them. All `customer-7` records land together too, possibly in a different partition. This is Section 7 of the guide.
- Offsets count up **per partition**, each starting at 0.
- Stop the consumer with Ctrl+C, start it again with `--from-beginning`, and every record comes back. Reading does not remove data.

Now send a few records **without** keys (remove the `parse.key` properties, or just type lines with no colon in a new producer) and observe them spread across partitions.

---

## 6. Consumer groups, lag, and rebalancing

Close the console consumer from the previous step. Open **three terminals**.

**Terminals 1 and 2: two consumers in the same group.** Run this identical command in both:

```bash
kf kafka-console-consumer.sh --topic orders --group billing \
  --property print.partition=true
```

**Terminal 3: produce and inspect.** Send a dozen keyed records with the producer from Section 5, then describe the group:

```bash
kf kafka-consumer-groups.sh --describe --group billing
```

The output has one row per partition. Look at three columns:

| Column | Meaning in the guide |
|---|---|
| `CURRENT-OFFSET` | the committed offset, the group's bookmark in `__consumer_offsets` |
| `LOG-END-OFFSET` | the latest offset in the partition |
| `LAG` | the difference, Section 14's primary health metric |
| `CONSUMER-ID` | which member owns this partition. Three partitions, two members, so one member owns two. |

**Watch a rebalance.** Press Ctrl+C in terminal 1. Within a few seconds terminal 2 starts receiving records from all three partitions. Run `--describe` again and every partition now points at the surviving member. That is the "A consumer crashes" row in Section 10.

Start terminal 1 again and describe once more. Partitions are shared out again.

**Idle consumer.** Start a third and fourth consumer in group `billing`. With three partitions, the fourth member owns nothing. Section 4.6: a group's useful size is capped by the partition count.

**Independent groups.** Start one more consumer with `--group analytics --from-beginning`. It receives the entire history even though `billing` has already read it. Groups each get their own full copy.

List all groups:

```bash
kf kafka-consumer-groups.sh --list
```

---

## 7. Reset offsets and replay

Stop **every** consumer in group `billing` first. The reset tool refuses to run while any member is active.

Preview, then apply:

```bash
kf kafka-consumer-groups.sh --group billing --topic orders --reset-offsets --to-earliest --dry-run
kf kafka-consumer-groups.sh --group billing --topic orders --reset-offsets --to-earliest --execute
```

Describe the group again. `CURRENT-OFFSET` is 0 on every partition and `LAG` equals `LOG-END-OFFSET`. Start a `billing` consumer and it re-reads everything from the start.

Other useful targets:

```bash
--to-latest                              # skip to the end
--to-offset 5 --topic orders:0           # one partition, exact position
--shift-by -3                            # step back three records on every partition
--to-datetime 2026-09-30T10:00:00.000    # first record at or after that time
--by-duration PT1H                       # one hour back from now
```

Remember what actually happened here: nothing was edited. The tool committed a new record to `__consumer_offsets` with the same key and value 0. Latest wins.

---

## 8. Peek inside `__consumer_offsets`

Internal topics are hidden from the console consumer by default. Turn that off and use the formatter that decodes the binary offset records:

```bash
kf kafka-console-consumer.sh --topic __consumer_offsets --from-beginning \
  --consumer-property exclude.internal.topics=false \
  --formatter org.apache.kafka.tools.consumer.OffsetsMessageFormatter
```

Each line is one commit, shaped like the record in Section 4.4 of the guide: a key of `(group, topic, partition)` and a value holding the offset and a timestamp. Produce a few records, let a `billing` consumer commit, and watch new lines appear.

Notes:

- The formatter class name above is for Kafka 4.x. On 3.x it was `kafka.coordinator.group.GroupMetadataManager\$OffsetsMessageFormatter`.
- The topic has 50 partitions by default. The one a group lands in is `hash(group.id) % 50`. Its leader is the group coordinator.

---

## 9. Log compaction

Create a topic that keeps only the latest value per key. The segment and ratio settings are tiny so compaction kicks in quickly instead of after hours:

```bash
kf kafka-topics.sh --create --topic customer-address --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact \
  --config segment.ms=10000 \
  --config min.cleanable.dirty.ratio=0.01 \
  --config delete.retention.ms=10000
```

Produce several updates for the same keys:

```bash
kf kafka-console-producer.sh --topic customer-address \
  --property parse.key=true --property key.separator=:
```

```
alice:1 Main St
bob:5 High St
alice:22 Oak Ave
alice:9 Elm Rd
bob:88 Park Ln
```

Compaction never touches the **active** segment, so you need it to roll. Wait at least 10 seconds after your last message, then send one more record such as `carol:1 New St` to force a new segment, and wait another 30 seconds or so.

Read it back:

```bash
kf kafka-console-consumer.sh --topic customer-address --from-beginning \
  --property print.key=true --property key.separator=" -> "
```

Only the latest `alice` and `bob` records survive. Send a **tombstone** by producing a key with a null value:

```bash
kf kafka-console-producer.sh --topic customer-address \
  --property parse.key=true --property key.separator=: --property null.marker=NULL
```

```
bob:NULL
```

After the next compaction pass, `bob` disappears from a fresh `--from-beginning` read.

---

## 10. Three brokers: replication, `acks`, and killing a broker

The single-broker setup cannot show replication. This section stands up three brokers so you can watch leader election and `min.insync.replicas` behave as described in Section 4.8 and 10 of the guide.

### 10.1 Compose file

Bring down the single broker first (`docker compose down -v`). Save this as `docker-compose.cluster.yml`:

```yaml
x-kafka-common: &kafka-common
  image: apache/kafka:latest
  environment: &kafka-env
    KAFKA_PROCESS_ROLES: broker,controller
    KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT
    KAFKA_INTER_BROKER_LISTENER_NAME: INTERNAL
    KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
    KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker-1:9093,2@broker-2:9093,3@broker-3:9093
    CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
    KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
    KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
    KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
    KAFKA_DEFAULT_REPLICATION_FACTOR: 3
    KAFKA_MIN_INSYNC_REPLICAS: 2
    KAFKA_NUM_PARTITIONS: 3
    KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    KAFKA_LOG_DIRS: /var/lib/kafka/data

services:
  broker-1:
    <<: *kafka-common
    container_name: broker-1
    ports: ["9092:9092"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 1
      KAFKA_LISTENERS: INTERNAL://:19092,EXTERNAL://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://broker-1:19092,EXTERNAL://localhost:9092
    volumes: [broker-1-data:/var/lib/kafka/data]

  broker-2:
    <<: *kafka-common
    container_name: broker-2
    ports: ["9094:9094"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 2
      KAFKA_LISTENERS: INTERNAL://:19092,EXTERNAL://:9094,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://broker-2:19092,EXTERNAL://localhost:9094
    volumes: [broker-2-data:/var/lib/kafka/data]

  broker-3:
    <<: *kafka-common
    container_name: broker-3
    ports: ["9095:9095"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 3
      KAFKA_LISTENERS: INTERNAL://:19092,EXTERNAL://:9095,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://broker-3:19092,EXTERNAL://localhost:9095
    volumes: [broker-3-data:/var/lib/kafka/data]

volumes:
  broker-1-data:
  broker-2-data:
  broker-3-data:
```

```bash
docker compose -f docker-compose.cluster.yml up -d
```

Redefine the helper to point at `broker-1` and list all three bootstrap addresses so the client can still connect when one broker is down:

```fish
function kf
    docker exec -it broker-1 /opt/kafka/bin/$argv[1] \
        --bootstrap-server broker-1:19092,broker-2:19092,broker-3:19092 $argv[2..]
end
```

### 10.2 Create a replicated topic

```bash
kf kafka-topics.sh --create --topic payments --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2
kf kafka-topics.sh --describe --topic payments
```

Each partition now shows three replicas, a leader, and `Isr: 1,2,3`. The leaders are spread across brokers, matching the diagram in Section 4.7.

### 10.3 Produce with `acks=all`

```bash
kf kafka-console-producer.sh --topic payments --producer-property acks=all \
  --property parse.key=true --property key.separator=:
```

Send a few records. They succeed silently.

### 10.4 Kill one broker

In another terminal:

```bash
docker stop broker-2
kf kafka-topics.sh --describe --topic payments
```

Any partition that broker 2 led has a **new leader**, and every `Isr` list is missing `2`. Go back to the producer and send more records. They still succeed: two in-sync replicas remain, which satisfies `min.insync.replicas=2`. Zero data loss, no producer change. That is the "A broker crashes" row of Section 10.

### 10.5 Kill a second broker

```bash
docker stop broker-3
```

Send another record from the `acks=all` producer. After a short delay you see `NotEnoughReplicasException`. Only one replica is in sync and the floor is two, so the leader refuses the write rather than pretend it is durable. Reads still work, which you can confirm with a console consumer.

Start a producer **without** `acks=all` (drop the `--producer-property`, or set `acks=1`) and the same write goes through. `min.insync.replicas` only gates `acks=all`, exactly as Section 4.8 says.

### 10.6 Recover

```bash
docker start broker-2 broker-3
kf kafka-topics.sh --describe --topic payments
```

Within a few seconds the ISR lists refill to `1,2,3`. Leaders may stay where they moved to; that is fine. To rebalance leadership back to the preferred replicas:

```bash
kf kafka-leader-election.sh --election-type preferred --all-topic-partitions
```

---

## 11. Optional: a web UI

Add this service to either compose file to browse topics, messages, consumer groups, and lag in a browser:

```yaml
  kafka-ui:
    image: ghcr.io/kafbat/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:19092        # single broker
      # KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: broker-1:19092,broker-2:19092,broker-3:19092   # cluster
```

Open <http://localhost:8080>. The **Consumers** page shows the same lag numbers as `kafka-consumer-groups.sh --describe`.

---

## 12. Write your own producer and consumer (.NET)

Once the console tools make sense, write the same thing in code. This uses the `Confluent.Kafka` client, which wraps librdkafka.

```bash
mkdir -p samples/dotnet && cd samples/dotnet
dotnet new console -n KafkaPlay && cd KafkaPlay
dotnet add package Confluent.Kafka
```

Minimal `Program.cs` that produces three keyed records and then consumes them as group `dotnet-demo`:

```csharp
using Confluent.Kafka;

const string topic = "orders";
const string bootstrap = "localhost:9092";

// Producer: acks=all and idempotence on, as the guide recommends.
var producerConfig = new ProducerConfig
{
    BootstrapServers = bootstrap,
    Acks = Acks.All,
    EnableIdempotence = true,
    LingerMs = 5,
};

using (var producer = new ProducerBuilder<string, string>(producerConfig).Build())
{
    foreach (var (key, value) in new[] { ("customer-42", "login"), ("customer-42", "checkout"), ("customer-7", "login") })
    {
        var result = await producer.ProduceAsync(topic, new Message<string, string> { Key = key, Value = value });
        Console.WriteLine($"produced {key}:{value} -> partition {result.Partition.Value} offset {result.Offset.Value}");
    }
}

// Consumer: manual commit after processing gives at-least-once.
var consumerConfig = new ConsumerConfig
{
    BootstrapServers = bootstrap,
    GroupId = "dotnet-demo",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
};

using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
consumer.Subscribe(topic);

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
try
{
    while (!cts.IsCancellationRequested)
    {
        var record = consumer.Consume(cts.Token);
        Console.WriteLine($"consumed p{record.Partition.Value}@{record.Offset.Value} {record.Message.Key}:{record.Message.Value}");
        consumer.Commit(record);   // commit after processing
    }
}
catch (OperationCanceledException) { }
finally
{
    consumer.Close();
}
```

```bash
dotnet run
```

Then check the group from the CLI:

```bash
kf kafka-consumer-groups.sh --describe --group dotnet-demo
```

Try flipping `EnableAutoCommit` to `true`, moving the `Commit` call before the `Console.WriteLine`, or killing the process mid-loop, and predict what `CURRENT-OFFSET` will show before you look.

The `.gitignore` already covers `bin/` and `obj/`.

---

## 13. Tear down

```bash
# single broker
docker compose down          # stop, keep data
docker compose down -v       # stop and delete the data volume

# three-broker cluster
docker compose -f docker-compose.cluster.yml down -v
```

Tarball option: Ctrl+C the broker terminal, then delete the log directory named by `log.dirs` in `config/server.properties` (default `/tmp/kraft-combined-logs`) if you want a clean slate.

---

## 14. Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| `docker: command not found` in WSL | Docker Desktop's WSL integration is off for this distro, or Docker is not installed in WSL. Enable it in Docker Desktop settings, or `sudo apt install docker.io docker-compose-v2`. |
| Client on the host cannot connect, but `docker exec` works | The advertised listener does not match how you connect. Host clients must use `localhost:9092` (the `EXTERNAL` listener). Containers must use `kafka:19092`. |
| `Connection to node -1 (localhost/127.0.0.1:9092) could not be established` | Broker is still starting, or the port is not published. Check `docker compose logs kafka`. |
| Topic auto-created with 3 partitions and RF 1 when you did not ask | `KAFKA_AUTO_CREATE_TOPICS_ENABLE` is on. The compose files above turn it off so you create topics deliberately. |
| `--reset-offsets` fails with "group is not empty" | A consumer in that group is still running. Stop every member first. |
| Compaction never happens | The records are all still in the active segment. Wait past `segment.ms` and produce one more record to roll it. |
| `NotEnoughReplicasException` on a single broker | The topic has `min.insync.replicas` higher than 1. On one broker that can never be satisfied for `acks=all`. |
| Cluster brokers keep restarting with `InconsistentClusterIdException` | Old volumes from a previous run with a different `CLUSTER_ID`. Run `docker compose -f docker-compose.cluster.yml down -v`. |
| Console consumer shows nothing for a new group | New groups start at `latest` by default. Add `--from-beginning`, or produce something new. |
