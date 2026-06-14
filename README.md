# Kafka Lag Monitor

A VS Code extension for monitoring Apache Kafka consumer lag, browsing topic
metadata, and (in later phases) browsing messages and producing test
messages — all from the Explorer sidebar.

## Status

**Phase 1 (this version):** read-only Explorer view showing, per configured
connection, the list of topics (with partition counts) and consumer groups
(with total lag and per-partition breakdown). Connections are configured
directly in `settings.json` — a connection-management wizard, the Lag
Dashboard, Message Browser, and Produce webviews are planned in follow-up
phases (see `docs/superpowers/specs/2026-06-13-kafka-lag-monitor-design.md`).

SASL/SSL authentication is not yet wired up; only PLAINTEXT and SSL-without-SASL
connections are supported.

## Configuration

Add one or more connection profiles to your VS Code settings:

```jsonc
"kafkaLagMonitor.connections": [
  {
    "name": "local-cluster",
    "brokers": ["localhost:9091", "localhost:9092", "localhost:9095"],
    "sasl": null,
    "ssl": false,
    "clientId": "kafka-lag-monitor"
  }
],
"kafkaLagMonitor.lagWarningThreshold": 100,
"kafkaLagMonitor.lagCriticalThreshold": 1000
```

## Development

```bash
npm install
npm run compile   # or: npm run watch
npm test          # unit tests (node:test)
```

Press `F5` in VS Code to launch the Extension Development Host.

## Manual integration test

With the local `kafka-orchestrator` cluster running (`localhost:9091`):

```bash
cd ../java-kafka-cli
./bin/kafka-topics.sh --bootstrap-server localhost:9091 --create --topic orders.events --partitions 3 --replication-factor 1
for i in 1 2 3 4 5; do echo "order-$i"; done | ./bin/kafka-console-producer.sh --bootstrap-server localhost:9091 --topic orders.events
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9091 --topic orders.events --group order-service --max-messages 2
```

Then `F5` the extension and expand `local-cluster` in the Explorer sidebar —
`orders.events` should show 3 partitions, and `order-service` should show a
total lag of 3.
