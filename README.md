# Kafka Lag Monitor

A VS Code extension for monitoring Apache Kafka consumer lag, browsing topic
metadata, and (in later phases) browsing messages and producing test
messages — all from the Explorer sidebar.

## Status

**Phase 1 (this version):** an Explorer view showing, per configured
connection, the list of topics (with partition counts) and consumer groups
(with total lag and per-partition breakdown). Connections are managed with
the **Kafka: Add/Edit/Remove Connection** and **Kafka: Reconnect** commands
(available from the Explorer view title bar and by right-clicking a
connection), backed by VS Code settings and SecretStorage. Clicking a topic
opens a Topic Metadata webview showing its partitions (leader, replicas, ISR)
and configuration. The Lag Dashboard, Message Browser, and Produce webviews
are planned in follow-up phases (see
`docs/superpowers/specs/2026-06-13-kafka-lag-monitor-design.md`).

SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) and SSL connections are supported.
mTLS / client-certificate SSL is not yet supported.

## Configuration

The easiest way to add a connection is the **Kafka: Add Connection** command
(the `+` icon in the Explorer view title bar), which prompts for a name,
brokers, SSL, authentication, and (for SASL) a username/password. SASL
credentials are stored in VS Code's SecretStorage, not in settings.

Connection profiles can also be viewed or hand-edited in your VS Code
settings (SASL credentials are not stored here — use the Add/Edit Connection
commands for those):

```jsonc
"kafkaLagMonitor.connections": [
  {
    "name": "local-cluster",
    "brokers": ["localhost:9091", "localhost:9092", "localhost:9095"],
    "sasl": null,
    "ssl": false,
    "clientId": "kafka-lag-monitor"
  },
  {
    "name": "secure-cluster",
    "brokers": ["broker1:9093"],
    "sasl": { "mechanism": "scram-sha-512" },
    "ssl": true,
    "clientId": "kafka-lag-monitor"
  }
],
"kafkaLagMonitor.lagWarningThreshold": 100,
"kafkaLagMonitor.lagCriticalThreshold": 1000
```

## Commands

- **Kafka: Add Connection** — wizard to create a new connection profile.
- **Kafka: Edit Connection** — wizard to update an existing connection profile (leave the username/password fields blank to keep the currently stored credentials).
- **Kafka: Remove Connection** — removes a connection profile and its stored credentials, after confirmation.
- **Kafka: Reconnect** — disconnects and re-creates a connection's client (useful after editing brokers or credentials).
- **Kafka Lag Monitor: Refresh** — refreshes the Explorer view.

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
