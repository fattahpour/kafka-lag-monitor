# Kafka Lag Monitor

A VS Code extension for monitoring Apache Kafka consumer lag, browsing topic
metadata, and (in later phases) browsing messages and producing test
messages — all from the Explorer sidebar.

## Status

**Phase 1 (this version):** an Explorer view showing, per configured
connection, the list of topics (with partition counts) and consumer groups
(with total lag and per-partition breakdown). Connections are managed with
the **Kafka: Add Connection** command (the `+` icon in the Explorer view
title bar) and the **Kafka: Edit Connection**, **Kafka: Remove Connection**,
and **Kafka: Reconnect** commands (right-click a connection), backed by VS
Code settings and SecretStorage. Clicking a topic opens a Topic Metadata
webview showing its partitions (leader, replicas, ISR) and configuration.
Clicking a consumer group opens a Lag Dashboard webview showing total lag,
overall status, and a per-topic/per-partition progress-bar breakdown, with a
manual refresh button and an auto-refresh toggle (interval configured via
`kafkaLagMonitor.pollIntervalSeconds`). Right-clicking a topic and choosing
**Kafka: Browse Messages** opens a Message Browser webview showing a table of
the topic's most recent messages (Offset, Timestamp, Key, Value, Headers) for
a chosen partition, with Earliest/Prev/Next/Latest/Refresh navigation and a
partition selector. Right-clicking a topic and choosing **Kafka: Produce
Message** opens a Produce webview with Partition, Key, Value, and Headers
fields and a Send button; on success the result banner shows the partition
and offset of the produced message, and on failure it shows the kafkajs error
message verbatim.

SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) and SSL connections are supported,
including mutual TLS (mTLS) with a client certificate, private key, and an
optional custom CA certificate (configured as file paths; an encrypted
private key's passphrase is stored in SecretStorage).

## Configuration

The easiest way to add a connection is the **Kafka: Add Connection** command
(the `+` icon in the Explorer view title bar), which prompts for a name,
brokers, SSL (plain, or "with client certificate" for mTLS — CA/cert/key file
paths plus an optional private key passphrase), authentication, and (for
SASL) a username/password. SASL credentials and the mTLS key passphrase are
stored in VS Code's SecretStorage, not in settings.

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
  },
  {
    "name": "mtls-cluster",
    "brokers": ["broker1:9093"],
    "sasl": null,
    "ssl": { "ca": "/etc/kafka/ca.pem", "cert": "/etc/kafka/client-cert.pem", "key": "/etc/kafka/client-key.pem" },
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
total lag of 3. Clicking `order-service` opens the Lag Dashboard, which should
show a Total Lag of 3 with one `orders.events` section and per-partition
progress bars. Right-click `orders.events` and choose **Kafka: Browse
Messages** — the panel should open for partition 0 showing the most recent
messages with Offset/Timestamp/Key/Value/Headers columns; use the partition
selector and the Earliest/Prev/Next/Latest/Refresh buttons to navigate.
Right-click `orders.events` and choose **Kafka: Produce Message** — fill in a
key (e.g. `order-6`), a value (e.g. `{"id":6,"status":"created"}`), and one
header (e.g. `trace-id` / `abc-123`), then click **Send**. The result banner
should show `Sent to partition <p>, offset <o>`. Switch to (or re-open via
**Kafka: Browse Messages**) the Message Browser panel and click **Latest** to
confirm the new message appears at that offset with the key, value, and
header you entered.
