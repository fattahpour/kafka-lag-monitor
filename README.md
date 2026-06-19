# Kafka Lag Monitor

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Kafka Lag Monitor is a VS Code extension for inspecting Apache Kafka clusters
from the Explorer sidebar. It shows topics, consumer groups, lag totals,
partition lag, topic metadata, recent messages, and a simple producer panel for
test messages.

## Plugin address

- Azure DevOps: https://dev.azure.com/fattahpour/kafka-lag-monitor/_git/kafka-lag-monitor
- GitHub: https://github.com/fattahpour/kafka-lag-monitor
- VSIX package: `kafka-lag-monitor-0.0.1.vsix`

The extension is not published to the VS Code Marketplace yet. Install it from
the VSIX package in this repository.

## Install the extension

1. Open VS Code.
2. Open the Command Palette.
3. Run **Extensions: Install from VSIX...**.
4. Select `kafka-lag-monitor-0.0.1.vsix`.
5. Reload VS Code if prompted.

For development, open this repository in VS Code and press `F5` to launch the
Extension Development Host.

## How to use

1. Open the **Explorer** sidebar in VS Code.
2. Find the **Kafka Lag Monitor** view.
3. Click the `+` icon in the view title bar, or run **Kafka: Add Connection**
   from the Command Palette.
4. Enter a connection name and Kafka brokers, for example `localhost:9092` or
   `broker1:9093,broker2:9093`.
5. Choose the connection security mode: no SSL, SSL, mTLS, or SASL.
6. Expand the connection in **Kafka Lag Monitor**.
7. Expand **Topics** to inspect topic names and partition counts.
8. Click a topic to open its metadata.
9. Right-click a topic and choose **Kafka: Browse Messages** to inspect recent
   records.
10. Right-click a topic and choose **Kafka: Produce Message** to send a test
    record.
11. Expand **Consumer Groups** to see total lag by group.
12. Click a consumer group to open the Lag Dashboard.

Connection passwords and mTLS key passphrases are stored in VS Code
SecretStorage. They are not written to connection files.

## Main features

- Explorer tree for Kafka connections, topics, and consumer groups.
- Topic metadata view with partitions, leaders, replicas, ISR, and configs.
- Lag Dashboard with total lag and per-topic/per-partition breakdown.
- Message Browser with partition selection and Earliest, Prev, Next, Latest,
  and Refresh navigation.
- Produce Message panel with partition, key, value, and headers fields.
- Connection wizard for plain Kafka, SSL, mTLS, and SASL.
- Connection edit, remove, reconnect, and refresh commands.

## Commands

- **Kafka: Add Connection** - create a new Kafka connection profile.
- **Kafka: Edit Connection** - update an existing connection profile.
- **Kafka: Remove Connection** - remove a connection and its stored secrets.
- **Kafka: Reconnect** - reconnect after changing brokers or credentials.
- **Kafka: Browse Messages** - open a topic message browser.
- **Kafka: Produce Message** - open a topic producer panel.
- **Kafka Lag Monitor: Refresh** - reload the Explorer tree.

## Configuration

The recommended setup path is **Kafka: Add Connection**. It prompts for brokers,
SSL/mTLS settings, SASL mechanism, username, and password.

Connection profiles are stored in the first workspace folder at:

```text
.vscode/kafka-lag-monitor.connections.json
```

If that file does not exist yet, the extension uses a default local profile for
`localhost:9092`. You can also inspect or hand-edit non-secret connection
settings in the workspace file:

```json
{
  "connections": [
    {
      "name": "local-cluster",
      "brokers": ["localhost:9092"],
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
      "ssl": {
        "ca": "/etc/kafka/ca.pem",
        "cert": "/etc/kafka/client-cert.pem",
        "key": "/etc/kafka/client-key.pem"
      },
      "clientId": "kafka-lag-monitor"
    }
  ]
}
```

Lag thresholds and dashboard polling remain VS Code settings:

```jsonc
"kafkaLagMonitor.lagWarningThreshold": 100,
"kafkaLagMonitor.lagCriticalThreshold": 1000,
"kafkaLagMonitor.pollIntervalSeconds": 10
```

## Build and package

Install dependencies, compile, and run tests:

```bash
npm install
npm run compile
npm test
```

Build the VSIX package:

```bash
npx @vscode/vsce package --allow-missing-repository
```

The package output is:

```text
kafka-lag-monitor-0.0.1.vsix
```

## Local integration test

With Kafka running locally, create a topic and some lag:

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic orders.events --partitions 3 --replication-factor 1
for i in 1 2 3 4 5; do echo "order-$i"; done | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders.events
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic orders.events --group order-service --max-messages 2
```

Then run the extension with `F5`, add a `localhost:9092` connection, expand the
connection, and check:

- `orders.events` appears under **Topics**.
- `order-service` appears under **Consumer Groups**.
- Clicking `order-service` opens the Lag Dashboard.
- Right-clicking `orders.events` opens Browse Messages and Produce Message.

## License

[MIT](LICENSE)
