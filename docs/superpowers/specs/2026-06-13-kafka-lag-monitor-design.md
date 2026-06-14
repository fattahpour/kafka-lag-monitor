# Kafka Lag Monitor — VS Code Extension Design

## Overview

"Kafka Lag Monitor" is a VS Code extension for monitoring Apache Kafka consumer lag, browsing topic messages, viewing topic/partition metadata, and producing test messages — all from a sidebar tree view and a set of webview panels. It connects directly to Kafka brokers (no external CLI or Java dependency required).

## Goals

- Surface consumer group lag per topic/partition, visually (bar charts) and at a glance (sidebar badges).
- Let a developer see *which messages a consumer group has already consumed vs. which remain* by browsing real message payloads around the group's committed offset.
- Show topic metadata (partitions, replicas, ISR, configs).
- Allow producing test messages to a topic.
- Support multiple named cluster connections, including SASL/SSL.

## Non-Goals (v1)

- No consumer group admin mutations (no `resetOffsets`, no `deleteGroups`, no ACL management). The extension is read-only with respect to consumer groups — it must never join a real consumer group or commit offsets, to avoid disrupting production consumers.
- No batch/file-based produce (single message only).
- No long-running background polling when no relevant panel is open.

## Tech Stack

- TypeScript, bundled with esbuild (consistent with the author's other VS Code extensions).
- [`kafkajs`](https://kafka.js.org/) as the Kafka client library — supports PLAINTEXT, SASL (PLAIN/SCRAM), and SSL.
- Webviews are vanilla HTML/CSS/JS using VS Code's CSS variables for theming. No charting library — lag bars are CSS divs, trend sparklines (future) are inline SVG.
- Testing: plain mocha/ts-mocha for logic-only unit tests (kafkajs clients injected as interfaces so they can be faked), `@vscode/test-cli` for extension/tree-provider smoke tests.

## Architecture

```
extension.ts (activation: registers tree view, commands, webview panels)
├── connection/
│   ├── connectionManager.ts   — KafkaJS client cache per profile; connect/disconnect/status
│   ├── profileStore.ts        — reads connection profiles from settings; reads/writes
│   │                             credentials via SecretStorage
├── kafka/
│   ├── adminService.ts        — listTopics, fetchTopicMetadata, describeConfigs,
│   │                             listGroups, describeGroups, fetchOffsets, lag calculation
│   ├── consumerService.ts     — ephemeral consumer for message browsing
│   │                             (unique random groupId, manual seek, no commits)
│   └── producerService.ts     — cached producer per connection, send single message
├── treeView/
│   └── kafkaExplorerProvider.ts — TreeDataProvider: connections → Topics / Consumer Groups
├── webview/
│   ├── lagDashboardPanel.ts
│   ├── messageBrowserPanel.ts
│   ├── producePanel.ts
│   └── topicMetadataPanel.ts
└── polling/
    └── pollingManager.ts       — interval timers scoped to the currently-visible
                                   Lag Dashboard panel only
```

### Data flow

1. User adds a connection profile (broker list + optional SASL/SSL). Credentials go to SecretStorage.
2. Expanding a connection node in the tree lazily creates a cached kafkajs `Kafka` + `Admin` client via `connectionManager`.
3. The tree populates **Topics** (via `admin.listTopics` / `fetchTopicMetadata`) and **Consumer Groups** (via `admin.listGroups`, with per-group total lag computed via `adminService` for the badge).
4. Clicking a topic opens the **Topic Metadata** webview; right-click offers **Produce** and **Browse Messages**.
5. Clicking a consumer group opens the **Lag Dashboard** webview, scoped to that group (one section per topic it consumes).
6. Clicking a partition row under a group in the tree opens the **Message Browser**, pre-scoped to that topic/partition/group.
7. The Lag Dashboard's refresh button (and optional auto-poll) re-runs the lag fetch and `postMessage`s updated data into the webview.

## Connection Management

### Settings schema

```jsonc
"kafkaLagMonitor.connections": [
  {
    "name": "local-cluster",
    "brokers": ["localhost:9091", "localhost:9092", "localhost:9095"],
    "sasl": null,            // or { "mechanism": "plain" | "scram-sha-256" | "scram-sha-512" }
    "ssl": false,
    "clientId": "kafka-lag-monitor"
  }
],
"kafkaLagMonitor.lagWarningThreshold": 100,
"kafkaLagMonitor.lagCriticalThreshold": 1000,
"kafkaLagMonitor.pollIntervalSeconds": 10
```

SASL username/password (and an SSL key passphrase, if any) are stored in VS Code `SecretStorage`, keyed by connection name — never written to `settings.json`.

### Commands

- **Kafka: Add Connection** — quickinput wizard (name, brokers, auth type, then credential prompts if needed).
- **Kafka: Edit Connection**
- **Kafka: Remove Connection** — also removes its SecretStorage entries.
- **Kafka: Reconnect** — re-creates the cached client for a connection after a failure.

### Connection lifecycle

`connectionManager` lazily creates and caches a kafkajs `Kafka` instance and `Admin` client per profile on first tree expansion. Connection status (connected ✓ / error ⚠ / idle ⚪) is reflected in the tree node icon, with the error message in the tooltip.

## Sidebar Tree View

Single nested `TreeDataProvider`, one root node per connection profile:

```
🔌 local-cluster ✓
  ▾ Topics (12)
      orders.events   (6 partitions)   → click: Topic Metadata webview
                                          right-click: Produce, Browse Messages
      payments.dlq    (3 partitions)
  ▾ Consumer Groups (4)
      order-service  ●1.2k             → click: Lag Dashboard webview
        ▾ orders.events  ●1.2k
            p0: 401/600 (199)
            p3: 5/600 (595)
      audit-logger   ●0
```

Lag badge color comes from `lagWarningThreshold` / `lagCriticalThreshold`. Sidebar badges are refreshed via a manual "Refresh" action on the connection node — they are **not** continuously auto-polled, to keep background broker calls bounded to what the user has actually opened.

## Webview Panels

### Lag Dashboard (per consumer group)

- **Fetch**: `admin.fetchOffsets({ groupId })` for committed offsets per partition, plus `admin.fetchTopicOffsets(topic)` for the high watermark, for each topic the group consumes.
- **Lag formula**: `lag = highWatermark - committedOffset`. If no committed offset exists yet, the partition is shown as "not started" with `lag = highWatermark`.
- **UI**: summary cards (total lag, count of partitions over the warning threshold, topic name(s)) + one horizontal bar per partition, colored by warning/critical thresholds. One section per topic if the group consumes multiple topics.
- **Refresh**: manual refresh button, plus an auto-poll toggle using `pollIntervalSeconds`. Auto-poll runs **only while this panel is the visible tab** (`pollingManager` starts the timer on reveal, stops it on hide/dispose).

### Message Browser (topic + partition, optionally + groupId)

- **Window**: on open, fetch `endOffset` (high watermark) and, if a `groupId` is provided, `groupOffset` (committed offset). Default window = `[max(groupOffset - 25, 0), min(groupOffset + 25, endOffset)]`, centered on the consumed/pending boundary. Without a `groupId`, defaults to the most recent 50 messages.
- **Fetching**: an ephemeral kafkajs consumer with a unique random `groupId` (e.g. `kafka-lag-monitor-browse-<uuid>`) is created, assigned alone (so it receives all partitions of the subscribed topic), seeks each relevant partition to the window start, fetches the batch, then disconnects. It never commits offsets and never reuses a real group's id.
- **Table columns**: Offset | tag (`consumed` if `offset < groupOffset`, else `pending`) | Timestamp | Key | Value (JSON pretty-printed if parseable, else raw text, truncated with expand) | Headers.
- **Controls**: partition selector, "jump to earliest / latest / group offset" shortcuts, page size, prev/next. Manual refresh only — each refresh recomputes the window.

### Produce (per topic)

- **Form**: partition (auto-by-key or explicit partition number), key (text), value (textarea), headers (add/remove key-value rows), Send button.
- Uses a `Producer` cached per connection (created lazily on first send, disposed on extension deactivation or connection removal).
- **Result**: success banner shows `{ partition, offset }` from kafkajs `RecordMetadata`; failure banner shows the kafkajs error message verbatim.

### Topic Metadata (per topic)

- `admin.fetchTopicMetadata({ topics: [topic] })` → partitions table (partition id, leader, replicas, ISR).
- `admin.describeConfigs({ resources: [{ type: TOPIC, name: topic }] })` → config table (e.g. `retention.ms`, `cleanup.policy`).
- Manual refresh only — this data changes rarely.

## Polling & Refresh

`pollingManager` owns a single interval timer tied to the active Lag Dashboard panel:

- Starts when the panel becomes visible and auto-poll is enabled.
- Stops on `onDidChangeViewState` (hidden) or `onDidDispose`.
- A failed poll tick shows an inline warning in the dashboard but does not stop the timer. After 3 consecutive failures, the dashboard prompts the user to use **Kafka: Reconnect**.

No other panel or the sidebar tree auto-polls; they refresh on open or on manual refresh.

## Error Handling & Logging

- An output channel, "Kafka Lag Monitor", receives kafkajs's internal logs via a custom `logCreator`, plus extension-level diagnostic messages.
- Connection failures: tree node shows ⚠ with the error in the tooltip, a context-menu "Reconnect" action, and a one-time warning notification per connection (not repeated on every failed poll).
- Webview-level failures (e.g., topic deleted mid-session, ACL denied on `describeConfigs`): an inline error banner with a "Retry" button; the underlying error is also logged to the output channel.
- Produce failures surface the kafkajs error message directly in the result banner.

## Testing Strategy

- **Logic-only unit tests** (mocha/ts-mocha), with `AdminService`/`ConsumerService`/`ProducerService` depending on injected kafkajs client interfaces so fakes can be substituted:
  - Lag calculation, including the "no committed offset yet" case and multi-partition aggregation.
  - Message-window computation and consumed/pending boundary tagging.
  - Connection profile validation (malformed broker lists, missing SASL fields, etc.).
  - Tree item construction: labels, icons, and badge colors at each threshold boundary.
- **Extension smoke tests** via `@vscode/test-cli` — activation, tree view registration, command registration.
- **Manual integration check** (documented in the README): against the author's local `kafka-orchestrator` cluster (`localhost:9091`) — create a topic, produce messages, run a consumer to generate lag, and verify the dashboard numbers and message browser tags.

## Phasing / Roadmap

1. **Scaffold + connections** — project setup, `connectionManager`, `profileStore` (settings + SecretStorage), add/edit/remove/reconnect commands, sidebar tree (Topics + Consumer Groups with lag badges), Topic Metadata webview.
2. **Lag Dashboard** webview — summary cards, per-partition bars, manual refresh, auto-poll toggle.
3. **Message Browser** webview — offset window computation, ephemeral consumer, consumed/pending tagging, pagination controls.
4. **Produce** webview — form, cached producer, result banner.
5. **Polish** — error handling pass across all panels, output-channel logging, README/CHANGELOG, icon, `vsce` packaging.

## Branding

- Extension display name: "Kafka Lag Monitor"
- Publisher: `fattahpour` (consistent with the author's `edi-insight` extension)
