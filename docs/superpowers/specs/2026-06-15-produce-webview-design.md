# Produce Webview — Design

## Overview

Implements roadmap Phase 4 (see `docs/superpowers/specs/2026-06-13-kafka-lag-monitor-design.md`, "Phasing / Roadmap" item 4) on top of the merged Phase 1–3 foundation. Adds a "Produce" webview, opened via a right-click "Kafka: Produce Message" action on a topic node, that sends a single message (key, value, optional explicit partition, optional headers) to the topic using a kafkajs `Producer` cached per connection.

## Goals

- Right-clicking a topic node in the sidebar and choosing **Kafka: Produce Message** opens a "Produce" webview for that topic (singleton panel, same coexistence/retitle pattern as Topic Metadata and the Message Browser — producing to a different topic retitles/re-renders the same panel).
- Form fields:
  - **Partition**: a `<select>` populated with `Auto (by key)` (value `""`) plus `0..partitionCount-1`.
  - **Key**: a text input. Empty input means "no key" (`null`), not an empty-string key.
  - **Value**: a textarea, sent verbatim as a string.
  - **Headers**: zero or more key/value row pairs, with "+ Add header" / per-row "Remove" buttons. Rows with an empty key are dropped when sending.
  - **Send** button.
- On Send, the form posts to the controller, which sends via `ProducerService.send()` using a producer **cached per connection** (`ConnectionManager.getProducerService`), created lazily on first send for that connection and disposed when the connection is disconnected/reconnected.
- **Result banner**: on success, shows `Sent to partition <p>, offset <o>` (from kafkajs's `RecordMetadata`); on failure, shows the kafkajs error message verbatim. The Send button is disabled while a send is in flight and re-enabled when the result arrives.
- Form values are **not cleared** after a send (success or failure) — the user can tweak key/value/headers and send again.
- `package.json`: new `kafkaLagMonitor.produce` command ("Kafka: Produce Message") with a `view/item/context` entry for `viewItem == kafkaTopic`, alongside the existing `kafkaLagMonitor.browseMessages` entry.
- `README.md` "Status" section updated: Produce webview moves from "planned" to implemented, with a short description; the roadmap-remaining note is removed (Phases 1–4 of the original roadmap are now complete — only Phase 5 "Polish" remains).

## Non-Goals

- **Tombstones** (`value: null`) — the Value textarea always sends a string (possibly empty `""`), never `null`. Deferred to a later phase if needed.
- **JSON validation/formatting of the Value field** — sent exactly as typed; no pretty-printing or parse-on-input (unlike the Message Browser's read-only pretty-print).
- **Duplicate header keys** — if the user adds two rows with the same key, the later row silently wins (`Record<string, string>` semantics). No validation or warning.
- **Batch/file-based produce** — single message only, per the master spec's non-goals.
- **Disposing cached producers on extension `deactivate()`** — out of scope, matching the existing admin-client precedent (also not disposed on `deactivate()` today). Cached producers ARE disposed on `disconnect()`/`reconnect()` for a connection.
- **A "Retry" button on the error page** — matches existing precedent (Topic Metadata, Lag Dashboard, Message Browser all use a plain `renderErrorHtml` with no Retry button).
- Any consumer-group admin mutation — out of scope for the whole extension per the master spec's non-goals (unaffected by this phase; Produce only uses a `Producer`, never a consumer/group).

## Architecture & Components

### `src/kafka/producerClient.ts` (new)

Pure interface types, no kafkajs or vscode import — mirrors `adminClient.ts`/`consumerClient.ts`'s role as the thin seam between `kafkajs` and the rest of the extension.

```typescript
export interface ProducerSendResult {
  partition: number;
  offset: string;
}

export interface KafkaProducerClient {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  send(args: {
    topic: string;
    partition?: number;
    key: string | null;
    value: string;
    headers: Record<string, string>;
  }): Promise<ProducerSendResult>;
}
```

`partition: undefined` means "let kafkajs's default partitioner choose" (by key, or round-robin if `key` is `null`).

### `src/kafka/kafkaProducerAdapter.ts` (new, untested glue)

kafkajs-specific adapter implementing `KafkaProducerClient`, mirroring `kafkaAdminAdapter.ts`/`kafkaConsumerAdapter.ts`. Not unit tested — same precedent as those two adapters (thin wrapper around a third-party client, exercised by the manual integration test).

```typescript
import { Producer } from 'kafkajs';
import { KafkaProducerClient } from './producerClient';

export function createKafkaProducerClient(producer: Producer): KafkaProducerClient {
  return {
    connect: () => producer.connect(),
    disconnect: () => producer.disconnect(),
    send: async ({ topic, partition, key, value, headers }) => {
      const [metadata] = await producer.send({
        topic,
        messages: [{ partition, key, value, headers }],
      });
      return { partition: metadata.partition, offset: metadata.baseOffset ?? '0' };
    },
  };
}
```

`metadata.baseOffset` is typed as optional (`string | undefined`) by kafkajs; the `?? '0'` fallback keeps `ProducerSendResult.offset` a plain `string` without throwing on a (practically never observed) missing value.

### `src/kafka/producerService.ts` (new, unit-tested)

The one place with real request-normalization logic — analogous to `ConsumerService`/`AdminService` wrapping their respective client interfaces.

```typescript
import { KafkaProducerClient, ProducerSendResult } from './producerClient';

export interface HeaderEntry {
  key: string;
  value: string;
}

export interface ProduceRequest {
  topic: string;
  partition: number | null;
  key: string;
  value: string;
  headers: HeaderEntry[];
}

export class ProducerService {
  constructor(private readonly client: KafkaProducerClient) {}

  async send(request: ProduceRequest): Promise<ProducerSendResult> {
    const headers: Record<string, string> = {};
    for (const header of request.headers) {
      if (header.key === '') continue;
      headers[header.key] = header.value;
    }
    return this.client.send({
      topic: request.topic,
      partition: request.partition ?? undefined,
      key: request.key === '' ? null : request.key,
      value: request.value,
      headers,
    });
  }
}
```

### `src/connection/connectionManager.ts` additions

Add a `ProducerClientFactory` and a second per-profile cache (`producers`), mirroring the existing `clients` (admin) cache. The producer for a profile is created and connected **lazily**, on the first `getProducerService()` call after the connection is `connected`, and is disposed alongside the admin client in `disconnect()`/`reconnect()`.

```typescript
import { KafkaProducerClient } from '../kafka/producerClient';
import { ProducerService } from '../kafka/producerService';

export type ProducerClientFactory = (profile: ConnectionProfile) => Promise<KafkaProducerClient>;

export class ConnectionManager {
  private readonly clients = new Map<string, KafkaAdminClient>();
  private readonly producers = new Map<string, KafkaProducerClient>();
  private readonly states = new Map<string, ConnectionState>();
  private readonly generations = new Map<string, number>();

  constructor(
    private readonly createAdminClient: AdminClientFactory,
    private readonly createProducerClient: ProducerClientFactory,
  ) {}

  // ... getState, nextGeneration, isCurrentGeneration, connect unchanged ...

  async reconnect(profile: ConnectionProfile): Promise<void> {
    const gen = this.nextGeneration(profile.name);
    this.states.set(profile.name, { status: 'connecting' });

    const existing = this.clients.get(profile.name);
    if (existing) {
      this.clients.delete(profile.name);
      await existing.disconnect().catch(() => undefined);
    }
    const existingProducer = this.producers.get(profile.name);
    if (existingProducer) {
      this.producers.delete(profile.name);
      await existingProducer.disconnect().catch(() => undefined);
    }

    // ... rest unchanged (create admin client, connect, set state) ...
  }

  async disconnect(profileName: string): Promise<void> {
    this.nextGeneration(profileName);
    const client = this.clients.get(profileName);
    if (client) {
      await client.disconnect();
      this.clients.delete(profileName);
    }
    const producer = this.producers.get(profileName);
    if (producer) {
      this.producers.delete(profileName);
      await producer.disconnect().catch(() => undefined);
    }
    this.states.set(profileName, { status: 'idle' });
  }

  getAdminService(profileName: string): AdminService | undefined {
    // unchanged
  }

  async getProducerService(profile: ConnectionProfile): Promise<ProducerService | undefined> {
    if (this.getState(profile.name).status !== 'connected') return undefined;

    let client = this.producers.get(profile.name);
    if (!client) {
      client = await this.createProducerClient(profile);
      await client.connect();
      this.producers.set(profile.name, client);
    }
    return new ProducerService(client);
  }
}
```

Notes:
- `getAdminService(profileName: string)` stays unchanged (synchronous, keyed by name only) — `getProducerService` takes the full `ConnectionProfile` (not just the name) because the `ProducerClientFactory` needs it to build a `Kafka` instance on first use, the same way `connect()` does for the admin client.
- Producer disposal uses `.catch(() => undefined)` in both `disconnect()` and `reconnect()` — a producer-disconnect failure must not block the admin client/state cleanup that follows it.

### Produce render — `src/webviews/producePanel.ts` (new)

No server-fetched data is embedded in the initial page (unlike the Message Browser's `initialData`) — the only dynamic inputs are the topic name (for the title) and the partition count (for the `<select>`). `escapeHtml` is reused from `topicMetadataPanel.ts`.

```typescript
import { escapeHtml } from './topicMetadataPanel';
import { HeaderEntry } from '../kafka/producerService';

export interface ProduceSendMessage {
  type: 'send';
  partition: number | null;
  key: string;
  value: string;
  headers: HeaderEntry[];
}

export function renderProduceHtml(topic: string, partitionCount: number): string {
  const safeTopic = escapeHtml(topic);
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Produce: ${safeTopic}</title>
<style>
  body { font-family: var(--vscode-font-family, sans-serif); padding: 0 16px; }
  label { display: block; margin: 12px 0 4px; font-weight: bold; }
  select, input[type="text"], textarea {
    width: 100%; box-sizing: border-box; font-family: var(--vscode-font-family, sans-serif);
  }
  textarea { font-family: var(--vscode-editor-font-family, monospace); min-height: 120px; }
  .header-row { display: flex; gap: 8px; margin-bottom: 4px; }
  .header-row input { flex: 1; }
  #result { margin: 12px 0; padding: 8px; display: none; }
  #result.success {
    background: var(--vscode-inputValidation-infoBackground, #1d3a5a);
    border: 1px solid var(--vscode-inputValidation-infoBorder, #3794ff);
  }
  #result.error {
    background: var(--vscode-inputValidation-errorBackground, #5a1d1d);
    border: 1px solid var(--vscode-inputValidation-errorBorder, #be1100);
  }
  .actions { margin: 16px 0; }
</style>
</head>
<body>
<h2>Produce: ${safeTopic}</h2>

<label for="partition">Partition</label>
<select id="partition"></select>

<label for="key">Key</label>
<input type="text" id="key">

<label for="value">Value</label>
<textarea id="value"></textarea>

<label>Headers</label>
<div id="headers"></div>
<button id="addHeader" type="button">+ Add header</button>

<div class="actions">
  <button id="send" type="button">Send</button>
</div>
<div id="result"></div>

<script>
  const vscode = acquireVsCodeApi();
  const PARTITION_COUNT = ${partitionCount};

  const partitionSelect = document.getElementById('partition');
  const autoOption = document.createElement('option');
  autoOption.value = '';
  autoOption.textContent = 'Auto (by key)';
  partitionSelect.appendChild(autoOption);
  for (let i = 0; i < PARTITION_COUNT; i++) {
    const option = document.createElement('option');
    option.value = String(i);
    option.textContent = String(i);
    partitionSelect.appendChild(option);
  }

  const headersContainer = document.getElementById('headers');
  function addHeaderRow() {
    const row = document.createElement('div');
    row.className = 'header-row';

    const keyInput = document.createElement('input');
    keyInput.type = 'text';
    keyInput.placeholder = 'Header key';
    row.appendChild(keyInput);

    const valueInput = document.createElement('input');
    valueInput.type = 'text';
    valueInput.placeholder = 'Header value';
    row.appendChild(valueInput);

    const removeButton = document.createElement('button');
    removeButton.type = 'button';
    removeButton.textContent = 'Remove';
    removeButton.addEventListener('click', () => row.remove());
    row.appendChild(removeButton);

    headersContainer.appendChild(row);
  }
  document.getElementById('addHeader').addEventListener('click', addHeaderRow);

  const sendButton = document.getElementById('send');
  const resultDiv = document.getElementById('result');

  sendButton.addEventListener('click', () => {
    const headers = [];
    for (const row of headersContainer.children) {
      const inputs = row.querySelectorAll('input');
      headers.push({ key: inputs[0].value, value: inputs[1].value });
    }
    const partitionValue = partitionSelect.value;
    sendButton.disabled = true;
    resultDiv.style.display = 'none';
    vscode.postMessage({
      type: 'send',
      partition: partitionValue === '' ? null : Number(partitionValue),
      key: document.getElementById('key').value,
      value: document.getElementById('value').value,
      headers,
    });
  });

  window.addEventListener('message', (event) => {
    const message = event.data;
    if (message.type !== 'result') return;
    sendButton.disabled = false;
    resultDiv.style.display = 'block';
    if (message.success) {
      resultDiv.className = 'success';
      resultDiv.textContent = 'Sent to partition ' + message.partition + ', offset ' + message.offset;
    } else {
      resultDiv.className = 'error';
      resultDiv.textContent = message.message;
    }
  });
</script>
</body>
</html>`;
}
```

### Controller — `src/webviews/producePanelController.ts` (new)

Singleton panel, same shape as `MessageBrowserPanel`/`TopicMetadataPanel`. `send()` follows the `navigate()`-style race guard (`gen = this.generation`, not incremented — guards against the panel being reused for a different topic while a send is in flight, same accepted pattern documented in the Message Browser design).

```typescript
import * as vscode from 'vscode';
import { ConnectionManager } from '../connection/connectionManager';
import { ConnectionProfile } from '../connection/types';
import { renderProduceHtml, ProduceSendMessage } from './producePanel';
import { renderErrorHtml } from './topicMetadataPanel';

export class ProducePanel {
  private static currentPanel: ProducePanel | undefined;

  private profile: ConnectionProfile | undefined;
  private topicName = '';
  private generation = 0;

  private constructor(
    private readonly panel: vscode.WebviewPanel,
    private readonly connectionManager: ConnectionManager,
  ) {
    this.panel.webview.onDidReceiveMessage((message: ProduceSendMessage) => {
      if (message.type === 'send') void this.send(message);
    });
    this.panel.onDidDispose(() => {
      ProducePanel.currentPanel = undefined;
    });
  }

  static async show(connectionManager: ConnectionManager, profile: ConnectionProfile, topicName: string): Promise<void> {
    let instance = ProducePanel.currentPanel;
    if (instance) {
      instance.panel.reveal();
    } else {
      const panel = vscode.window.createWebviewPanel('kafkaProduce', 'Produce', vscode.ViewColumn.Active, {
        enableScripts: true,
        retainContextWhenHidden: true,
      });
      instance = new ProducePanel(panel, connectionManager);
      ProducePanel.currentPanel = instance;
    }
    instance.panel.title = `Produce: ${topicName}`;
    instance.profile = profile;
    instance.topicName = topicName;
    await instance.renderFull();
  }

  private async renderFull(): Promise<void> {
    const gen = ++this.generation;
    const profile = this.profile!;
    const adminService = this.connectionManager.getAdminService(profile.name);
    if (!adminService) {
      if (gen !== this.generation) return;
      this.panel.webview.html = renderErrorHtml('Not connected — expand the connection in the sidebar first.');
      return;
    }
    try {
      const metadata = await adminService.getTopicMetadata(this.topicName);
      if (gen !== this.generation) return;
      this.panel.webview.html = renderProduceHtml(this.topicName, metadata.partitions.length);
    } catch (err) {
      if (gen !== this.generation) return;
      this.panel.webview.html = renderErrorHtml((err as Error).message);
    }
  }

  private async send(message: ProduceSendMessage): Promise<void> {
    const gen = this.generation;
    const profile = this.profile!;
    const topic = this.topicName;
    try {
      const producerService = await this.connectionManager.getProducerService(profile);
      if (gen !== this.generation) return;
      if (!producerService) {
        void this.panel.webview.postMessage({
          type: 'result',
          success: false,
          message: 'Not connected — expand the connection in the sidebar first.',
        });
        return;
      }
      const result = await producerService.send({
        topic,
        partition: message.partition,
        key: message.key,
        value: message.value,
        headers: message.headers,
      });
      if (gen !== this.generation) return;
      void this.panel.webview.postMessage({ type: 'result', success: true, partition: result.partition, offset: result.offset });
    } catch (err) {
      if (gen !== this.generation) return;
      void this.panel.webview.postMessage({ type: 'result', success: false, message: (err as Error).message });
    }
  }
}
```

### Tree view wiring — `src/treeView/kafkaExplorerProvider.ts`

No change needed — the `'topic'` case already sets `item.contextValue = 'kafkaTopic'` (added in Phase 3 for `kafkaLagMonitor.browseMessages`), and `kafkaLagMonitor.produce` reuses the same context value.

### `src/extension.ts`

- New import: `import { createKafkaProducerClient } from './kafka/kafkaProducerAdapter';`
- New import: `import { ProducePanel } from './webviews/producePanelController';`
- `connectionManager` construction gains a second factory argument:

  ```typescript
  const connectionManager = new ConnectionManager(
    async (profile) => createKafkaAdminClient((await buildKafka(profile)).admin()),
    async (profile) => createKafkaProducerClient((await buildKafka(profile)).producer()),
  );
  ```

- After the existing `kafkaLagMonitor.browseMessages` registration:

  ```typescript
  context.subscriptions.push(
    vscode.commands.registerCommand(
      'kafkaLagMonitor.produce',
      async (profile: ConnectionProfile, topicName: string) => {
        await ProducePanel.show(connectionManager, profile, topicName);
      },
    ),
  );
  ```

### `package.json`

- New entry under `contributes.commands` (placed before `kafkaLagMonitor.browseMessages`, matching the design doc's tree mockup order "Produce, Browse Messages"):

  ```jsonc
  {
    "command": "kafkaLagMonitor.produce",
    "title": "Kafka: Produce Message"
  }
  ```

- New entry under `contributes.menus["view/item/context"]` (same position, before the `browseMessages` entry):

  ```jsonc
  {
    "command": "kafkaLagMonitor.produce",
    "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaTopic"
  }
  ```

### `README.md`

- "Status" section: add a sentence after the Message Browser description: right-clicking a topic and choosing **Kafka: Produce Message** opens a Produce webview with Partition/Key/Value/Headers fields and a Send button; on success the result banner shows the partition and offset of the produced message, on failure it shows the kafkajs error verbatim. Remove the now-stale "A Produce webview is planned in a follow-up phase" sentence — Phases 1–4 of the roadmap are complete.
- "Manual integration test" section: after the Message Browser verification step, add a step: right-click `orders.events`, choose **Kafka: Produce Message**, send a message with a key/value and one header, and verify the result banner shows `Sent to partition <p>, offset <o>`; then use **Kafka: Browse Messages** to confirm the new message appears at that offset.

## Error Handling

- Not connected (no `AdminService` for the profile) on `show()`: `renderErrorHtml('Not connected — expand the connection in the sidebar first.')`, same wording/pattern as Topic Metadata, the Lag Dashboard, and the Message Browser.
- `getTopicMetadata` failure on `show()` (e.g., topic deleted, SASL credentials missing from `buildKafka`): `renderErrorHtml(err.message)` — full error page, same as Topic Metadata.
- `getProducerService` returning `undefined` during `send()` (connection dropped after the panel was opened): inline `{ type: 'result', success: false, message: 'Not connected — expand the connection in the sidebar first.' }` banner — the form is left intact (per the "keep values" requirement), unlike the full-page error replacement used on `show()`.
- Producer creation/connect failure or `producer.send()` failure (e.g., partition out of range, topic deleted mid-session, ACL denied): `{ type: 'result', success: false, message: (err as Error).message }` — the kafkajs error message verbatim, per the master spec's "Produce failures surface the kafkajs error message directly in the result banner."
- The producer's underlying `Kafka` instance is built via the existing `buildKafka()` helper, so kafkajs's internal logs already flow to the "Kafka Lag Monitor" output channel — no additional logging wiring needed.
- **XSS**: the only dynamic value rendered server-side is the topic name, escaped via `escapeHtml` for both `<title>` and the `<h2>`. All other content (partition options, header rows, result banner text) is created via `document.createElement`/`textContent` in the client script — never `innerHTML`.

## Testing Strategy

Unit-tested (pure logic, `node:test`):

- `src/test/producerService.test.ts` (new), using a fake `KafkaProducerClient` (inline `createFakeProducerClient` helper, same style as `createFakeAdminClient` in `connectionManager.test.ts`):
  - header rows with an empty key (`''`) are dropped; rows with a non-empty key are passed through as `Record<string, string>` entries.
  - an empty `key` string (`''`) is converted to `null`; a non-empty key is passed through unchanged.
  - `partition: null` is converted to `undefined`; a numeric partition is passed through unchanged.
  - the `ProducerSendResult` returned by the client (`{ partition, offset }`) is passed through unchanged.
- `src/test/producePanel.test.ts` (new):
  - `renderProduceHtml` output contains the escaped topic name (in `<title>` and `<h2>`), the `#partition`/`#key`/`#value`/`#headers`/`#addHeader`/`#send`/`#result` element ids, and the `PARTITION_COUNT` constant.
  - the partition `<select>` is populated with an `Auto (by key)` option (value `""`) followed by `0..partitionCount-1` — verified by checking the embedded script for the loop bound and option creation, consistent with how `messageBrowserPanel.test.ts` verifies `renderMessageBrowserHtml`'s embedded script content.
  - clicking `#send` posts a `{ type: 'send', partition, key, value, headers }` message (verified via the script source, same approach as the Message Browser's nav-button tests).
  - a topic name containing `</script>`-like characters is escaped (via `escapeHtml`), matching the XSS-safety check style used in `topicMetadataPanel.test.ts`.
- `src/test/connectionManager.test.ts` (extend):
  - All existing `new ConnectionManager(async () => client)` calls gain a second argument, e.g. `async () => createFakeProducerClient()`, using a new inline `createFakeProducerClient` helper (mirrors `createFakeAdminClient`).
  - New tests:
    - `getProducerService returns undefined when not connected`.
    - `getProducerService creates and connects a producer client lazily on first call, and reuses it on subsequent calls` (assert the factory's create-count is `1` after two `getProducerService` calls).
    - `disconnect disposes the cached producer client` (assert the fake's `disconnect` was called, and a subsequent `getProducerService` after `connect()` again creates a fresh producer — create-count `2`).
    - `reconnect disposes the cached producer client` (same disposal assertion as `disconnect`, but via `reconnect()`).

Compile-only (vscode glue, no unit tests — matches `kafkaAdminAdapter.ts`/`kafkaConsumerAdapter.ts` and `topicMetadataPanelController.ts`/`messageBrowserPanelController.ts` precedent):

- `src/kafka/kafkaProducerAdapter.ts`, `src/webviews/producePanelController.ts`, the `connectionManager` construction change and `kafkaLagMonitor.produce` registration in `extension.ts`, and the `package.json` command/menu wiring.

Existing tests unaffected:

- `src/test/treeItems.test.ts` is not modified — the `'topic'` node's `contextValue` is unchanged (already `'kafkaTopic'` since Phase 3).
- `src/test/adminService.test.ts`, `src/test/consumerService.test.ts`, `src/test/messageBrowserPanel.test.ts`, `src/test/lagDashboardPanel.test.ts`, `src/test/lag.test.ts` are not modified — none of their inputs or the code they exercise change in this phase.
