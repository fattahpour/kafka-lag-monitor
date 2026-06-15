# Message Browser Webview — Design

## Overview

Implements roadmap Phase 3 (see `docs/superpowers/specs/2026-06-13-kafka-lag-monitor-design.md`, "Phasing / Roadmap" item 3) on top of the merged Phase 1/2 foundation. Adds a "Message Browser" webview, opened via a right-click "Kafka: Browse Messages" action on a topic node, showing a paginated table of raw messages for a chosen partition with Earliest/Prev/Next/Latest/Refresh navigation. Message fetching uses a short-lived, per-fetch kafkajs consumer with a unique random group id — it never joins or affects any real consumer group.

## Goals

- Right-clicking a topic node in the sidebar and choosing **Kafka: Browse Messages** opens a "Message Browser" webview for that topic (singleton panel, like Topic Metadata and the Lag Dashboard — browsing a different topic retitles/re-renders the same panel).
- On open, the panel defaults to **partition 0** and shows the most recent messages (up to `PAGE_SIZE = 50`) — i.e. the window `[max(highWatermark - 50, lowWatermark), highWatermark]`.
- Table columns: **Offset | Timestamp | Key | Value | Headers**.
  - Value is JSON-pretty-printed (`JSON.stringify(JSON.parse(value), null, 2)`) when it parses as JSON, otherwise shown raw.
  - Values longer than `VALUE_TRUNCATE_LENGTH = 300` characters are truncated with a "Show more" / "Show less" toggle.
- Controls: a partition `<select>` (populated from the topic's partition count) and **Earliest / Prev / Next / Latest / Refresh** buttons.
  - Earliest and Prev are disabled when the current window's `from <= lowWatermark`.
  - Next and Latest are disabled when the current window's `to >= highWatermark`.
- **Refresh** re-fetches the *current* window, clamped to the current low/high watermarks — it does not jump back to "latest".
- Switching partitions via the `<select>` resets to the "latest" window for the new partition.
- All fetches use an ephemeral kafkajs consumer with a unique random `groupId` (`kafka-lag-monitor-browse-<uuid>`), `autoCommit: false`; it never commits offsets and never reuses a real consumer group's id.
- `README.md` "Status" section updated: Message Browser moves from "planned" to implemented, with a short description; the remaining roadmap list drops to just the Produce webview.

## Non-Goals

- A second entry point that opens the Message Browser pre-scoped to a specific partition/consumer-group offset (e.g. clicking a partition row under a group in the tree) and the associated "consumed"/"pending" tag column — deferred to a later phase.
- Viewing multiple partitions in one panel/table.
- A configurable page size (`PAGE_SIZE` is a fixed constant).
- Auto-poll / auto-refresh for the Message Browser — manual Refresh only.
- A "Retry" button on the error page — matches existing precedent (Topic Metadata and Lag Dashboard also have no Retry button on their error pages, only `renderErrorHtml`).
- Produce / message editing — later roadmap phase.
- Any consumer-group admin mutation (offset reset, commit, etc.) — out of scope for the whole extension per the master spec's non-goals.

## Architecture & Components

### `src/kafka/consumerClient.ts` (new)

Pure interface types, no kafkajs or vscode import — mirrors `adminClient.ts`'s role as the thin seam between `kafkajs` and the rest of the extension.

```typescript
export interface RawKafkaMessage {
  offset: string;
  timestamp: string;
  key: string | null;
  value: string | null;
  headers: Record<string, string>;
}

export interface KafkaConsumerClient {
  fetchMessages(args: {
    topic: string;
    partition: number;
    fromOffset: number;
    toOffset: number;
  }): Promise<RawKafkaMessage[]>;
}
```

`fetchMessages` returns messages with `offset >= fromOffset && offset < toOffset`, sorted ascending by offset. An empty result (`fromOffset >= toOffset`, or no messages in range) is valid and returns `[]`.

### `src/kafka/kafkaConsumerAdapter.ts` (new, untested glue)

kafkajs-specific adapter implementing `KafkaConsumerClient`, mirroring `kafkaAdminAdapter.ts`. Not unit tested — same precedent as `kafkaAdminAdapter.ts` (thin wrapper around a third-party client, exercised by the manual integration test).

```typescript
import { Kafka } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import { KafkaConsumerClient, RawKafkaMessage } from './consumerClient';

const FETCH_TIMEOUT_MS = 15000;

function mapHeaders(
  headers?: Record<string, Buffer | string | (Buffer | string)[] | undefined>,
): Record<string, string> {
  const result: Record<string, string> = {};
  if (!headers) return result;
  for (const [key, value] of Object.entries(headers)) {
    if (value === undefined) continue;
    const values = Array.isArray(value) ? value : [value];
    result[key] = values.map((v) => v.toString()).join(', ');
  }
  return result;
}

export function createKafkaConsumerClient(kafka: Kafka): KafkaConsumerClient {
  return {
    fetchMessages: async ({ topic, partition, fromOffset, toOffset }) => {
      if (fromOffset >= toOffset) return [];

      const consumer = kafka.consumer({ groupId: `kafka-lag-monitor-browse-${randomUUID()}` });
      const messages: RawKafkaMessage[] = [];

      try {
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: false });

        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(() => {
            reject(new Error(`Timed out waiting for messages from "${topic}" partition ${partition}`));
          }, FETCH_TIMEOUT_MS);

          const finish = () => {
            clearTimeout(timer);
            resolve();
          };

          consumer.on(consumer.events.GROUP_JOIN, () => {
            consumer.seek({ topic, partition, offset: String(fromOffset) });
          });

          consumer.on(consumer.events.CRASH, ({ payload }) => {
            clearTimeout(timer);
            reject(payload.error);
          });

          consumer
            .run({
              autoCommit: false,
              eachBatch: async ({ batch, heartbeat }) => {
                if (batch.partition === partition) {
                  for (const message of batch.messages) {
                    const offset = Number(message.offset);
                    if (offset >= fromOffset && offset < toOffset) {
                      messages.push({
                        offset: message.offset,
                        timestamp: message.timestamp,
                        key: message.key ? message.key.toString('utf8') : null,
                        value: message.value ? message.value.toString('utf8') : null,
                        headers: mapHeaders(message.headers),
                      });
                    }
                  }
                  if (!batch.isEmpty() && Number(batch.lastOffset()) >= toOffset - 1) {
                    finish();
                  }
                }
                await heartbeat();
              },
            })
            .catch(reject);
        });
      } finally {
        await consumer.stop().catch(() => undefined);
        await consumer.disconnect().catch(() => undefined);
      }

      return messages.sort((a, b) => Number(a.offset) - Number(b.offset));
    },
  };
}
```

### `src/kafka/adminService.ts` additions

New type and method, alongside the existing `getGroupLag`:

```typescript
export interface PartitionOffsets {
  partition: number;
  low: number;
  high: number;
}
```

```typescript
async getTopicOffsets(topic: string): Promise<PartitionOffsets[]> {
  const offsets = await this.admin.fetchTopicOffsets(topic);
  return offsets.map((o) => ({ partition: o.partition, low: Number(o.low), high: Number(o.high) }));
}
```

`fetchTopicOffsets` is already part of `KafkaAdminClient` (used by `getGroupLag`) and already returns `low`/`high` per partition — no changes to `adminClient.ts` or `kafkaAdminAdapter.ts` are needed.

### `src/kafka/consumerService.ts` (new, unit-tested)

Pure logic (no vscode/kafkajs import). Depends on `AdminService` (for watermarks) and a `KafkaConsumerClient` (for message fetching) — both injected, so it's testable with the same fake-client pattern as `adminService.test.ts`.

```typescript
import { AdminService } from './adminService';
import { KafkaConsumerClient, RawKafkaMessage } from './consumerClient';

export const PAGE_SIZE = 50;

export interface MessageWindow {
  from: number;
  to: number;
}

export type NavAction = 'latest' | 'earliest' | 'prev' | 'next' | 'refresh';

export interface MessageView {
  offset: number;
  timestamp: string;
  key: string | null;
  value: string | null;
  headers: Record<string, string>;
}

export interface MessagePage {
  partition: number;
  lowWatermark: number;
  highWatermark: number;
  window: MessageWindow;
  messages: MessageView[];
}

export function computeWindow(
  action: NavAction,
  low: number,
  high: number,
  current?: MessageWindow,
): MessageWindow {
  switch (action) {
    case 'latest':
      return { from: Math.max(high - PAGE_SIZE, low), to: high };
    case 'earliest':
      return { from: low, to: Math.min(low + PAGE_SIZE, high) };
    case 'prev': {
      if (!current) return computeWindow('latest', low, high);
      const to = Math.max(current.from, low);
      const from = Math.max(to - PAGE_SIZE, low);
      return { from, to };
    }
    case 'next': {
      if (!current) return computeWindow('latest', low, high);
      const from = Math.min(current.to, high);
      const to = Math.min(from + PAGE_SIZE, high);
      return { from, to };
    }
    case 'refresh': {
      if (!current) return computeWindow('latest', low, high);
      const from = Math.min(Math.max(current.from, low), high);
      const to = Math.min(Math.max(current.to, from), high);
      return { from, to };
    }
  }
}

function toMessageView(raw: RawKafkaMessage): MessageView {
  return {
    offset: Number(raw.offset),
    timestamp: raw.timestamp,
    key: raw.key,
    value: raw.value,
    headers: raw.headers,
  };
}

export class ConsumerService {
  constructor(
    private readonly consumerClient: KafkaConsumerClient,
    private readonly adminService: AdminService,
  ) {}

  async fetchPage(
    topic: string,
    partition: number,
    action: NavAction,
    currentWindow?: MessageWindow,
  ): Promise<MessagePage> {
    const offsets = await this.adminService.getTopicOffsets(topic);
    const partitionOffsets = offsets.find((o) => o.partition === partition);
    if (!partitionOffsets) {
      throw new Error(`Partition ${partition} not found for topic "${topic}"`);
    }
    const { low, high } = partitionOffsets;
    const window = computeWindow(action, low, high, currentWindow);
    const raw = await this.consumerClient.fetchMessages({
      topic,
      partition,
      fromOffset: window.from,
      toOffset: window.to,
    });
    return {
      partition,
      lowWatermark: low,
      highWatermark: high,
      window,
      messages: raw.map(toMessageView),
    };
  }
}
```

**`computeWindow` behavior at boundaries:**

- `latest`/`earliest` always clamp to `[low, high]`, so an empty partition (`low === high`) yields `{ from: low, to: low }` (zero messages).
- `prev`/`next` at the low/high watermark return an empty window (`from === to`) rather than throwing — the UI disables these buttons at the boundary, so this is a defensive fallback, not a normal path.
- `refresh` clamps the existing window into `[low, high]` (handles retention having moved `low` up past the old `from`, or `high` having moved). If the old window falls entirely outside `[low, high]`, the result collapses to an empty window at the nearer bound; the user can then click Earliest/Latest to recover.

### Message Browser data + render — `src/webviews/messageBrowserPanel.ts` (new)

Pure, unit-tested (no vscode import), mirrors `lagDashboardPanel.ts`. Imports `MessagePage` and `MessageWindow` from `../kafka/consumerService`, and reuses `escapeHtml` from `./topicMetadataPanel` for the `<title>` tag (import, no new shared module — same precedent as the Lag Dashboard). `VALUE_TRUNCATE_LENGTH` is defined in this module, not `consumerService.ts` — it's a presentation concern, not a windowing concern. (`renderErrorHtml`, also from `./topicMetadataPanel`, is used by the controller, not this module.)

**Types and constants:**

```typescript
import { MessagePage, MessageWindow } from '../kafka/consumerService';
import { escapeHtml } from './topicMetadataPanel';

export const VALUE_TRUNCATE_LENGTH = 300;

export interface MessageHeaderView {
  key: string;
  value: string;
}

export interface MessageRowView {
  offset: number;
  timestamp: string;
  key: string | null;
  value: string | null;
  headers: MessageHeaderView[];
}

export interface MessageBrowserData {
  topic: string;
  partition: number;
  partitionCount: number;
  lowWatermark: number;
  highWatermark: number;
  window: MessageWindow;
  messages: MessageRowView[];
}
```

**`toMessageBrowserData(topic: string, partitionCount: number, page: MessagePage): MessageBrowserData`**

- `value`: if the raw value parses as JSON, replace it with `JSON.stringify(JSON.parse(value), null, 2)`; otherwise leave it as the raw string. `null` stays `null`.
- `headers`: `Object.entries(message.headers).map(([key, value]) => ({ key, value }))` — preserves insertion order.
- All other fields (`offset`, `timestamp`, `key`, `lowWatermark`, `highWatermark`, `window`, `partition`, `partitionCount`) pass through unchanged.

```typescript
function formatValue(value: string | null): string | null {
  if (value === null) return null;
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch {
    return value;
  }
}

export function toMessageBrowserData(topic: string, partitionCount: number, page: MessagePage): MessageBrowserData {
  return {
    topic,
    partition: page.partition,
    partitionCount,
    lowWatermark: page.lowWatermark,
    highWatermark: page.highWatermark,
    window: page.window,
    messages: page.messages.map((m) => ({
      offset: m.offset,
      timestamp: m.timestamp,
      key: m.key,
      value: formatValue(m.value),
      headers: Object.entries(m.headers).map(([key, value]) => ({ key, value })),
    })),
  };
}
```

**`renderMessageBrowserHtml(topic: string, data: MessageBrowserData): string`**

Full HTML document:

```html
<h2 id="title"></h2>
<div class="controls">
  <label>Partition: <select id="partition"></select></label>
  <button id="earliest">Earliest</button>
  <button id="prev">Prev</button>
  <button id="next">Next</button>
  <button id="latest">Latest</button>
  <button id="refresh">Refresh</button>
</div>
<div id="banner" style="display:none"></div>
<p id="windowInfo"></p>
<table>
<thead><tr><th>Offset</th><th>Timestamp</th><th>Key</th><th>Value</th><th>Headers</th></tr></thead>
<tbody id="rows"></tbody>
</table>
```

Plus an inline `<script>`:

- `const initialData = {JSON-serialized MessageBrowserData, with "<" escaped to "<"};`
- `const VALUE_TRUNCATE_LENGTH = 300;`
- `const vscode = acquireVsCodeApi();`
- `function render(data) { ... }`, using `document.createElement`/`textContent`/`createTextNode` only (no `innerHTML`) — this is **load-bearing for security**, since message keys/values/headers are arbitrary, untrusted Kafka payload data that could contain `<script>`-like content:
  - `#title` ← `'Messages: ' + data.topic + ' (partition ' + data.partition + ')'`.
  - `#partition` `<select>` rebuilt with one `<option>` per `0..data.partitionCount-1`, the one matching `data.partition` marked `selected`.
  - `#windowInfo` ← `'Showing offsets ' + data.window.from + '-' + data.window.to + ' of ' + data.lowWatermark + '-' + data.highWatermark'`.
  - `#earliest` and `#prev` `.disabled = data.window.from <= data.lowWatermark`.
  - `#next` and `#latest` `.disabled = data.window.to >= data.highWatermark`.
  - `#rows` cleared and rebuilt:
    - `data.messages.length === 0` → single row, single `<td colspan="5">No messages in this range.</td>`.
    - Otherwise, one `<tr>` per message: Offset (`String(offset)`), Timestamp (`new Date(Number(timestamp)).toLocaleString()`), Key (`key === null ? '(null)' : key`), Value (via `appendTruncatable`, `value === null ? '(null)' : value`), Headers (`headers.map(h => h.key + '=' + h.value).join(', ')`).
- `function appendTruncatable(cell, text) { ... }`:
  - If `text.length <= VALUE_TRUNCATE_LENGTH`, set `cell.textContent = text` and return.
  - Otherwise append a text node with `text.slice(0, VALUE_TRUNCATE_LENGTH) + '... '` plus a "Show more" `<button class="show-more">`. Clicking toggles between the truncated text + "Show more" and the full text + "Show less", rebuilding the cell's children each time.
- `render(initialData)` called on load.
- `window.addEventListener('message', (event) => { ... })`:
  - `{ type: 'update', data }` → hide `#banner`, `render(data)`.
  - `{ type: 'error', message }` → show `#banner` with `message` (plain text, no reconnect-hint logic — Message Browser has no polling).
- `#partition` `change` → `vscode.postMessage({ type: 'setPartition', partition: Number(select.value) })`.
- `#earliest`/`#prev`/`#next`/`#latest`/`#refresh` `click` → `vscode.postMessage({ type: 'nav', action: 'earliest' | 'prev' | 'next' | 'latest' | 'refresh' })`.

CSS additions (mirroring the Lag Dashboard's banner/card style with VS Code CSS variables):

```css
table { border-collapse: collapse; width: 100%; margin: 12px 0; }
th, td { border: 1px solid var(--vscode-panel-border, #ccc); padding: 4px 8px; text-align: left; vertical-align: top; }
th { background: var(--vscode-editor-lineHighlightBackground, #eee); }
td.value, td.headers { font-family: var(--vscode-editor-font-family, monospace); white-space: pre-wrap; word-break: break-all; }
.controls { display: flex; align-items: center; gap: 8px; margin: 12px 0; flex-wrap: wrap; }
.show-more { background: none; border: none; color: var(--vscode-textLink-foreground, #3794ff); cursor: pointer; padding: 0; text-decoration: underline; }
#banner { background: var(--vscode-inputValidation-errorBackground, #5a1d1d); border: 1px solid var(--vscode-inputValidation-errorBorder, #be1100); padding: 8px; margin: 12px 0; }
```

The Value and Headers cells get `class="value"` / `class="headers"` for the monospace/wrap styling.

### Controller — `src/webviews/messageBrowserPanelController.ts` (new)

vscode glue (compile-only, no unit tests — matches `topicMetadataPanelController.ts`/`lagDashboardPanelController.ts` precedent). Singleton panel.

Unlike the Lag Dashboard, this controller does **not** route consumer-client creation through `ConnectionManager` — `ConnectionManager` only caches `KafkaAdminClient`s (admin connections are long-lived; ephemeral browse consumers are not). Instead, `extension.ts` passes in a `createConsumerClient` factory function (built from the same per-profile Kafka config used for the admin client — see the `extension.ts` section below). This keeps `ConnectionManager` and its existing test suite (`connectionManager.test.ts`) untouched.

```typescript
import * as vscode from 'vscode';
import { ConnectionManager } from '../connection/connectionManager';
import { ConnectionProfile } from '../connection/types';
import { KafkaConsumerClient } from '../kafka/consumerClient';
import { ConsumerService, MessageWindow, NavAction } from '../kafka/consumerService';
import { renderMessageBrowserHtml, toMessageBrowserData } from './messageBrowserPanel';
import { renderErrorHtml } from './topicMetadataPanel';

export type ConsumerClientFactory = (profile: ConnectionProfile) => Promise<KafkaConsumerClient>;

export class MessageBrowserPanel {
  private static currentPanel: MessageBrowserPanel | undefined;

  private profile: ConnectionProfile | undefined;
  private topicName = '';
  private partition = 0;
  private partitionCount = 0;
  private currentWindow: MessageWindow | undefined;
  private generation = 0;

  private constructor(
    private readonly panel: vscode.WebviewPanel,
    private readonly connectionManager: ConnectionManager,
    private readonly createConsumerClient: ConsumerClientFactory,
  ) {
    this.panel.webview.onDidReceiveMessage((message: { type: string; action?: NavAction; partition?: number }) => {
      if (message.type === 'nav' && message.action) void this.navigate(message.action);
      else if (message.type === 'setPartition' && message.partition !== undefined) void this.changePartition(message.partition);
    });
    this.panel.onDidDispose(() => {
      MessageBrowserPanel.currentPanel = undefined;
    });
  }

  static async show(
    connectionManager: ConnectionManager,
    createConsumerClient: ConsumerClientFactory,
    profile: ConnectionProfile,
    topicName: string,
  ): Promise<void> {
    let instance = MessageBrowserPanel.currentPanel;
    if (instance) {
      instance.panel.reveal();
    } else {
      const panel = vscode.window.createWebviewPanel('kafkaMessageBrowser', 'Messages', vscode.ViewColumn.Active, {
        enableScripts: true,
        retainContextWhenHidden: true,
      });
      instance = new MessageBrowserPanel(panel, connectionManager, createConsumerClient);
      MessageBrowserPanel.currentPanel = instance;
    }
    instance.panel.title = `Messages: ${topicName}`;
    instance.profile = profile;
    instance.topicName = topicName;
    instance.partition = 0;
    instance.currentWindow = undefined;
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
      const consumerClient = await this.createConsumerClient(profile);
      const consumerService = new ConsumerService(consumerClient, adminService);
      const page = await consumerService.fetchPage(this.topicName, this.partition, 'latest');
      if (gen !== this.generation) return;
      this.partitionCount = metadata.partitions.length;
      this.currentWindow = page.window;
      const data = toMessageBrowserData(this.topicName, this.partitionCount, page);
      this.panel.webview.html = renderMessageBrowserHtml(this.topicName, data);
    } catch (err) {
      if (gen !== this.generation) return;
      this.panel.webview.html = renderErrorHtml((err as Error).message);
    }
  }

  private async navigate(action: NavAction): Promise<void> {
    const gen = this.generation;
    const profile = this.profile!;
    const adminService = this.connectionManager.getAdminService(profile.name);
    if (!adminService) return;
    try {
      const consumerClient = await this.createConsumerClient(profile);
      const consumerService = new ConsumerService(consumerClient, adminService);
      const page = await consumerService.fetchPage(this.topicName, this.partition, action, this.currentWindow);
      if (gen !== this.generation) return;
      this.currentWindow = page.window;
      const data = toMessageBrowserData(this.topicName, this.partitionCount, page);
      void this.panel.webview.postMessage({ type: 'update', data });
    } catch (err) {
      if (gen !== this.generation) return;
      void this.panel.webview.postMessage({ type: 'error', message: (err as Error).message });
    }
  }

  private async changePartition(partition: number): Promise<void> {
    this.partition = partition;
    this.currentWindow = undefined;
    await this.navigate('latest');
  }
}
```

- `renderFull()` is used only by `show()` (initial open / topic switch) — full `webview.html` re-render, which resets the partition select to `0` and the window to "latest" (matches the `partition = 0` / `currentWindow = undefined` reset in `show()`).
- `navigate()` (Earliest/Prev/Next/Latest/Refresh) and `changePartition()` (the `<select>`) use `postMessage` only, no `webview.html` reassignment — same pattern as the Lag Dashboard's `refresh()`.
- `generation` guards both paths against races (e.g., user browses a different topic while a `navigate()` from the old topic is in flight).
- `retainContextWhenHidden: true` keeps the webview's DOM/JS alive across tab hide/show — no resync needed since there's no polling to restart.
- A fresh `KafkaConsumerClient` is created via `createConsumerClient(profile)` on every `renderFull`/`navigate` call. Each call internally spins up and tears down its own ephemeral consumer (see `kafkaConsumerAdapter.ts`), so there is no persistent consumer connection to manage, and credentials are re-resolved from `SecretStorage` each time (negligible cost, and correctly picks up rotated credentials).

### Tree view wiring — `src/treeView/kafkaExplorerProvider.ts`

`getTreeItem`'s `'topic'` case gains a `contextValue`, used by the new context-menu entry in `package.json`:

```typescript
case 'topic': {
  const view = buildTopicNode(element.topic.name, element.topic.partitionCount);
  const item = new vscode.TreeItem(view.label, vscode.TreeItemCollapsibleState.None);
  item.description = view.description;
  item.contextValue = 'kafkaTopic';
  item.command = {
    command: 'kafkaLagMonitor.showTopicMetadata',
    title: 'Show Topic Metadata',
    arguments: [element.profile, element.topic.name],
  };
  return item;
}
```

Clicking the topic still opens Topic Metadata (unchanged `item.command`); "Kafka: Browse Messages" is reached via the right-click context menu, same coexistence pattern as the connection node's click-to-expand vs. right-click Edit/Remove/Reconnect.

### `src/extension.ts`

- Factor the existing inline `Kafka` construction (currently only used by the admin-client factory) into a shared `buildKafka(profile)` helper, so both the admin client and the new consumer-client factory build their `Kafka` instance the same way (SASL credential lookup, SSL, `clientId`, `logCreator`):

  ```typescript
  async function buildKafka(profile: ConnectionProfile): Promise<Kafka> {
    let sasl: SASLOptions | undefined;
    if (profile.sasl) {
      const username = await getCredential(context.secrets, profile.name, 'username');
      const password = await getCredential(context.secrets, profile.name, 'password');
      if (username === undefined || password === undefined) {
        throw new Error(
          `Missing SASL credentials for connection "${profile.name}". Use the 'Kafka: Add Connection' command to set them.`,
        );
      }
      sasl = buildSasl(profile.sasl.mechanism, username, password);
    }
    return new Kafka({
      clientId: profile.clientId,
      brokers: profile.brokers,
      ssl: profile.ssl,
      sasl,
      logCreator: createKafkaLogCreator((line) => output.appendLine(line)),
    });
  }

  const connectionManager = new ConnectionManager(async (profile) =>
    createKafkaAdminClient((await buildKafka(profile)).admin()),
  );

  const createConsumerClient = async (profile: ConnectionProfile) =>
    createKafkaConsumerClient(await buildKafka(profile));
  ```

- New import: `import { createKafkaConsumerClient } from './kafka/kafkaConsumerAdapter';`
- New import: `import { MessageBrowserPanel } from './webviews/messageBrowserPanelController';`
- After the existing `kafkaLagMonitor.showTopicMetadata` registration:

  ```typescript
  context.subscriptions.push(
    vscode.commands.registerCommand(
      'kafkaLagMonitor.browseMessages',
      async (profile: ConnectionProfile, topicName: string) => {
        await MessageBrowserPanel.show(connectionManager, createConsumerClient, profile, topicName);
      },
    ),
  );
  ```

### `package.json`

- New entry under `contributes.commands`:

  ```jsonc
  {
    "command": "kafkaLagMonitor.browseMessages",
    "title": "Kafka: Browse Messages"
  }
  ```

- New entry under `contributes.menus["view/item/context"]`:

  ```jsonc
  {
    "command": "kafkaLagMonitor.browseMessages",
    "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaTopic"
  }
  ```

### `README.md`

- "Status" section: after the existing sentence about the Lag Dashboard, add a sentence describing the Message Browser (right-click a topic → "Kafka: Browse Messages" opens a table of the topic's most recent messages for a chosen partition, with Earliest/Prev/Next/Latest/Refresh navigation and a partition selector). Change "Message Browser and Produce webviews are planned in follow-up phases" to "A Produce webview is planned in a follow-up phase".
- "Manual integration test" section: after the existing Lag Dashboard verification step, add a step: right-click `orders.events` in the Explorer sidebar, choose **Kafka: Browse Messages**, and verify the panel opens for partition 0 showing the most recent messages with Offset/Timestamp/Key/Value/Headers columns; use the partition selector and the Earliest/Prev/Next/Latest/Refresh buttons to navigate.

## Error Handling

- Not connected (no `AdminService` for the profile) on `show()`: `renderErrorHtml('Not connected — expand the connection in the sidebar first.')`, same wording/pattern as Topic Metadata and the Lag Dashboard.
- `getTopicMetadata` / initial `fetchPage` failure on `show()` (e.g., topic deleted, SASL credentials missing from `buildKafka`, consumer fetch timeout): `renderErrorHtml(err.message)` — full error page, same as Topic Metadata.
- `fetchPage` failure during `navigate()` (Earliest/Prev/Next/Latest/Refresh) or `changePartition()`: `{ type: 'error', message }` banner — the table keeps showing the last successful page; no retry timer (Message Browser has no polling, unlike the Lag Dashboard's `pollError`/reconnect-hint).
- "Partition not found" (`ConsumerService.fetchPage` when `getTopicOffsets` doesn't return the requested partition — e.g. the topic's partition count changed mid-session): surfaces as a normal `{ type: 'error', message }` banner with the message `Partition <n> not found for topic "<topic>"`.
- Manual navigation while not connected (admin service becomes unavailable mid-session, e.g. after a disconnect): `navigate()`/`changePartition()` return early without posting a message — banner state is left as-is, same edge-case handling as the Lag Dashboard's `refresh()`.
- **XSS**: message keys, values, and headers are arbitrary, untrusted Kafka payload bytes (decoded as UTF-8). `renderMessageBrowserHtml`'s client script renders every field via `document.createElement`/`textContent`/`createTextNode` — never `innerHTML` — so payload content can never be interpreted as HTML/script. The initial `MessageBrowserData` embedded in the page's `<script>` tag is serialized with `JSON.stringify(data).replace(/</g, '\\u003c')`, preventing a message value containing `</script>` from breaking out of the script context (same fix already applied in the Lag Dashboard for `initialData`).

## Testing Strategy

Unit-tested (pure logic, `node:test`):

- `src/test/consumerService.test.ts` (new):
  - `computeWindow`: `latest` and `earliest` for a normal partition, an empty partition (`low === high`), and a partition with fewer than `PAGE_SIZE` messages (`high - low < PAGE_SIZE`); `prev`/`next` from a mid-range window, and at the low/high watermark boundary (returns an empty window); `refresh` clamping a window whose `from`/`to` fall outside the current `[low, high]` (retention moved `low` up past the old `from`); `prev`/`next`/`refresh` with no `current` window fall back to `latest`.
  - `ConsumerService.fetchPage`: using a fake `AdminService` (via `createFakeAdminClient`-style `KafkaAdminClient` fake with `fetchTopicOffsets`) and a fake `KafkaConsumerClient` (`fetchMessages` returning canned `RawKafkaMessage[]`):
    - happy path maps `RawKafkaMessage[]` to `MessageView[]` (numeric `offset`, passthrough `timestamp`/`key`/`value`/`headers`) and returns the correct `lowWatermark`/`highWatermark`/`window`.
    - throws `Partition <n> not found for topic "<topic>"` when `getTopicOffsets` doesn't include the requested partition.
    - passes the computed `window.from`/`window.to` through to `fetchMessages` as `fromOffset`/`toOffset`.
- `src/test/messageBrowserPanel.test.ts` (new):
  - `toMessageBrowserData`: a JSON-parseable value is pretty-printed with `JSON.stringify(..., null, 2)`; a non-JSON value passes through raw; a `null` value/key stays `null`; `headers` (a `Record<string, string>`) becomes an ordered `{key, value}[]`; `partition`/`partitionCount`/`lowWatermark`/`highWatermark`/`window` pass through unchanged.
  - `renderMessageBrowserHtml`: output contains the topic name (in `<title>` and the embedded `initialData`), a `<script>` tag with the serialized `initialData`, the `VALUE_TRUNCATE_LENGTH` constant, and the `#title`/`#partition`/`#earliest`/`#prev`/`#next`/`#latest`/`#refresh`/`#banner`/`#windowInfo`/`#rows` element ids; verify `initialData`'s `<` characters are escaped to `<` (e.g. when a message value contains `<script>`).

Compile-only (vscode glue, no unit tests — matches `topicMetadataPanelController.ts`/`lagDashboardPanelController.ts` precedent):

- `src/kafka/kafkaConsumerAdapter.ts`, `src/webviews/messageBrowserPanelController.ts`, the `buildKafka` refactor and `kafkaLagMonitor.browseMessages` registration in `extension.ts`, and the `package.json` command/menu wiring.

Existing tests unaffected:

- `src/test/connectionManager.test.ts` is not modified — `ConnectionManager`'s constructor and public API are unchanged; the consumer-client factory is wired entirely in `extension.ts` and passed directly to `MessageBrowserPanel.show()`.
- `src/test/adminService.test.ts` gains no new tests for `getTopicOffsets` beyond what's exercised indirectly via `consumerService.test.ts`'s fakes — but since `getTopicOffsets` is a small, independently-meaningful mapping (mirrors `fetchTopicOffsets`'s `low`/`high` strings to numbers), add one direct test there too: `getTopicOffsets maps partition/low/high to numbers`, following the existing fake-`fetchTopicOffsets` pattern from the `getGroupLag` tests.
- `src/test/treeItems.test.ts` and `src/test/lag.test.ts` are not modified (the `'topic'` node's new `contextValue` is additive; existing assertions on label/description/collapsible state still hold).
