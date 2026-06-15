# Message Browser Webview Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement roadmap Phase 3 — a "Message Browser" webview, opened via a right-click "Kafka: Browse Messages" action on a topic node, showing a paginated table of raw messages for a chosen partition with Earliest/Prev/Next/Latest/Refresh navigation, backed by an ephemeral per-fetch kafkajs consumer.

**Architecture:** A new pure `src/kafka/consumerClient.ts` defines the `KafkaConsumerClient`/`RawKafkaMessage` interface (mirrors `adminClient.ts`). `src/kafka/kafkaConsumerAdapter.ts` implements it with a short-lived kafkajs consumer (unique random `groupId`, seek-then-eachBatch, never commits). `AdminService` gains `getTopicOffsets`. A new pure `src/kafka/consumerService.ts` holds `computeWindow` (Earliest/Prev/Next/Latest/Refresh windowing math) and `ConsumerService.fetchPage` (combines watermarks + fetched messages into a `MessagePage`). A new pure `src/webviews/messageBrowserPanel.ts` (mirrors `lagDashboardPanel.ts`) holds `toMessageBrowserData` and `renderMessageBrowserHtml`. A new `src/webviews/messageBrowserPanelController.ts` holds the singleton `MessageBrowserPanel` vscode glue class. `kafkaExplorerProvider.ts`'s `'topic'` node gains `contextValue: 'kafkaTopic'` for the new context-menu entry. `extension.ts` factors a `buildKafka(profile)` helper and registers `kafkaLagMonitor.browseMessages`.

**Tech Stack:** TypeScript, vscode Extension API (WebviewPanel, `postMessage`), kafkajs (ephemeral `Consumer`), node:test.

**Reference spec:** `docs/superpowers/specs/2026-06-15-message-browser-design.md`

---

## Task 1: `AdminService.getTopicOffsets`

**Files:**
- Modify: `src/kafka/adminService.ts`
- Modify: `src/test/adminService.test.ts`

- [ ] **Step 1: Write the failing test**

Append to the end of `src/test/adminService.test.ts`:

```typescript

test('getTopicOffsets maps partition/low/high to numbers', async () => {
  const admin = createFakeAdminClient({
    fetchTopicOffsets: async (topic) => {
      assert.equal(topic, 'orders.events');
      return [
        { partition: 0, offset: '600', high: '600', low: '0' },
        { partition: 1, offset: '220', high: '220', low: '20' },
      ];
    },
  });

  const result = await new AdminService(admin).getTopicOffsets('orders.events');

  assert.deepEqual(result, [
    { partition: 0, low: 0, high: 600 },
    { partition: 1, low: 20, high: 220 },
  ]);
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npm test`
Expected: FAIL — TypeScript compile error, e.g. `Property 'getTopicOffsets' does not exist on type 'AdminService'`.

- [ ] **Step 3: Add `PartitionOffsets` and `getTopicOffsets` to `src/kafka/adminService.ts`**

Change the `ConsumerGroupSummary` interface block from:

```typescript
export interface ConsumerGroupSummary {
  groupId: string;
}

const TOPIC_RESOURCE_TYPE = 2; // kafkajs ResourceTypes.TOPIC
```

to:

```typescript
export interface ConsumerGroupSummary {
  groupId: string;
}

export interface PartitionOffsets {
  partition: number;
  low: number;
  high: number;
}

const TOPIC_RESOURCE_TYPE = 2; // kafkajs ResourceTypes.TOPIC
```

Then add a new method at the end of the `AdminService` class, after `getGroupLag`:

```typescript
  async getTopicOffsets(topic: string): Promise<PartitionOffsets[]> {
    const offsets = await this.admin.fetchTopicOffsets(topic);
    return offsets.map((o) => ({ partition: o.partition, low: Number(o.low), high: Number(o.high) }));
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 69`, `# pass 69`, `# fail 0` (68 existing + 1 new).

- [ ] **Step 5: Commit**

```bash
git add src/kafka/adminService.ts src/test/adminService.test.ts
git commit -m "feat: add AdminService.getTopicOffsets for the Message Browser"
```

---

## Task 2: `KafkaConsumerClient` interface and kafkajs adapter

**Files:**
- Create: `src/kafka/consumerClient.ts`
- Create: `src/kafka/kafkaConsumerAdapter.ts`

No new unit tests — `kafkaConsumerAdapter.ts` is a thin wrapper around kafkajs, matching the established compile-only treatment of `kafkaAdminAdapter.ts` (exercised by the manual integration test, not unit tests). `consumerClient.ts` is a pure interface with no logic to test.

- [ ] **Step 1: Create `src/kafka/consumerClient.ts`**

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

- [ ] **Step 2: Create `src/kafka/kafkaConsumerAdapter.ts`**

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

- [ ] **Step 3: Run the tests to verify nothing broke**

Run: `npm run compile && npm test 2>&1 | tail -8`
Expected: compile succeeds; `# tests 69`, `# pass 69`, `# fail 0` (unchanged from Task 1 — this task adds no new tests).

- [ ] **Step 4: Commit**

```bash
git add src/kafka/consumerClient.ts src/kafka/kafkaConsumerAdapter.ts
git commit -m "feat: add KafkaConsumerClient interface and ephemeral kafkajs adapter"
```

---

## Task 3: `computeWindow`

**Files:**
- Create: `src/kafka/consumerService.ts`
- Test: `src/test/consumerService.test.ts`

- [ ] **Step 1: Write the failing tests**

Create `src/test/consumerService.test.ts`:

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { computeWindow } from '../kafka/consumerService';

test('computeWindow latest and earliest for a normal partition', () => {
  assert.deepEqual(computeWindow('latest', 0, 200), { from: 150, to: 200 });
  assert.deepEqual(computeWindow('earliest', 0, 200), { from: 0, to: 50 });
});

test('computeWindow latest and earliest for an empty partition', () => {
  assert.deepEqual(computeWindow('latest', 100, 100), { from: 100, to: 100 });
  assert.deepEqual(computeWindow('earliest', 100, 100), { from: 100, to: 100 });
});

test('computeWindow latest and earliest for a partition with fewer than PAGE_SIZE messages', () => {
  assert.deepEqual(computeWindow('latest', 0, 30), { from: 0, to: 30 });
  assert.deepEqual(computeWindow('earliest', 0, 30), { from: 0, to: 30 });
});

test('computeWindow prev and next from a mid-range window', () => {
  assert.deepEqual(computeWindow('prev', 0, 200, { from: 150, to: 200 }), { from: 100, to: 150 });
  assert.deepEqual(computeWindow('next', 0, 200, { from: 100, to: 150 }), { from: 150, to: 200 });
});

test('computeWindow prev and next at the low/high watermark boundary return an empty window', () => {
  assert.deepEqual(computeWindow('prev', 0, 200, { from: 0, to: 50 }), { from: 0, to: 0 });
  assert.deepEqual(computeWindow('next', 0, 200, { from: 150, to: 200 }), { from: 200, to: 200 });
});

test('computeWindow refresh clamps the current window into the low/high range', () => {
  assert.deepEqual(computeWindow('refresh', 100, 200, { from: 0, to: 50 }), { from: 100, to: 100 });
  assert.deepEqual(computeWindow('refresh', 0, 120, { from: 100, to: 200 }), { from: 100, to: 120 });
});

test('computeWindow prev, next, and refresh fall back to latest when there is no current window', () => {
  assert.deepEqual(computeWindow('prev', 0, 200), { from: 150, to: 200 });
  assert.deepEqual(computeWindow('next', 0, 200), { from: 150, to: 200 });
  assert.deepEqual(computeWindow('refresh', 0, 200), { from: 150, to: 200 });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test`
Expected: FAIL — TypeScript compile error `Cannot find module '../kafka/consumerService'`.

- [ ] **Step 3: Create `src/kafka/consumerService.ts`**

```typescript
export const PAGE_SIZE = 50;

export interface MessageWindow {
  from: number;
  to: number;
}

export type NavAction = 'latest' | 'earliest' | 'prev' | 'next' | 'refresh';

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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 76`, `# pass 76`, `# fail 0` (69 from Task 1, plus the 7 new tests above).

- [ ] **Step 5: Commit**

```bash
git add src/kafka/consumerService.ts src/test/consumerService.test.ts
git commit -m "feat: add computeWindow for Message Browser pagination"
```

---

## Task 4: `ConsumerService.fetchPage`

**Files:**
- Modify: `src/kafka/consumerService.ts`
- Modify: `src/test/consumerService.test.ts`

- [ ] **Step 1: Write the failing tests**

In `src/test/consumerService.test.ts`, change the import block from:

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { computeWindow } from '../kafka/consumerService';
```

to:

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { AdminService } from '../kafka/adminService';
import { KafkaAdminClient } from '../kafka/adminClient';
import { KafkaConsumerClient } from '../kafka/consumerClient';
import { computeWindow, ConsumerService } from '../kafka/consumerService';

function createFakeAdminClient(overrides: Partial<KafkaAdminClient>): KafkaAdminClient {
  const notImplemented = () => {
    throw new Error('not implemented in fake');
  };
  return {
    connect: notImplemented,
    disconnect: notImplemented,
    listTopics: notImplemented,
    fetchTopicMetadata: notImplemented,
    describeConfigs: notImplemented,
    listGroups: notImplemented,
    fetchOffsets: notImplemented,
    fetchTopicOffsets: notImplemented,
    ...overrides,
  } as KafkaAdminClient;
}

function createFakeConsumerClient(overrides: Partial<KafkaConsumerClient>): KafkaConsumerClient {
  return {
    fetchMessages: async () => [],
    ...overrides,
  };
}
```

Then append these tests at the end of the file:

```typescript

test('fetchPage maps RawKafkaMessage[] to MessageView[] and returns watermarks and window', async () => {
  const admin = createFakeAdminClient({
    fetchTopicOffsets: async () => [{ partition: 0, offset: '200', high: '200', low: '0' }],
  });
  const consumerClient = createFakeConsumerClient({
    fetchMessages: async () => [
      { offset: '150', timestamp: '1700000000000', key: 'k1', value: 'v1', headers: { h: '1' } },
      { offset: '151', timestamp: '1700000000001', key: null, value: null, headers: {} },
    ],
  });

  const service = new ConsumerService(consumerClient, new AdminService(admin));
  const page = await service.fetchPage('orders.events', 0, 'latest');

  assert.equal(page.partition, 0);
  assert.equal(page.lowWatermark, 0);
  assert.equal(page.highWatermark, 200);
  assert.deepEqual(page.window, { from: 150, to: 200 });
  assert.deepEqual(page.messages, [
    { offset: 150, timestamp: '1700000000000', key: 'k1', value: 'v1', headers: { h: '1' } },
    { offset: 151, timestamp: '1700000000001', key: null, value: null, headers: {} },
  ]);
});

test('fetchPage throws when the requested partition is not found', async () => {
  const admin = createFakeAdminClient({
    fetchTopicOffsets: async () => [{ partition: 0, offset: '200', high: '200', low: '0' }],
  });
  const consumerClient = createFakeConsumerClient({});

  const service = new ConsumerService(consumerClient, new AdminService(admin));

  await assert.rejects(
    () => service.fetchPage('orders.events', 5, 'latest'),
    /Partition 5 not found for topic "orders\.events"/,
  );
});

test('fetchPage passes the computed window through to fetchMessages as fromOffset/toOffset', async () => {
  const admin = createFakeAdminClient({
    fetchTopicOffsets: async () => [{ partition: 0, offset: '200', high: '200', low: '0' }],
  });
  let receivedArgs: { topic: string; partition: number; fromOffset: number; toOffset: number } | undefined;
  const consumerClient = createFakeConsumerClient({
    fetchMessages: async (args) => {
      receivedArgs = args;
      return [];
    },
  });

  const service = new ConsumerService(consumerClient, new AdminService(admin));
  await service.fetchPage('orders.events', 0, 'earliest');

  assert.deepEqual(receivedArgs, { topic: 'orders.events', partition: 0, fromOffset: 0, toOffset: 50 });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test`
Expected: FAIL — TypeScript compile error, e.g. `Module '"../kafka/consumerService"' has no exported member 'ConsumerService'`.

- [ ] **Step 3: Add `MessageView`, `MessagePage`, and `ConsumerService` to `src/kafka/consumerService.ts`**

Change the import-less header of `src/kafka/consumerService.ts` from:

```typescript
export const PAGE_SIZE = 50;

export interface MessageWindow {
  from: number;
  to: number;
}

export type NavAction = 'latest' | 'earliest' | 'prev' | 'next' | 'refresh';
```

to:

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
```

Then append this after `computeWindow` (at the end of the file):

```typescript

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

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 79`, `# pass 79`, `# fail 0` (76 from Task 3, plus the 3 new tests above).

- [ ] **Step 5: Commit**

```bash
git add src/kafka/consumerService.ts src/test/consumerService.test.ts
git commit -m "feat: add ConsumerService.fetchPage"
```

---

## Task 5: `toMessageBrowserData`

**Files:**
- Create: `src/webviews/messageBrowserPanel.ts`
- Test: `src/test/messageBrowserPanel.test.ts`

- [ ] **Step 1: Write the failing tests**

Create `src/test/messageBrowserPanel.test.ts`:

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { toMessageBrowserData } from '../webviews/messageBrowserPanel';
import { MessagePage } from '../kafka/consumerService';

function buildPage(overrides: Partial<MessagePage> = {}): MessagePage {
  return {
    partition: 0,
    lowWatermark: 0,
    highWatermark: 200,
    window: { from: 150, to: 200 },
    messages: [],
    ...overrides,
  };
}

test('toMessageBrowserData pretty-prints JSON values, passes through non-JSON values, and keeps nulls', () => {
  const page = buildPage({
    messages: [
      { offset: 150, timestamp: '1700000000000', key: 'order-1', value: '{"id":1,"status":"ok"}', headers: {} },
      { offset: 151, timestamp: '1700000000001', key: 'order-2', value: 'not json', headers: {} },
      { offset: 152, timestamp: '1700000000002', key: null, value: null, headers: {} },
    ],
  });

  const data = toMessageBrowserData('orders.events', 3, page);

  assert.equal(data.messages[0].value, JSON.stringify({ id: 1, status: 'ok' }, null, 2));
  assert.equal(data.messages[1].value, 'not json');
  assert.equal(data.messages[2].key, null);
  assert.equal(data.messages[2].value, null);
});

test('toMessageBrowserData converts headers to an ordered array and passes through partition/window/watermarks', () => {
  const page = buildPage({
    partition: 2,
    lowWatermark: 10,
    highWatermark: 60,
    window: { from: 10, to: 60 },
    messages: [{ offset: 10, timestamp: '1700000000000', key: 'k', value: 'v', headers: { a: '1', b: '2' } }],
  });

  const data = toMessageBrowserData('orders.events', 3, page);

  assert.equal(data.topic, 'orders.events');
  assert.equal(data.partition, 2);
  assert.equal(data.partitionCount, 3);
  assert.equal(data.lowWatermark, 10);
  assert.equal(data.highWatermark, 60);
  assert.deepEqual(data.window, { from: 10, to: 60 });
  assert.deepEqual(data.messages[0].headers, [
    { key: 'a', value: '1' },
    { key: 'b', value: '2' },
  ]);
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test`
Expected: FAIL — TypeScript compile error `Cannot find module '../webviews/messageBrowserPanel'`.

- [ ] **Step 3: Create `src/webviews/messageBrowserPanel.ts`**

```typescript
import { MessagePage, MessageWindow } from '../kafka/consumerService';

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

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 81`, `# pass 81`, `# fail 0` (79 from Task 4, plus the 2 new tests above).

- [ ] **Step 5: Commit**

```bash
git add src/webviews/messageBrowserPanel.ts src/test/messageBrowserPanel.test.ts
git commit -m "feat: add toMessageBrowserData for the Message Browser"
```

---

## Task 6: `renderMessageBrowserHtml`

**Files:**
- Modify: `src/webviews/messageBrowserPanel.ts`
- Modify: `src/test/messageBrowserPanel.test.ts`

- [ ] **Step 1: Write the failing tests**

In `src/test/messageBrowserPanel.test.ts`, change the import line from:

```typescript
import { toMessageBrowserData } from '../webviews/messageBrowserPanel';
```

to:

```typescript
import { renderMessageBrowserHtml, toMessageBrowserData } from '../webviews/messageBrowserPanel';
```

Then append these tests at the end of the file:

```typescript

test('renderMessageBrowserHtml includes the topic name and control element ids', () => {
  const data = toMessageBrowserData('orders.events', 3, buildPage());
  const html = renderMessageBrowserHtml('orders.events', data);

  assert.match(html, /<title>Messages: orders\.events<\/title>/);
  assert.match(html, /id="title"/);
  assert.match(html, /id="partition"/);
  assert.match(html, /id="earliest"/);
  assert.match(html, /id="prev"/);
  assert.match(html, /id="next"/);
  assert.match(html, /id="latest"/);
  assert.match(html, /id="refresh"/);
  assert.match(html, /id="banner"/);
  assert.match(html, /id="windowInfo"/);
  assert.match(html, /id="rows"/);
});

test('renderMessageBrowserHtml embeds the serialized initial data and VALUE_TRUNCATE_LENGTH', () => {
  const data = toMessageBrowserData('orders.events', 3, buildPage());
  const html = renderMessageBrowserHtml('orders.events', data);

  assert.match(html, /<script>[\s\S]*const initialData = \{[\s\S]*"topic":"orders\.events"[\s\S]*\}[\s\S]*<\/script>/);
  assert.match(html, /const VALUE_TRUNCATE_LENGTH = 300;/);
});

test('renderMessageBrowserHtml escapes "</script>" sequences inside the serialized initial data', () => {
  const page = buildPage({
    messages: [
      { offset: 150, timestamp: '1700000000000', key: '</script><script>alert(1)</script>', value: null, headers: {} },
    ],
  });
  const data = toMessageBrowserData('orders.events', 3, page);
  const html = renderMessageBrowserHtml('orders.events', data);

  const closingTagCount = (html.match(/<\/script>/g) || []).length;
  assert.equal(closingTagCount, 1);
});

test('renderMessageBrowserHtml wires the partition select and nav buttons to postMessage', () => {
  const data = toMessageBrowserData('orders.events', 3, buildPage());
  const html = renderMessageBrowserHtml('orders.events', data);

  assert.match(html, /postMessage\(\{ type: 'setPartition', partition: Number\(event\.target\.value\) \}\)/);
  assert.match(html, /postMessage\(\{ type: 'nav', action: 'earliest' \}\)/);
  assert.match(html, /postMessage\(\{ type: 'nav', action: 'prev' \}\)/);
  assert.match(html, /postMessage\(\{ type: 'nav', action: 'next' \}\)/);
  assert.match(html, /postMessage\(\{ type: 'nav', action: 'latest' \}\)/);
  assert.match(html, /postMessage\(\{ type: 'nav', action: 'refresh' \}\)/);
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test`
Expected: FAIL — TypeScript compile error, e.g. `Module '"../webviews/messageBrowserPanel"' has no exported member 'renderMessageBrowserHtml'`.

- [ ] **Step 3: Add `renderMessageBrowserHtml` to `src/webviews/messageBrowserPanel.ts`**

Change the import line at the top of `src/webviews/messageBrowserPanel.ts` from:

```typescript
import { MessagePage, MessageWindow } from '../kafka/consumerService';
```

to:

```typescript
import { MessagePage, MessageWindow } from '../kafka/consumerService';
import { escapeHtml } from './topicMetadataPanel';
```

Then append this function at the end of the file:

```typescript

export function renderMessageBrowserHtml(topic: string, data: MessageBrowserData): string {
  const safeTopic = escapeHtml(topic);
  const initialData = JSON.stringify(data).replace(/</g, '\\u003c');
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Messages: ${safeTopic}</title>
<style>
  body { font-family: var(--vscode-font-family, sans-serif); padding: 0 16px; }
  table { border-collapse: collapse; width: 100%; margin: 12px 0; }
  th, td { border: 1px solid var(--vscode-panel-border, #ccc); padding: 4px 8px; text-align: left; vertical-align: top; }
  th { background: var(--vscode-editor-lineHighlightBackground, #eee); }
  td.value, td.headers { font-family: var(--vscode-editor-font-family, monospace); white-space: pre-wrap; word-break: break-all; }
  .controls { display: flex; align-items: center; gap: 8px; margin: 12px 0; flex-wrap: wrap; }
  .show-more { background: none; border: none; color: var(--vscode-textLink-foreground, #3794ff); cursor: pointer; padding: 0; text-decoration: underline; }
  #banner { background: var(--vscode-inputValidation-errorBackground, #5a1d1d); border: 1px solid var(--vscode-inputValidation-errorBorder, #be1100); padding: 8px; margin: 12px 0; }
</style>
</head>
<body>
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
<script>
  const vscode = acquireVsCodeApi();
  const initialData = ${initialData};
  const VALUE_TRUNCATE_LENGTH = ${VALUE_TRUNCATE_LENGTH};

  function appendTruncatable(cell, text) {
    if (text.length <= VALUE_TRUNCATE_LENGTH) {
      cell.textContent = text;
      return;
    }

    function renderTruncated() {
      cell.textContent = '';
      cell.appendChild(document.createTextNode(text.slice(0, VALUE_TRUNCATE_LENGTH) + '... '));
      const button = document.createElement('button');
      button.className = 'show-more';
      button.textContent = 'Show more';
      button.addEventListener('click', renderExpanded);
      cell.appendChild(button);
    }

    function renderExpanded() {
      cell.textContent = '';
      cell.appendChild(document.createTextNode(text + ' '));
      const button = document.createElement('button');
      button.className = 'show-more';
      button.textContent = 'Show less';
      button.addEventListener('click', renderTruncated);
      cell.appendChild(button);
    }

    renderTruncated();
  }

  function render(data) {
    document.getElementById('title').textContent = 'Messages: ' + data.topic + ' (partition ' + data.partition + ')';

    const partitionSelect = document.getElementById('partition');
    partitionSelect.textContent = '';
    for (let i = 0; i < data.partitionCount; i++) {
      const option = document.createElement('option');
      option.value = String(i);
      option.textContent = String(i);
      if (i === data.partition) option.selected = true;
      partitionSelect.appendChild(option);
    }

    document.getElementById('windowInfo').textContent =
      'Showing offsets ' + data.window.from + '-' + data.window.to + ' of ' + data.lowWatermark + '-' + data.highWatermark;

    document.getElementById('earliest').disabled = data.window.from <= data.lowWatermark;
    document.getElementById('prev').disabled = data.window.from <= data.lowWatermark;
    document.getElementById('next').disabled = data.window.to >= data.highWatermark;
    document.getElementById('latest').disabled = data.window.to >= data.highWatermark;

    const rows = document.getElementById('rows');
    rows.textContent = '';

    if (data.messages.length === 0) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = 5;
      td.textContent = 'No messages in this range.';
      tr.appendChild(td);
      rows.appendChild(tr);
      return;
    }

    for (const message of data.messages) {
      const tr = document.createElement('tr');

      const offsetCell = document.createElement('td');
      offsetCell.textContent = String(message.offset);
      tr.appendChild(offsetCell);

      const timestampCell = document.createElement('td');
      timestampCell.textContent = new Date(Number(message.timestamp)).toLocaleString();
      tr.appendChild(timestampCell);

      const keyCell = document.createElement('td');
      keyCell.textContent = message.key === null ? '(null)' : message.key;
      tr.appendChild(keyCell);

      const valueCell = document.createElement('td');
      valueCell.className = 'value';
      appendTruncatable(valueCell, message.value === null ? '(null)' : message.value);
      tr.appendChild(valueCell);

      const headersCell = document.createElement('td');
      headersCell.className = 'headers';
      headersCell.textContent = message.headers.map((h) => h.key + '=' + h.value).join(', ');
      tr.appendChild(headersCell);

      rows.appendChild(tr);
    }
  }

  render(initialData);

  window.addEventListener('message', (event) => {
    const message = event.data;
    if (message.type === 'update') {
      document.getElementById('banner').style.display = 'none';
      render(message.data);
    } else if (message.type === 'error') {
      const banner = document.getElementById('banner');
      banner.textContent = message.message;
      banner.style.display = 'block';
    }
  });

  document.getElementById('partition').addEventListener('change', (event) => {
    vscode.postMessage({ type: 'setPartition', partition: Number(event.target.value) });
  });

  document.getElementById('earliest').addEventListener('click', () => {
    vscode.postMessage({ type: 'nav', action: 'earliest' });
  });
  document.getElementById('prev').addEventListener('click', () => {
    vscode.postMessage({ type: 'nav', action: 'prev' });
  });
  document.getElementById('next').addEventListener('click', () => {
    vscode.postMessage({ type: 'nav', action: 'next' });
  });
  document.getElementById('latest').addEventListener('click', () => {
    vscode.postMessage({ type: 'nav', action: 'latest' });
  });
  document.getElementById('refresh').addEventListener('click', () => {
    vscode.postMessage({ type: 'nav', action: 'refresh' });
  });
</script>
</body>
</html>`;
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 85`, `# pass 85`, `# fail 0` (81 from Task 5, plus the 4 new tests above).

- [ ] **Step 5: Commit**

```bash
git add src/webviews/messageBrowserPanel.ts src/test/messageBrowserPanel.test.ts
git commit -m "feat: add renderMessageBrowserHtml for the Message Browser webview"
```

---

## Task 7: `MessageBrowserPanel` controller

**Files:**
- Create: `src/webviews/messageBrowserPanelController.ts`

No new unit tests — `MessageBrowserPanel` is a vscode `WebviewPanel` glue class, matching the established compile-only treatment of `topicMetadataPanelController.ts`/`lagDashboardPanelController.ts`. The pure functions it calls are already covered by Tasks 3-6.

- [ ] **Step 1: Create `src/webviews/messageBrowserPanelController.ts`**

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

- [ ] **Step 2: Run the tests to verify nothing broke**

Run: `npm run compile && npm test 2>&1 | tail -8`
Expected: compile succeeds; `# tests 85`, `# pass 85`, `# fail 0` (unchanged from Task 6 — this task adds no new tests).

- [ ] **Step 3: Commit**

```bash
git add src/webviews/messageBrowserPanelController.ts
git commit -m "feat: add MessageBrowserPanel controller"
```

---

## Task 8: Tree view wiring — "Kafka: Browse Messages" context menu entry

**Files:**
- Modify: `src/treeView/kafkaExplorerProvider.ts`
- Modify: `package.json`

No new unit tests — `src/test/treeItems.test.ts` is unaffected (the `'topic'` node's new `contextValue` is additive; existing assertions on label/description still hold).

- [ ] **Step 1: Give the `'topic'` tree node a `contextValue`**

In `src/treeView/kafkaExplorerProvider.ts`, in `getTreeItem`'s `'topic'` case, change:

```typescript
      case 'topic': {
        const view = buildTopicNode(element.topic.name, element.topic.partitionCount);
        const item = new vscode.TreeItem(view.label, vscode.TreeItemCollapsibleState.None);
        item.description = view.description;
        item.command = {
          command: 'kafkaLagMonitor.showTopicMetadata',
          title: 'Show Topic Metadata',
          arguments: [element.profile, element.topic.name],
        };
        return item;
      }
```

to:

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

- [ ] **Step 2: Add the `kafkaLagMonitor.browseMessages` command entry to `package.json`**

In `package.json`, in `contributes.commands`, change the last entry from:

```json
      {
        "command": "kafkaLagMonitor.reconnect",
        "title": "Kafka: Reconnect"
      }
    ],
```

to:

```json
      {
        "command": "kafkaLagMonitor.reconnect",
        "title": "Kafka: Reconnect"
      },
      {
        "command": "kafkaLagMonitor.browseMessages",
        "title": "Kafka: Browse Messages"
      }
    ],
```

- [ ] **Step 3: Add the context-menu entry to `package.json`**

In `package.json`, in `contributes.menus["view/item/context"]`, change:

```json
      "view/item/context": [
        {
          "command": "kafkaLagMonitor.editConnection",
          "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaConnection"
        },
        {
          "command": "kafkaLagMonitor.removeConnection",
          "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaConnection"
        },
        {
          "command": "kafkaLagMonitor.reconnect",
          "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaConnection"
        }
      ]
```

to:

```json
      "view/item/context": [
        {
          "command": "kafkaLagMonitor.editConnection",
          "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaConnection"
        },
        {
          "command": "kafkaLagMonitor.removeConnection",
          "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaConnection"
        },
        {
          "command": "kafkaLagMonitor.reconnect",
          "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaConnection"
        },
        {
          "command": "kafkaLagMonitor.browseMessages",
          "when": "view == kafkaLagMonitor.explorer && viewItem == kafkaTopic"
        }
      ]
```

- [ ] **Step 4: Run the tests and verify package.json validity**

Run: `npm run compile && npm test 2>&1 | tail -8`
Expected: compile succeeds; `# tests 85`, `# pass 85`, `# fail 0` (unchanged — this task adds no new tests).

Run: `node -e "JSON.parse(require('fs').readFileSync('package.json','utf8')); console.log('valid json')"`
Expected: `valid json`

- [ ] **Step 5: Commit**

```bash
git add src/treeView/kafkaExplorerProvider.ts package.json
git commit -m "feat: add Kafka: Browse Messages context menu entry on topic nodes"
```

---

## Task 9: `extension.ts` wiring — `buildKafka` helper, consumer client factory, command registration

**Files:**
- Modify: `src/extension.ts`

No new unit tests — command registration and the `buildKafka`/`createConsumerClient` factories are vscode/kafkajs glue, matching the established compile-only treatment of `extension.ts`.

- [ ] **Step 1: Add new imports**

In `src/extension.ts`, change the import block from:

```typescript
import { Kafka, SASLOptions } from 'kafkajs';
import * as vscode from 'vscode';
import { registerConnectionCommands } from './connection/connectionCommands';
import { ConnectionManager } from './connection/connectionManager';
import { getConnectionProfiles, getLagThresholds } from './connection/profileStore';
import { getCredential } from './connection/secretStore';
import { ConnectionProfile, SaslMechanism } from './connection/types';
import { createKafkaAdminClient } from './kafka/kafkaAdminAdapter';
import { createKafkaLogCreator } from './logging/kafkaLogCreator';
import { KafkaExplorerProvider } from './treeView/kafkaExplorerProvider';
import { LagDashboardPanel } from './webviews/lagDashboardPanelController';
import { TopicMetadataPanel } from './webviews/topicMetadataPanelController';
```

to:

```typescript
import { Kafka, SASLOptions } from 'kafkajs';
import * as vscode from 'vscode';
import { registerConnectionCommands } from './connection/connectionCommands';
import { ConnectionManager } from './connection/connectionManager';
import { getConnectionProfiles, getLagThresholds } from './connection/profileStore';
import { getCredential } from './connection/secretStore';
import { ConnectionProfile, SaslMechanism } from './connection/types';
import { createKafkaAdminClient } from './kafka/kafkaAdminAdapter';
import { createKafkaConsumerClient } from './kafka/kafkaConsumerAdapter';
import { createKafkaLogCreator } from './logging/kafkaLogCreator';
import { KafkaExplorerProvider } from './treeView/kafkaExplorerProvider';
import { LagDashboardPanel } from './webviews/lagDashboardPanelController';
import { MessageBrowserPanel } from './webviews/messageBrowserPanelController';
import { TopicMetadataPanel } from './webviews/topicMetadataPanelController';
```

- [ ] **Step 2: Factor `buildKafka(profile)` and add `createConsumerClient`**

In `src/extension.ts`, change:

```typescript
  const connectionManager = new ConnectionManager(async (profile) => {
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
    const kafka = new Kafka({
      clientId: profile.clientId,
      brokers: profile.brokers,
      ssl: profile.ssl,
      sasl,
      logCreator: createKafkaLogCreator((line) => output.appendLine(line)),
    });
    return createKafkaAdminClient(kafka.admin());
  });
```

to:

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

  const createConsumerClient = async (profile: ConnectionProfile) => createKafkaConsumerClient(await buildKafka(profile));
```

- [ ] **Step 3: Register `kafkaLagMonitor.browseMessages`**

In `src/extension.ts`, change the end of `activate()` from:

```typescript
  context.subscriptions.push(
    vscode.commands.registerCommand(
      'kafkaLagMonitor.showLagDashboard',
      async (profile: ConnectionProfile, groupId: string) => {
        const pollIntervalSeconds = vscode.workspace
          .getConfiguration('kafkaLagMonitor')
          .get('pollIntervalSeconds', 10);
        await LagDashboardPanel.show(connectionManager, profile, groupId, thresholds, pollIntervalSeconds);
      },
    ),
  );
}
```

to:

```typescript
  context.subscriptions.push(
    vscode.commands.registerCommand(
      'kafkaLagMonitor.showLagDashboard',
      async (profile: ConnectionProfile, groupId: string) => {
        const pollIntervalSeconds = vscode.workspace
          .getConfiguration('kafkaLagMonitor')
          .get('pollIntervalSeconds', 10);
        await LagDashboardPanel.show(connectionManager, profile, groupId, thresholds, pollIntervalSeconds);
      },
    ),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'kafkaLagMonitor.browseMessages',
      async (profile: ConnectionProfile, topicName: string) => {
        await MessageBrowserPanel.show(connectionManager, createConsumerClient, profile, topicName);
      },
    ),
  );
}
```

- [ ] **Step 4: Run the tests to verify nothing broke**

Run: `npm run compile && npm test 2>&1 | tail -8`
Expected: compile succeeds; `# tests 85`, `# pass 85`, `# fail 0` (unchanged — this task adds no new tests).

- [ ] **Step 5: Commit**

```bash
git add src/extension.ts
git commit -m "feat: register the Kafka: Browse Messages command"
```

---

## Task 10: Update README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Describe the Message Browser in the "Status" section**

In `README.md`, replace the "Status" section paragraph:

```markdown
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
`kafkaLagMonitor.pollIntervalSeconds`). Message Browser and Produce webviews
are planned in follow-up phases (see
`docs/superpowers/specs/2026-06-13-kafka-lag-monitor-design.md`).
```

with:

```markdown
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
partition selector. A Produce webview is planned in a follow-up phase (see
`docs/superpowers/specs/2026-06-13-kafka-lag-monitor-design.md`).
```

- [ ] **Step 2: Add a Message Browser verification step to the manual integration test**

In `README.md`, replace the final paragraph of the "Manual integration test" section:

```markdown
Then `F5` the extension and expand `local-cluster` in the Explorer sidebar —
`orders.events` should show 3 partitions, and `order-service` should show a
total lag of 3. Clicking `order-service` opens the Lag Dashboard, which should
show a Total Lag of 3 with one `orders.events` section and per-partition
progress bars.
```

with:

```markdown
Then `F5` the extension and expand `local-cluster` in the Explorer sidebar —
`orders.events` should show 3 partitions, and `order-service` should show a
total lag of 3. Clicking `order-service` opens the Lag Dashboard, which should
show a Total Lag of 3 with one `orders.events` section and per-partition
progress bars. Right-click `orders.events` and choose **Kafka: Browse
Messages** — the panel should open for partition 0 showing the most recent
messages with Offset/Timestamp/Key/Value/Headers columns; use the partition
selector and the Earliest/Prev/Next/Latest/Refresh buttons to navigate.
```

- [ ] **Step 3: Final verification**

Run: `npm run compile && npm test 2>&1 | tail -8`
Expected: compile succeeds; `# tests 85`, `# pass 85`, `# fail 0`.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: document the Message Browser webview"
```
