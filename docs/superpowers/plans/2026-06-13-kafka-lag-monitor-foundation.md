# Kafka Lag Monitor — Phase 1 Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the extension project and deliver a working, read-only sidebar tree that shows configured Kafka connections, their topics, and per-consumer-group lag — configured via VS Code settings (manual JSON for now; a connection wizard comes in a follow-up plan).

**Architecture:** Pure, vscode-independent modules (profile validation, lag math, `AdminService`, `ConnectionManager`, tree-item builders) are covered by `node:test` unit tests with fakes/injected dependencies. Thin vscode-glue modules (profile store, `TreeDataProvider`, `extension.ts`) wire the pure modules to the VS Code API and are verified manually via the Extension Development Host against the local `kafka-orchestrator` cluster (`localhost:9091`).

**Tech Stack:** TypeScript (`tsc`), `kafkajs` (Kafka admin client), Node built-in test runner (`node:test` / `node:assert/strict`), VS Code Extension API (TreeView, SecretStorage, configuration). Follows the same build/test conventions as the author's `vscode-edi-insight-plugin` (plain `tsc`, `node --test ./out/test/*.test.js`).

---

## Reference: Out of Scope for This Plan

These are deferred to follow-up plans (per the design spec's phasing) and are listed here so no task accidentally tries to build them:

- Connection add/edit/remove/reconnect commands (wizard UI) — for this plan, connections are added by editing `settings.json` directly.
- Topic Metadata webview, Lag Dashboard webview, Message Browser webview, Produce webview.
- Auto-polling / `pollingManager`.
- Output-channel-based kafkajs logging wiring into a real `OutputChannel` (the pure formatter is built now; wiring it into `new Kafka({ logCreator })` happens when `ConnectionManager` is wired into `extension.ts` in this plan's Task 10, using `console` as the sink is NOT acceptable — see Task 10 for the real wiring).

---

### Task 1: Project scaffold

**Files:**
- Create: `package.json`
- Create: `tsconfig.json`
- Create: `src/extension.ts`

- [ ] **Step 1: Create `package.json`**

```json
{
  "name": "kafka-lag-monitor",
  "displayName": "Kafka Lag Monitor",
  "description": "Monitor Kafka consumer lag, browse topics, and inspect cluster metadata from VS Code.",
  "version": "0.0.1",
  "publisher": "fattahpour",
  "license": "MIT",
  "engines": {
    "vscode": "^1.75.0"
  },
  "categories": ["Other"],
  "activationEvents": [],
  "main": "./out/extension.js",
  "scripts": {
    "vscode:prepublish": "npm run compile",
    "compile": "tsc -p ./",
    "watch": "tsc -watch -p ./",
    "pretest": "npm run compile",
    "test": "node --test ./out/test/*.test.js"
  },
  "dependencies": {
    "kafkajs": "^2.2.4"
  },
  "devDependencies": {
    "@types/node": "^20.0.0",
    "@types/vscode": "^1.75.0",
    "typescript": "^5.4.0"
  }
}
```

- [ ] **Step 2: Create `tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2020",
    "module": "commonjs",
    "lib": ["ES2020"],
    "outDir": "./out",
    "rootDir": "./src",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "resolveJsonModule": true
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules", "out"]
}
```

- [ ] **Step 3: Create minimal `src/extension.ts`**

```typescript
import * as vscode from 'vscode';

export function activate(context: vscode.ExtensionContext): void {
  const output = vscode.window.createOutputChannel('Kafka Lag Monitor');
  output.appendLine('Kafka Lag Monitor activated');
  context.subscriptions.push(output);
}

export function deactivate(): void {}
```

- [ ] **Step 4: Install dependencies**

Run: `npm install`
Expected: `node_modules/` created, `package-lock.json` created, no errors.

- [ ] **Step 5: Compile**

Run: `npm run compile`
Expected: completes with no errors, `out/extension.js` exists.

- [ ] **Step 6: Commit**

```bash
git add package.json package-lock.json tsconfig.json src/extension.ts
git commit -m "chore: scaffold Kafka Lag Monitor extension project"
```

---

### Task 2: Kafka log line formatter (pure)

**Files:**
- Create: `src/logging/kafkaLogCreator.ts`
- Test: `src/test/kafkaLogCreator.test.ts`

- [ ] **Step 1: Write the failing test**

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { createKafkaLogCreator } from '../logging/kafkaLogCreator';

test('formats a basic log entry as "[LABEL] namespace: message"', () => {
  const lines: string[] = [];
  const log = createKafkaLogCreator((line) => lines.push(line))(1);

  log({
    namespace: 'CONNECTION',
    level: 1,
    label: 'ERROR',
    log: { message: 'Connection error' },
  });

  assert.deepEqual(lines, ['[ERROR] CONNECTION: Connection error']);
});

test('appends extra log fields as key=value pairs', () => {
  const lines: string[] = [];
  const log = createKafkaLogCreator((line) => lines.push(line))(2);

  log({
    namespace: 'CONNECTION',
    level: 2,
    label: 'WARN',
    log: { message: 'Retrying', broker: 'localhost:9092', retryCount: 2 },
  });

  assert.deepEqual(lines, ['[WARN] CONNECTION: Retrying (broker=localhost:9092, retryCount=2)']);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm run compile && node --test ./out/test/kafkaLogCreator.test.js`
Expected: FAIL — `Cannot find module '../logging/kafkaLogCreator'` (compile error) or test file missing from `out/`.

- [ ] **Step 3: Write the implementation**

```typescript
export interface KafkaLogEntry {
  namespace: string;
  level: number;
  label: string;
  log: { message: string; [key: string]: unknown };
}

export type KafkaLogCreator = (logLevel: number) => (entry: KafkaLogEntry) => void;

export function createKafkaLogCreator(sink: (line: string) => void): KafkaLogCreator {
  return () => (entry: KafkaLogEntry) => {
    const { namespace, label, log } = entry;
    const { message, ...extra } = log;
    const extraKeys = Object.keys(extra);
    const suffix =
      extraKeys.length > 0
        ? ' (' + extraKeys.map((k) => `${k}=${String(extra[k])}`).join(', ') + ')'
        : '';
    sink(`[${label}] ${namespace}: ${message}${suffix}`);
  };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm run compile && node --test ./out/test/kafkaLogCreator.test.js`
Expected: PASS — 2 tests, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add src/logging/kafkaLogCreator.ts src/test/kafkaLogCreator.test.ts
git commit -m "feat: add kafkajs log line formatter"
```

---

### Task 3: Connection profile types and validation (pure)

**Files:**
- Create: `src/connection/types.ts`
- Create: `src/connection/profileValidation.ts`
- Test: `src/test/profileValidation.test.ts`

- [ ] **Step 1: Create `src/connection/types.ts`**

```typescript
export type SaslMechanism = 'plain' | 'scram-sha-256' | 'scram-sha-512';

export interface SaslConfig {
  mechanism: SaslMechanism;
}

export interface ConnectionProfile {
  name: string;
  brokers: string[];
  sasl: SaslConfig | null;
  ssl: boolean;
  clientId: string;
}

export type ConnectionStatus = 'idle' | 'connecting' | 'connected' | 'error';
```

- [ ] **Step 2: Write the failing test**

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { parseConnectionProfiles, validateProfile } from '../connection/profileValidation';

test('validateProfile accepts a minimal valid profile', () => {
  const { profile, errors } = validateProfile({
    name: 'local-cluster',
    brokers: ['localhost:9091', 'localhost:9092'],
  });

  assert.deepEqual(errors, []);
  assert.deepEqual(profile, {
    name: 'local-cluster',
    brokers: ['localhost:9091', 'localhost:9092'],
    sasl: null,
    ssl: false,
    clientId: 'kafka-lag-monitor',
  });
});

test('validateProfile rejects a missing brokers array', () => {
  const { profile, errors } = validateProfile({ name: 'local-cluster' });

  assert.equal(profile, null);
  assert.equal(errors.length, 1);
  assert.match(errors[0], /brokers/);
});

test('validateProfile rejects a malformed broker entry', () => {
  const { profile, errors } = validateProfile({
    name: 'local-cluster',
    brokers: ['localhost'],
  });

  assert.equal(profile, null);
  assert.match(errors[0], /host:port/);
});

test('validateProfile rejects an unknown sasl mechanism', () => {
  const { profile, errors } = validateProfile({
    name: 'local-cluster',
    brokers: ['localhost:9091'],
    sasl: { mechanism: 'gssapi' },
  });

  assert.equal(profile, null);
  assert.match(errors[0], /sasl\.mechanism/);
});

test('validateProfile accepts sasl, ssl, and a custom clientId', () => {
  const { profile, errors } = validateProfile({
    name: 'secure-cluster',
    brokers: ['broker1:9092'],
    sasl: { mechanism: 'scram-sha-512' },
    ssl: true,
    clientId: 'my-client',
  });

  assert.deepEqual(errors, []);
  assert.deepEqual(profile, {
    name: 'secure-cluster',
    brokers: ['broker1:9092'],
    sasl: { mechanism: 'scram-sha-512' },
    ssl: true,
    clientId: 'my-client',
  });
});

test('parseConnectionProfiles separates valid profiles from invalid entries by index', () => {
  const { profiles, errors } = parseConnectionProfiles([
    { name: 'good', brokers: ['localhost:9091'] },
    { name: 'bad' },
  ]);

  assert.equal(profiles.length, 1);
  assert.equal(profiles[0].name, 'good');
  assert.deepEqual(errors, [{ index: 1, errors: errors[0].errors }]);
  assert.match(errors[0].errors[0], /brokers/);
});

test('parseConnectionProfiles returns empty results for non-array input', () => {
  const { profiles, errors } = parseConnectionProfiles('not-an-array');

  assert.deepEqual(profiles, []);
  assert.deepEqual(errors, []);
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `npm run compile && node --test ./out/test/profileValidation.test.js`
Expected: FAIL — `Cannot find module '../connection/profileValidation'`.

- [ ] **Step 4: Write the implementation**

```typescript
import { ConnectionProfile, SaslMechanism } from './types';

const SASL_MECHANISMS: SaslMechanism[] = ['plain', 'scram-sha-256', 'scram-sha-512'];
const BROKER_PATTERN = /^[\w.-]+:\d+$/;

export function validateProfile(raw: unknown): { profile: ConnectionProfile | null; errors: string[] } {
  const errors: string[] = [];

  if (typeof raw !== 'object' || raw === null) {
    return { profile: null, errors: ['Connection must be an object'] };
  }
  const obj = raw as Record<string, unknown>;

  if (typeof obj.name !== 'string' || obj.name.trim() === '') {
    errors.push('"name" must be a non-empty string');
  }

  let brokers: string[] = [];
  if (!Array.isArray(obj.brokers) || obj.brokers.length === 0) {
    errors.push('"brokers" must be a non-empty array of "host:port" strings');
  } else {
    for (const b of obj.brokers) {
      if (typeof b !== 'string' || !BROKER_PATTERN.test(b)) {
        errors.push(`"brokers" entry "${String(b)}" must look like "host:port"`);
      }
    }
    brokers = obj.brokers as string[];
  }

  let sasl: ConnectionProfile['sasl'] = null;
  if (obj.sasl !== null && obj.sasl !== undefined) {
    if (typeof obj.sasl !== 'object') {
      errors.push('"sasl" must be an object or null');
    } else {
      const mechanism = (obj.sasl as Record<string, unknown>).mechanism;
      if (typeof mechanism !== 'string' || !SASL_MECHANISMS.includes(mechanism as SaslMechanism)) {
        errors.push(`"sasl.mechanism" must be one of ${SASL_MECHANISMS.join(', ')}`);
      } else {
        sasl = { mechanism: mechanism as SaslMechanism };
      }
    }
  }

  const ssl = obj.ssl === true;
  const clientId =
    typeof obj.clientId === 'string' && obj.clientId.trim() !== '' ? obj.clientId : 'kafka-lag-monitor';

  if (errors.length > 0) {
    return { profile: null, errors };
  }

  return {
    profile: { name: obj.name as string, brokers, sasl, ssl, clientId },
    errors: [],
  };
}

export function parseConnectionProfiles(raw: unknown): {
  profiles: ConnectionProfile[];
  errors: { index: number; errors: string[] }[];
} {
  if (!Array.isArray(raw)) {
    return { profiles: [], errors: [] };
  }
  const profiles: ConnectionProfile[] = [];
  const errors: { index: number; errors: string[] }[] = [];
  raw.forEach((item, index) => {
    const result = validateProfile(item);
    if (result.profile) {
      profiles.push(result.profile);
    } else {
      errors.push({ index, errors: result.errors });
    }
  });
  return { profiles, errors };
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `npm run compile && node --test ./out/test/profileValidation.test.js`
Expected: PASS — 7 tests, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add src/connection/types.ts src/connection/profileValidation.ts src/test/profileValidation.test.ts
git commit -m "feat: add connection profile types and validation"
```

---

### Task 4: Lag calculation (pure)

**Files:**
- Create: `src/kafka/lag.ts`
- Test: `src/test/lag.test.ts`

- [ ] **Step 1: Write the failing test**

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { aggregateTopicLag, computePartitionLag, lagSeverity } from '../kafka/lag';

test('computePartitionLag reports lag when the group is behind', () => {
  const result = computePartitionLag(0, 401, 600);
  assert.deepEqual(result, { partition: 0, currentOffset: 401, endOffset: 600, lag: 199, status: 'lag' });
});

test('computePartitionLag reports ok when fully caught up', () => {
  const result = computePartitionLag(1, 600, 600);
  assert.deepEqual(result, { partition: 1, currentOffset: 600, endOffset: 600, lag: 0, status: 'ok' });
});

test('computePartitionLag reports not-started when there is no committed offset', () => {
  const result = computePartitionLag(2, null, 600);
  assert.deepEqual(result, { partition: 2, currentOffset: 0, endOffset: 600, lag: 600, status: 'not-started' });
});

test('computePartitionLag reports ok for an empty partition with no committed offset', () => {
  const result = computePartitionLag(3, null, 0);
  assert.deepEqual(result, { partition: 3, currentOffset: 0, endOffset: 0, lag: 0, status: 'ok' });
});

test('aggregateTopicLag sums lag across partitions', () => {
  const partitions = [
    computePartitionLag(0, 401, 600),
    computePartitionLag(1, 600, 600),
    computePartitionLag(2, null, 220),
  ];

  const result = aggregateTopicLag('orders.events', partitions);

  assert.equal(result.topic, 'orders.events');
  assert.equal(result.partitions, partitions);
  assert.equal(result.totalLag, 199 + 0 + 220);
});

test('lagSeverity boundaries', () => {
  assert.equal(lagSeverity(99, 100, 1000), 'none');
  assert.equal(lagSeverity(100, 100, 1000), 'warning');
  assert.equal(lagSeverity(999, 100, 1000), 'warning');
  assert.equal(lagSeverity(1000, 100, 1000), 'critical');
  assert.equal(lagSeverity(0, 100, 1000), 'none');
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm run compile && node --test ./out/test/lag.test.js`
Expected: FAIL — `Cannot find module '../kafka/lag'`.

- [ ] **Step 3: Write the implementation**

```typescript
export type LagStatus = 'ok' | 'lag' | 'not-started';

export interface PartitionLag {
  partition: number;
  currentOffset: number;
  endOffset: number;
  lag: number;
  status: LagStatus;
}

export function computePartitionLag(
  partition: number,
  committedOffset: number | null,
  highWatermark: number,
): PartitionLag {
  if (committedOffset === null) {
    return {
      partition,
      currentOffset: 0,
      endOffset: highWatermark,
      lag: highWatermark,
      status: highWatermark > 0 ? 'not-started' : 'ok',
    };
  }
  const lag = Math.max(highWatermark - committedOffset, 0);
  return {
    partition,
    currentOffset: committedOffset,
    endOffset: highWatermark,
    lag,
    status: lag > 0 ? 'lag' : 'ok',
  };
}

export interface TopicLag {
  topic: string;
  partitions: PartitionLag[];
  totalLag: number;
}

export function aggregateTopicLag(topic: string, partitions: PartitionLag[]): TopicLag {
  const totalLag = partitions.reduce((sum, p) => sum + p.lag, 0);
  return { topic, partitions, totalLag };
}

export type LagSeverity = 'none' | 'warning' | 'critical';

export function lagSeverity(totalLag: number, warningThreshold: number, criticalThreshold: number): LagSeverity {
  if (totalLag >= criticalThreshold) return 'critical';
  if (totalLag >= warningThreshold) return 'warning';
  return 'none';
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm run compile && node --test ./out/test/lag.test.js`
Expected: PASS — 6 tests, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add src/kafka/lag.ts src/test/lag.test.ts
git commit -m "feat: add consumer lag calculation"
```

---

### Task 5: Kafka admin client interface and AdminService (topics, metadata, configs)

**Files:**
- Create: `src/kafka/adminClient.ts`
- Create: `src/kafka/adminService.ts`
- Test: `src/test/adminService.test.ts`

- [ ] **Step 1: Create `src/kafka/adminClient.ts`**

This is the seam between our code and kafkajs: a minimal interface covering only the `Admin` methods we use, so tests can supply fakes and `ConnectionManager` (Task 7) can adapt the real kafkajs `Admin` to it.

```typescript
export interface KafkaTopicPartitionMetadata {
  partitionId: number;
  leader: number;
  replicas: number[];
  isr: number[];
}

export interface KafkaTopicMetadata {
  topics: Array<{ name: string; partitions: KafkaTopicPartitionMetadata[] }>;
}

export interface KafkaConfigEntry {
  configName: string;
  configValue: string | null;
  isDefault: boolean;
}

export interface KafkaDescribeConfigsResult {
  resources: Array<{ configEntries: KafkaConfigEntry[] }>;
}

export interface KafkaGroupOverview {
  groupId: string;
  protocolType: string;
}

export interface KafkaFetchOffsetsTopic {
  topic: string;
  partitions: Array<{ partition: number; offset: string }>;
}

export interface KafkaTopicOffset {
  partition: number;
  offset: string;
  high: string;
  low: string;
}

export interface KafkaAdminClient {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  listTopics(): Promise<string[]>;
  fetchTopicMetadata(args: { topics: string[] }): Promise<KafkaTopicMetadata>;
  describeConfigs(args: {
    resources: Array<{ type: number; name: string }>;
    includeSynonyms: boolean;
  }): Promise<KafkaDescribeConfigsResult>;
  listGroups(): Promise<{ groups: KafkaGroupOverview[] }>;
  fetchOffsets(args: { groupId: string }): Promise<KafkaFetchOffsetsTopic[]>;
  fetchTopicOffsets(topic: string): Promise<KafkaTopicOffset[]>;
}
```

- [ ] **Step 2: Write the failing test**

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { AdminService } from '../kafka/adminService';
import { KafkaAdminClient } from '../kafka/adminClient';

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

test('listTopics filters internal topics and reports partition counts', async () => {
  const admin = createFakeAdminClient({
    listTopics: async () => ['orders.events', '__consumer_offsets', 'payments.dlq'],
    fetchTopicMetadata: async ({ topics }) => ({
      topics: topics.map((name) => ({
        name,
        partitions: name === 'orders.events' ? [{}, {}, {}, {}, {}, {}] : [{}, {}, {}],
      })) as any,
    }),
  });

  const result = await new AdminService(admin).listTopics();

  assert.deepEqual(result, [
    { name: 'orders.events', partitionCount: 6 },
    { name: 'payments.dlq', partitionCount: 3 },
  ]);
});

test('listTopics returns an empty array when there are no user topics', async () => {
  const admin = createFakeAdminClient({
    listTopics: async () => ['__consumer_offsets'],
  });

  const result = await new AdminService(admin).listTopics();

  assert.deepEqual(result, []);
});

test('getTopicMetadata maps partition leader, replicas, and ISR', async () => {
  const admin = createFakeAdminClient({
    fetchTopicMetadata: async () => ({
      topics: [
        {
          name: 'orders.events',
          partitions: [
            { partitionId: 0, leader: 1, replicas: [1, 2, 3], isr: [1, 2, 3] },
            { partitionId: 1, leader: 2, replicas: [2, 3, 1], isr: [2, 3] },
          ],
        },
      ],
    }),
  });

  const result = await new AdminService(admin).getTopicMetadata('orders.events');

  assert.deepEqual(result, {
    name: 'orders.events',
    partitions: [
      { partitionId: 0, leader: 1, replicas: [1, 2, 3], isr: [1, 2, 3] },
      { partitionId: 1, leader: 2, replicas: [2, 3, 1], isr: [2, 3] },
    ],
  });
});

test('getTopicMetadata throws when the topic is missing from the response', async () => {
  const admin = createFakeAdminClient({
    fetchTopicMetadata: async () => ({ topics: [] }),
  });

  await assert.rejects(() => new AdminService(admin).getTopicMetadata('missing-topic'), /not found/);
});

test('getTopicConfig maps config entries', async () => {
  const admin = createFakeAdminClient({
    describeConfigs: async () => ({
      resources: [
        {
          configEntries: [
            { configName: 'retention.ms', configValue: '604800000', isDefault: false },
            { configName: 'cleanup.policy', configValue: 'delete', isDefault: true },
          ],
        },
      ],
    }),
  });

  const result = await new AdminService(admin).getTopicConfig('orders.events');

  assert.deepEqual(result, [
    { name: 'retention.ms', value: '604800000', isDefault: false },
    { name: 'cleanup.policy', value: 'delete', isDefault: true },
  ]);
});

test('getTopicConfig returns an empty array when no resource is returned', async () => {
  const admin = createFakeAdminClient({
    describeConfigs: async () => ({ resources: [] }),
  });

  const result = await new AdminService(admin).getTopicConfig('orders.events');

  assert.deepEqual(result, []);
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `npm run compile && node --test ./out/test/adminService.test.js`
Expected: FAIL — `Cannot find module '../kafka/adminService'`.

- [ ] **Step 4: Write the implementation**

```typescript
import { KafkaAdminClient } from './adminClient';

export interface TopicSummary {
  name: string;
  partitionCount: number;
}

export interface PartitionMetadata {
  partitionId: number;
  leader: number;
  replicas: number[];
  isr: number[];
}

export interface TopicMetadata {
  name: string;
  partitions: PartitionMetadata[];
}

export interface ConfigEntry {
  name: string;
  value: string | null;
  isDefault: boolean;
}

const TOPIC_RESOURCE_TYPE = 2; // kafkajs ResourceTypes.TOPIC

export class AdminService {
  constructor(private readonly admin: KafkaAdminClient) {}

  async listTopics(): Promise<TopicSummary[]> {
    const names = (await this.admin.listTopics()).filter((n) => !n.startsWith('__'));
    if (names.length === 0) return [];
    const metadata = await this.admin.fetchTopicMetadata({ topics: names });
    return metadata.topics.map((t) => ({ name: t.name, partitionCount: t.partitions.length }));
  }

  async getTopicMetadata(topic: string): Promise<TopicMetadata> {
    const metadata = await this.admin.fetchTopicMetadata({ topics: [topic] });
    const found = metadata.topics.find((t) => t.name === topic);
    if (!found) {
      throw new Error(`Topic "${topic}" not found`);
    }
    return {
      name: found.name,
      partitions: found.partitions.map((p) => ({
        partitionId: p.partitionId,
        leader: p.leader,
        replicas: p.replicas,
        isr: p.isr,
      })),
    };
  }

  async getTopicConfig(topic: string): Promise<ConfigEntry[]> {
    const result = await this.admin.describeConfigs({
      resources: [{ type: TOPIC_RESOURCE_TYPE, name: topic }],
      includeSynonyms: false,
    });
    const resource = result.resources[0];
    if (!resource) return [];
    return resource.configEntries.map((e) => ({ name: e.configName, value: e.configValue, isDefault: e.isDefault }));
  }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `npm run compile && node --test ./out/test/adminService.test.js`
Expected: PASS — 6 tests, 0 failures.

- [ ] **Step 6: Commit**

```bash
git add src/kafka/adminClient.ts src/kafka/adminService.ts src/test/adminService.test.ts
git commit -m "feat: add AdminService for topic listing, metadata, and configs"
```

---

### Task 6: AdminService — consumer groups and lag

**Files:**
- Modify: `src/kafka/adminService.ts`
- Modify: `src/test/adminService.test.ts`

- [ ] **Step 1: Append the failing tests to `src/test/adminService.test.ts`**

Add these tests at the end of the file (keep the existing imports and tests from Task 5):

```typescript
test('listConsumerGroups filters out non-consumer protocol groups', async () => {
  const admin = createFakeAdminClient({
    listGroups: async () => ({
      groups: [
        { groupId: 'order-service', protocolType: 'consumer' },
        { groupId: 'kafka-connect-cluster', protocolType: '' },
      ],
    }),
  });

  const result = await new AdminService(admin).listConsumerGroups();

  assert.deepEqual(result, [{ groupId: 'order-service' }]);
});

test('getGroupLag computes per-partition and total lag, including not-started partitions', async () => {
  const admin = createFakeAdminClient({
    fetchOffsets: async ({ groupId }) => {
      assert.equal(groupId, 'order-service');
      return [
        {
          topic: 'orders.events',
          partitions: [
            { partition: 0, offset: '401' },
            { partition: 1, offset: '-1' },
          ],
        },
      ];
    },
    fetchTopicOffsets: async (topic) => {
      assert.equal(topic, 'orders.events');
      return [
        { partition: 0, offset: '0', high: '600', low: '0' },
        { partition: 1, offset: '0', high: '220', low: '0' },
      ];
    },
  });

  const result = await new AdminService(admin).getGroupLag('order-service');

  assert.equal(result.length, 1);
  assert.equal(result[0].topic, 'orders.events');
  assert.equal(result[0].totalLag, 199 + 220);
  assert.deepEqual(result[0].partitions[0], {
    partition: 0,
    currentOffset: 401,
    endOffset: 600,
    lag: 199,
    status: 'lag',
  });
  assert.deepEqual(result[0].partitions[1], {
    partition: 1,
    currentOffset: 0,
    endOffset: 220,
    lag: 220,
    status: 'not-started',
  });
});

test('getGroupLag returns an empty array for a group with no committed offsets', async () => {
  const admin = createFakeAdminClient({
    fetchOffsets: async () => [],
  });

  const result = await new AdminService(admin).getGroupLag('idle-group');

  assert.deepEqual(result, []);
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm run compile && node --test ./out/test/adminService.test.js`
Expected: FAIL — compile error, `Property 'listConsumerGroups' does not exist on type 'AdminService'` (and `getGroupLag`).

- [ ] **Step 3: Add the implementation to `src/kafka/adminService.ts`**

Add this import at the top of the file (alongside the existing `KafkaAdminClient` import):

```typescript
import { aggregateTopicLag, computePartitionLag, TopicLag } from './lag';
```

Add this interface near the other interfaces:

```typescript
export interface ConsumerGroupSummary {
  groupId: string;
}
```

Add these two methods inside the `AdminService` class, after `getTopicConfig`:

```typescript
  async listConsumerGroups(): Promise<ConsumerGroupSummary[]> {
    const { groups } = await this.admin.listGroups();
    return groups.filter((g) => g.protocolType === 'consumer').map((g) => ({ groupId: g.groupId }));
  }

  async getGroupLag(groupId: string): Promise<TopicLag[]> {
    const offsetsByTopic = await this.admin.fetchOffsets({ groupId });
    const result: TopicLag[] = [];
    for (const { topic, partitions } of offsetsByTopic) {
      const highWatermarks = await this.admin.fetchTopicOffsets(topic);
      const hwByPartition = new Map(highWatermarks.map((h) => [h.partition, Number(h.high)]));
      const partitionLags = partitions.map((p) => {
        const committed = Number(p.offset);
        const highWatermark = hwByPartition.get(p.partition) ?? 0;
        return computePartitionLag(p.partition, committed < 0 ? null : committed, highWatermark);
      });
      result.push(aggregateTopicLag(topic, partitionLags));
    }
    return result;
  }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm run compile && node --test ./out/test/adminService.test.js`
Expected: PASS — 9 tests, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add src/kafka/adminService.ts src/test/adminService.test.ts
git commit -m "feat: add consumer group listing and lag calculation to AdminService"
```

---

### Task 7: ConnectionManager (vscode-agnostic, testable)

**Files:**
- Create: `src/connection/connectionManager.ts`
- Test: `src/test/connectionManager.test.ts`

- [ ] **Step 1: Write the failing test**

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { ConnectionManager } from '../connection/connectionManager';
import { KafkaAdminClient } from '../kafka/adminClient';
import { ConnectionProfile } from '../connection/types';

function createFakeAdminClient(overrides: Partial<KafkaAdminClient> = {}): KafkaAdminClient {
  const notImplemented = () => {
    throw new Error('not implemented in fake');
  };
  return {
    connect: async () => {},
    disconnect: async () => {},
    listTopics: notImplemented,
    fetchTopicMetadata: notImplemented,
    describeConfigs: notImplemented,
    listGroups: notImplemented,
    fetchOffsets: notImplemented,
    fetchTopicOffsets: notImplemented,
    ...overrides,
  } as KafkaAdminClient;
}

const profile: ConnectionProfile = {
  name: 'local-cluster',
  brokers: ['localhost:9091'],
  sasl: null,
  ssl: false,
  clientId: 'kafka-lag-monitor',
};

test('connect transitions idle -> connected and exposes an AdminService', async () => {
  const client = createFakeAdminClient();
  const manager = new ConnectionManager(() => client);

  assert.equal(manager.getState(profile.name).status, 'idle');

  await manager.connect(profile);

  assert.equal(manager.getState(profile.name).status, 'connected');
  assert.ok(manager.getAdminService(profile.name));
});

test('connect sets status to error with the failure message when connect() rejects', async () => {
  const client = createFakeAdminClient({
    connect: async () => {
      throw new Error('ECONNREFUSED');
    },
  });
  const manager = new ConnectionManager(() => client);

  await assert.rejects(() => manager.connect(profile), /ECONNREFUSED/);

  assert.deepEqual(manager.getState(profile.name), { status: 'error', error: 'ECONNREFUSED' });
  assert.equal(manager.getAdminService(profile.name), undefined);
});

test('disconnect resets status to idle and re-creates the client on the next connect', async () => {
  let createCount = 0;
  const manager = new ConnectionManager(() => {
    createCount += 1;
    return createFakeAdminClient();
  });

  await manager.connect(profile);
  await manager.disconnect(profile.name);

  assert.equal(manager.getState(profile.name).status, 'idle');
  assert.equal(manager.getAdminService(profile.name), undefined);

  await manager.connect(profile);

  assert.equal(createCount, 2);
});

test('getState returns idle for a profile that has never been connected', () => {
  const manager = new ConnectionManager(() => createFakeAdminClient());
  assert.deepEqual(manager.getState('never-seen'), { status: 'idle' });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm run compile && node --test ./out/test/connectionManager.test.js`
Expected: FAIL — `Cannot find module '../connection/connectionManager'`.

- [ ] **Step 3: Write the implementation**

```typescript
import { ConnectionProfile, ConnectionStatus } from './types';
import { KafkaAdminClient } from '../kafka/adminClient';
import { AdminService } from '../kafka/adminService';

export interface ConnectionState {
  status: ConnectionStatus;
  error?: string;
}

export type AdminClientFactory = (profile: ConnectionProfile) => KafkaAdminClient;

export class ConnectionManager {
  private readonly clients = new Map<string, KafkaAdminClient>();
  private readonly states = new Map<string, ConnectionState>();

  constructor(private readonly createAdminClient: AdminClientFactory) {}

  getState(profileName: string): ConnectionState {
    return this.states.get(profileName) ?? { status: 'idle' };
  }

  async connect(profile: ConnectionProfile): Promise<void> {
    this.states.set(profile.name, { status: 'connecting' });
    try {
      let client = this.clients.get(profile.name);
      if (!client) {
        client = this.createAdminClient(profile);
        this.clients.set(profile.name, client);
      }
      await client.connect();
      this.states.set(profile.name, { status: 'connected' });
    } catch (err) {
      this.states.set(profile.name, { status: 'error', error: (err as Error).message });
      throw err;
    }
  }

  async disconnect(profileName: string): Promise<void> {
    const client = this.clients.get(profileName);
    if (client) {
      await client.disconnect();
      this.clients.delete(profileName);
    }
    this.states.set(profileName, { status: 'idle' });
  }

  getAdminService(profileName: string): AdminService | undefined {
    const state = this.getState(profileName);
    const client = this.clients.get(profileName);
    if (state.status !== 'connected' || !client) return undefined;
    return new AdminService(client);
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm run compile && node --test ./out/test/connectionManager.test.js`
Expected: PASS — 4 tests, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add src/connection/connectionManager.ts src/test/connectionManager.test.ts
git commit -m "feat: add ConnectionManager for cached admin clients and connection status"
```

---

### Task 8: Tree item builders (pure)

**Files:**
- Create: `src/treeView/treeItems.ts`
- Test: `src/test/treeItems.test.ts`

- [ ] **Step 1: Write the failing test**

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { buildConnectionNode, buildGroupNode, buildPartitionNode, buildTopicNode } from '../treeView/treeItems';

test('buildConnectionNode shows a checkmark when connected and no description', () => {
  assert.deepEqual(buildConnectionNode('local-cluster', 'connected'), {
    label: 'local-cluster ✓',
    description: '',
  });
});

test('buildConnectionNode shows the error message as the description when errored', () => {
  assert.deepEqual(buildConnectionNode('local-cluster', 'error', 'ECONNREFUSED'), {
    label: 'local-cluster ⚠',
    description: 'ECONNREFUSED',
  });
});

test('buildTopicNode pluralizes the partition count', () => {
  assert.deepEqual(buildTopicNode('orders.events', 6), { label: 'orders.events', description: '6 partitions' });
  assert.deepEqual(buildTopicNode('single-partition-topic', 1), {
    label: 'single-partition-topic',
    description: '1 partition',
  });
});

test('buildGroupNode includes the total lag and severity', () => {
  assert.deepEqual(buildGroupNode('order-service', 1420, 'critical'), {
    label: 'order-service',
    description: '●1420',
    severity: 'critical',
  });
});

test('buildPartitionNode formats current/end (lag)', () => {
  assert.deepEqual(buildPartitionNode(0, 401, 600, 199), { label: 'p0: 401/600 (199)' });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm run compile && node --test ./out/test/treeItems.test.js`
Expected: FAIL — `Cannot find module '../treeView/treeItems'`.

- [ ] **Step 3: Write the implementation**

```typescript
import { ConnectionStatus } from '../connection/types';
import { LagSeverity } from '../kafka/lag';

export interface ConnectionNodeView {
  label: string;
  description: string;
}

const STATUS_ICONS: Record<ConnectionStatus, string> = {
  idle: '⚪',
  connecting: '…',
  connected: '✓',
  error: '⚠',
};

export function buildConnectionNode(name: string, status: ConnectionStatus, errorMessage?: string): ConnectionNodeView {
  return {
    label: `${name} ${STATUS_ICONS[status]}`,
    description: status === 'error' && errorMessage ? errorMessage : '',
  };
}

export interface TopicNodeView {
  label: string;
  description: string;
}

export function buildTopicNode(name: string, partitionCount: number): TopicNodeView {
  return { label: name, description: `${partitionCount} partition${partitionCount === 1 ? '' : 's'}` };
}

export interface GroupNodeView {
  label: string;
  description: string;
  severity: LagSeverity;
}

export function buildGroupNode(groupId: string, totalLag: number, severity: LagSeverity): GroupNodeView {
  return { label: groupId, description: `●${totalLag}`, severity };
}

export interface PartitionNodeView {
  label: string;
}

export function buildPartitionNode(
  partition: number,
  currentOffset: number,
  endOffset: number,
  lag: number,
): PartitionNodeView {
  return { label: `p${partition}: ${currentOffset}/${endOffset} (${lag})` };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm run compile && node --test ./out/test/treeItems.test.js`
Expected: PASS — 5 tests, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add src/treeView/treeItems.ts src/test/treeItems.test.ts
git commit -m "feat: add tree item label/description builders"
```

---

### Task 9: Profile store and secret store (settings + SecretStorage)

**Files:**
- Create: `src/connection/secretKey.ts`
- Test: `src/test/secretKey.test.ts`
- Create: `src/connection/secretStore.ts`
- Create: `src/connection/profileStore.ts`

- [ ] **Step 1: Write the failing test for the secret key formatter**

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { secretKey } from '../connection/secretKey';

test('secretKey namespaces by profile name and field', () => {
  assert.equal(secretKey('local-cluster', 'password'), 'kafkaLagMonitor.connection.local-cluster.password');
});

test('secretKey keeps different profiles and fields distinct', () => {
  assert.notEqual(secretKey('local-cluster', 'password'), secretKey('staging-cluster', 'password'));
  assert.notEqual(secretKey('local-cluster', 'username'), secretKey('local-cluster', 'password'));
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm run compile && node --test ./out/test/secretKey.test.js`
Expected: FAIL — `Cannot find module '../connection/secretKey'`.

- [ ] **Step 3: Write `src/connection/secretKey.ts`**

```typescript
export function secretKey(profileName: string, field: string): string {
  return `kafkaLagMonitor.connection.${profileName}.${field}`;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm run compile && node --test ./out/test/secretKey.test.js`
Expected: PASS — 2 tests, 0 failures.

- [ ] **Step 5: Write `src/connection/secretStore.ts`**

This wraps `vscode.SecretStorage`. It imports `vscode`, so it is exercised manually (Task 10) rather than via `node:test`.

```typescript
import * as vscode from 'vscode';
import { secretKey } from './secretKey';

export async function getCredential(
  secrets: vscode.SecretStorage,
  profileName: string,
  field: string,
): Promise<string | undefined> {
  return secrets.get(secretKey(profileName, field));
}

export async function setCredential(
  secrets: vscode.SecretStorage,
  profileName: string,
  field: string,
  value: string,
): Promise<void> {
  await secrets.store(secretKey(profileName, field), value);
}

export async function deleteCredentials(
  secrets: vscode.SecretStorage,
  profileName: string,
  fields: string[],
): Promise<void> {
  for (const field of fields) {
    await secrets.delete(secretKey(profileName, field));
  }
}
```

- [ ] **Step 6: Write `src/connection/profileStore.ts`**

This reads connection profiles and thresholds from VS Code settings. It imports `vscode`, so it is exercised manually (Task 10) rather than via `node:test`. Invalid entries are reported via the `onError` callback (wired to the output channel in Task 10) and skipped.

```typescript
import * as vscode from 'vscode';
import { parseConnectionProfiles } from './profileValidation';
import { ConnectionProfile } from './types';

export interface Thresholds {
  warning: number;
  critical: number;
}

export function getConnectionProfiles(onError: (message: string) => void): ConnectionProfile[] {
  const raw = vscode.workspace.getConfiguration('kafkaLagMonitor').get('connections', []);
  const { profiles, errors } = parseConnectionProfiles(raw);
  for (const { index, errors: entryErrors } of errors) {
    onError(`kafkaLagMonitor.connections[${index}]: ${entryErrors.join('; ')}`);
  }
  return profiles;
}

export function getLagThresholds(): Thresholds {
  const config = vscode.workspace.getConfiguration('kafkaLagMonitor');
  return {
    warning: config.get('lagWarningThreshold', 100),
    critical: config.get('lagCriticalThreshold', 1000),
  };
}
```

- [ ] **Step 7: Compile**

Run: `npm run compile`
Expected: completes with no errors.

- [ ] **Step 8: Commit**

```bash
git add src/connection/secretKey.ts src/connection/secretStore.ts src/connection/profileStore.ts src/test/secretKey.test.ts
git commit -m "feat: add connection profile and secret storage"
```

---

### Task 10: Kafka Explorer tree view and extension wiring

**Files:**
- Create: `src/kafka/kafkaAdminAdapter.ts`
- Create: `src/treeView/kafkaExplorerProvider.ts`
- Modify: `package.json`
- Modify: `src/extension.ts`
- Create: `.vscode/settings.json`
- Create: `README.md`

This task wires everything built so far into a working tree view. It has no `node:test` coverage of its own (every file here imports either `vscode` or `kafkajs`'s network client) — it is verified manually against the local `kafka-orchestrator` cluster in Step 7.

- [ ] **Step 1: Create `src/kafka/kafkaAdminAdapter.ts`**

Adapts a real kafkajs `Admin` client to our `KafkaAdminClient` interface (Task 5). This is the only file that imports `kafkajs`'s `Admin`/`ConfigResourceTypes` types.

```typescript
import { Admin, ConfigResourceTypes } from 'kafkajs';
import { KafkaAdminClient } from './adminClient';

export function createKafkaAdminClient(admin: Admin): KafkaAdminClient {
  return {
    connect: () => admin.connect(),
    disconnect: () => admin.disconnect(),
    listTopics: () => admin.listTopics(),
    fetchTopicMetadata: (args) => admin.fetchTopicMetadata(args),
    describeConfigs: (args) =>
      admin.describeConfigs({
        resources: args.resources.map((r) => ({ type: r.type as ConfigResourceTypes, name: r.name })),
        includeSynonyms: args.includeSynonyms,
      }),
    listGroups: () => admin.listGroups(),
    fetchOffsets: (args) => admin.fetchOffsets({ groupId: args.groupId }),
    fetchTopicOffsets: (topic) => admin.fetchTopicOffsets(topic),
  };
}
```

- [ ] **Step 2: Create `src/treeView/kafkaExplorerProvider.ts`**

```typescript
import * as vscode from 'vscode';
import { ConnectionManager } from '../connection/connectionManager';
import { Thresholds } from '../connection/profileStore';
import { ConnectionProfile } from '../connection/types';
import { TopicSummary } from '../kafka/adminService';
import { lagSeverity, PartitionLag, TopicLag } from '../kafka/lag';
import { buildConnectionNode, buildGroupNode, buildPartitionNode, buildTopicNode } from './treeItems';

export type KafkaTreeNode =
  | { kind: 'connection'; profile: ConnectionProfile }
  | { kind: 'topicsFolder'; profile: ConnectionProfile }
  | { kind: 'groupsFolder'; profile: ConnectionProfile }
  | { kind: 'topic'; topic: TopicSummary }
  | { kind: 'group'; groupId: string; totalLag: number; topicLags: TopicLag[] }
  | { kind: 'groupTopic'; topicLag: TopicLag }
  | { kind: 'partition'; partitionLag: PartitionLag }
  | { kind: 'message'; text: string };

export class KafkaExplorerProvider implements vscode.TreeDataProvider<KafkaTreeNode> {
  private readonly _onDidChangeTreeData = new vscode.EventEmitter<KafkaTreeNode | undefined>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  constructor(
    private readonly profiles: ConnectionProfile[],
    private readonly connectionManager: ConnectionManager,
    private readonly thresholds: Thresholds,
  ) {}

  refresh(): void {
    this._onDidChangeTreeData.fire(undefined);
  }

  getTreeItem(element: KafkaTreeNode): vscode.TreeItem {
    switch (element.kind) {
      case 'connection': {
        const state = this.connectionManager.getState(element.profile.name);
        const view = buildConnectionNode(element.profile.name, state.status, state.error);
        const item = new vscode.TreeItem(view.label, vscode.TreeItemCollapsibleState.Collapsed);
        item.description = view.description;
        return item;
      }
      case 'topicsFolder':
        return new vscode.TreeItem('Topics', vscode.TreeItemCollapsibleState.Collapsed);
      case 'groupsFolder':
        return new vscode.TreeItem('Consumer Groups', vscode.TreeItemCollapsibleState.Collapsed);
      case 'topic': {
        const view = buildTopicNode(element.topic.name, element.topic.partitionCount);
        const item = new vscode.TreeItem(view.label, vscode.TreeItemCollapsibleState.None);
        item.description = view.description;
        return item;
      }
      case 'group': {
        const severity = lagSeverity(element.totalLag, this.thresholds.warning, this.thresholds.critical);
        const view = buildGroupNode(element.groupId, element.totalLag, severity);
        const item = new vscode.TreeItem(
          view.label,
          element.topicLags.length > 0
            ? vscode.TreeItemCollapsibleState.Collapsed
            : vscode.TreeItemCollapsibleState.None,
        );
        item.description = view.description;
        return item;
      }
      case 'groupTopic': {
        const item = new vscode.TreeItem(element.topicLag.topic, vscode.TreeItemCollapsibleState.Collapsed);
        item.description = `●${element.topicLag.totalLag}`;
        return item;
      }
      case 'partition': {
        const view = buildPartitionNode(
          element.partitionLag.partition,
          element.partitionLag.currentOffset,
          element.partitionLag.endOffset,
          element.partitionLag.lag,
        );
        return new vscode.TreeItem(view.label, vscode.TreeItemCollapsibleState.None);
      }
      case 'message':
        return new vscode.TreeItem(element.text, vscode.TreeItemCollapsibleState.None);
    }
  }

  async getChildren(element?: KafkaTreeNode): Promise<KafkaTreeNode[]> {
    if (!element) {
      return this.profiles.map((profile) => ({ kind: 'connection', profile }));
    }

    switch (element.kind) {
      case 'connection': {
        if (this.connectionManager.getState(element.profile.name).status === 'idle') {
          await this.connectionManager.connect(element.profile).catch(() => undefined);
        }
        return [
          { kind: 'topicsFolder', profile: element.profile },
          { kind: 'groupsFolder', profile: element.profile },
        ];
      }
      case 'topicsFolder': {
        const adminService = this.connectionManager.getAdminService(element.profile.name);
        if (!adminService) {
          return [
            { kind: 'message', text: this.connectionManager.getState(element.profile.name).error ?? 'Not connected' },
          ];
        }
        try {
          const topics = await adminService.listTopics();
          return topics.map((topic) => ({ kind: 'topic', topic }));
        } catch (err) {
          return [{ kind: 'message', text: (err as Error).message }];
        }
      }
      case 'groupsFolder': {
        const adminService = this.connectionManager.getAdminService(element.profile.name);
        if (!adminService) {
          return [
            { kind: 'message', text: this.connectionManager.getState(element.profile.name).error ?? 'Not connected' },
          ];
        }
        try {
          const groups = await adminService.listConsumerGroups();
          const nodes: KafkaTreeNode[] = [];
          for (const group of groups) {
            const topicLags = await adminService.getGroupLag(group.groupId);
            const totalLag = topicLags.reduce((sum, t) => sum + t.totalLag, 0);
            nodes.push({ kind: 'group', groupId: group.groupId, totalLag, topicLags });
          }
          return nodes;
        } catch (err) {
          return [{ kind: 'message', text: (err as Error).message }];
        }
      }
      case 'group':
        return element.topicLags.map((topicLag) => ({ kind: 'groupTopic', topicLag }));
      case 'groupTopic':
        return element.topicLag.partitions.map((partitionLag) => ({ kind: 'partition', partitionLag }));
      case 'topic':
      case 'partition':
      case 'message':
        return [];
    }
  }
}
```

- [ ] **Step 3: Add `contributes` to `package.json`**

Add this top-level key to `package.json` (sibling of `"scripts"`, `"dependencies"`, etc.):

```json
  "contributes": {
    "configuration": {
      "title": "Kafka Lag Monitor",
      "properties": {
        "kafkaLagMonitor.connections": {
          "type": "array",
          "default": [],
          "description": "Kafka cluster connection profiles. Each entry: { name, brokers: [\"host:port\", ...], sasl: null, ssl, clientId }. SASL is not yet supported."
        },
        "kafkaLagMonitor.lagWarningThreshold": {
          "type": "number",
          "default": 100,
          "description": "Total consumer group lag at or above which the lag badge turns yellow."
        },
        "kafkaLagMonitor.lagCriticalThreshold": {
          "type": "number",
          "default": 1000,
          "description": "Total consumer group lag at or above which the lag badge turns red."
        },
        "kafkaLagMonitor.pollIntervalSeconds": {
          "type": "number",
          "default": 10,
          "description": "Auto-refresh interval in seconds for the Lag Dashboard webview (not yet implemented)."
        }
      }
    },
    "views": {
      "explorer": [
        {
          "id": "kafkaLagMonitor.explorer",
          "name": "Kafka Lag Monitor"
        }
      ]
    },
    "commands": [
      {
        "command": "kafkaLagMonitor.refresh",
        "title": "Kafka Lag Monitor: Refresh",
        "icon": "$(refresh)"
      }
    ],
    "menus": {
      "view/title": [
        {
          "command": "kafkaLagMonitor.refresh",
          "when": "view == kafkaLagMonitor.explorer",
          "group": "navigation"
        }
      ]
    }
  }
```

- [ ] **Step 4: Replace `src/extension.ts`**

```typescript
import { Kafka } from 'kafkajs';
import * as vscode from 'vscode';
import { ConnectionManager } from './connection/connectionManager';
import { getConnectionProfiles, getLagThresholds } from './connection/profileStore';
import { createKafkaAdminClient } from './kafka/kafkaAdminAdapter';
import { createKafkaLogCreator } from './logging/kafkaLogCreator';
import { KafkaExplorerProvider } from './treeView/kafkaExplorerProvider';

export function activate(context: vscode.ExtensionContext): void {
  const output = vscode.window.createOutputChannel('Kafka Lag Monitor');
  output.appendLine('Kafka Lag Monitor activated');
  context.subscriptions.push(output);

  const connectionManager = new ConnectionManager((profile) => {
    if (profile.sasl) {
      throw new Error('SASL authentication is not supported yet. Use a PLAINTEXT or SSL-only connection.');
    }
    const kafka = new Kafka({
      clientId: profile.clientId,
      brokers: profile.brokers,
      ssl: profile.ssl,
      logCreator: createKafkaLogCreator((line) => output.appendLine(line)),
    });
    return createKafkaAdminClient(kafka.admin());
  });

  const profiles = getConnectionProfiles((message) => output.appendLine(`[CONFIG] ${message}`));
  const thresholds = getLagThresholds();

  const explorer = new KafkaExplorerProvider(profiles, connectionManager, thresholds);
  const treeView = vscode.window.createTreeView('kafkaLagMonitor.explorer', { treeDataProvider: explorer });
  context.subscriptions.push(treeView);

  context.subscriptions.push(vscode.commands.registerCommand('kafkaLagMonitor.refresh', () => explorer.refresh()));
}

export function deactivate(): void {}
```

- [ ] **Step 5: Compile**

Run: `npm run compile`
Expected: completes with no errors, `out/extension.js` updated.

- [ ] **Step 6: Run the full unit test suite**

Run: `npm test`
Expected: PASS — all 35 tests across `kafkaLogCreator`, `profileValidation`, `lag`, `adminService`, `connectionManager`, `treeItems`, and `secretKey` test files, 0 failures.

- [ ] **Step 7: Create `.vscode/settings.json` pointing at the local `kafka-orchestrator` cluster**

```json
{
  "kafkaLagMonitor.connections": [
    {
      "name": "local-cluster",
      "brokers": ["localhost:9091", "localhost:9092", "localhost:9095"],
      "sasl": null,
      "ssl": false,
      "clientId": "kafka-lag-monitor"
    }
  ]
}
```

- [ ] **Step 8: Manual verification in the Extension Development Host**

1. Make sure the local cluster is running: `cd ../kafka-orchestrator && docker compose up -d` (or however that cluster is started — check its README if `localhost:9091` isn't reachable).
2. Create a topic and generate some lag using the sibling `java-kafka-cli` project:
   ```bash
   cd ../java-kafka-cli
   ./bin/kafka-topics.sh --bootstrap-server localhost:9091 --create --topic orders.events --partitions 3 --replication-factor 1
   for i in 1 2 3 4 5; do echo "order-$i"; done | ./bin/kafka-console-producer.sh --bootstrap-server localhost:9091 --topic orders.events
   ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9091 --topic orders.events --group order-service --max-messages 2
   ```
   This produces 5 messages and lets `order-service` consume 2, leaving lag on the topic.
3. In VS Code, open this project folder and press `F5` (Run > Start Debugging) to launch the Extension Development Host.
4. In the new window, open the Explorer sidebar (`Ctrl+Shift+E`). Expect to see a **Kafka Lag Monitor** view containing `local-cluster ✓`.
5. Expand `local-cluster ✓` → expect **Topics** and **Consumer Groups** folders.
6. Expand **Topics** → expect `orders.events` with description `3 partitions`.
7. Expand **Consumer Groups** → expect `order-service` with a description like `●3` (5 produced − 2 consumed = 3 lag, distributed across partitions).
8. Expand `order-service` → `orders.events` → expect partition rows like `p0: 1/2 (1)` etc., summing to the group's total lag.
9. Click the refresh icon in the view title bar → tree reloads without error.
10. Stop the cluster or kill a broker, click refresh again → **Topics** and **Consumer Groups** each collapse to a single item showing the connection error message (e.g. a kafkajs `connect ECONNREFUSED ...` message), and the Output panel's "Kafka Lag Monitor" channel shows the underlying kafkajs error log lines. (`local-cluster` itself keeps its `✓` until the extension reloads — updating that badge on a failed refresh is handled by the **Kafka: Reconnect** command in the connection-management follow-up plan.)

- [ ] **Step 9: Write `README.md`**

```markdown
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
```

- [ ] **Step 10: Commit**

```bash
git add src/kafka/kafkaAdminAdapter.ts src/treeView/kafkaExplorerProvider.ts package.json src/extension.ts .vscode/settings.json README.md
git commit -m "feat: wire Kafka Explorer tree view into the extension"
```

---

## Next Plans

- **Connection management commands** — `Kafka: Add/Edit/Remove/Reconnect Connection` (quickinput wizard, SecretStorage-backed credentials, SASL support in the admin client factory).
- **Topic Metadata webview** — `renderTopicMetadataHtml` + `topicMetadataPanel`, opened from the `topic` tree node.
- **Lag Dashboard webview** — snapshot bars, summary cards, manual refresh, auto-poll via `pollingManager`.
- **Message Browser webview** — offset-window computation, ephemeral consumer, consumed/pending tagging.
- **Produce webview** — form + cached producer.

