# Lag Dashboard Webview Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish roadmap Phase 2 — a "Lag Dashboard" webview, opened by clicking a consumer group in the sidebar, showing total/per-topic/per-partition lag with summary cards and progress bars, manual refresh, and an auto-poll toggle backed by a new `PollingManager`.

**Architecture:** A new pure `PollingManager` (`src/polling/pollingManager.ts`) wraps `setInterval`/`clearInterval`. A new pure `src/webviews/lagDashboardPanel.ts` (mirrors `topicMetadataPanel.ts`) holds the `toDashboardData` aggregation function and `renderLagDashboardHtml` full-page renderer, reusing `escapeHtml`/`renderErrorHtml` from `topicMetadataPanel.ts`. A new `src/webviews/lagDashboardPanelController.ts` holds the singleton `LagDashboardPanel` vscode glue class (generation-guarded full render on `show()`, `postMessage`-based incremental updates for refresh/poll). `kafkaExplorerProvider.ts`'s `'group'` tree node gains a `profile` field and a click command. `extension.ts` registers the new `kafkaLagMonitor.showLagDashboard` command.

**Tech Stack:** TypeScript, vscode Extension API (WebviewPanel with `retainContextWhenHidden`, `postMessage`, `onDidChangeViewState`), kafkajs, node:test (including the experimental `mock.timers` API).

**Reference spec:** `docs/superpowers/specs/2026-06-14-lag-dashboard-design.md`

---

## Task 1: `PollingManager`

**Files:**
- Create: `src/polling/pollingManager.ts`
- Test: `src/test/pollingManager.test.ts`

- [ ] **Step 1: Write the failing tests**

Create `src/test/pollingManager.test.ts`:

```typescript
import assert from 'node:assert/strict';
import test, { mock } from 'node:test';
import { PollingManager } from '../polling/pollingManager';

test('start() invokes tick repeatedly at the given interval', () => {
  mock.timers.enable({ apis: ['setInterval'] });
  try {
    const manager = new PollingManager();
    let calls = 0;
    manager.start(1000, () => {
      calls++;
    });

    mock.timers.tick(1000);
    assert.equal(calls, 1);

    mock.timers.tick(2000);
    assert.equal(calls, 3);
  } finally {
    mock.timers.reset();
  }
});

test('stop() halts further ticks', () => {
  mock.timers.enable({ apis: ['setInterval'] });
  try {
    const manager = new PollingManager();
    let calls = 0;
    manager.start(1000, () => {
      calls++;
    });

    mock.timers.tick(1000);
    assert.equal(calls, 1);

    manager.stop();
    mock.timers.tick(5000);
    assert.equal(calls, 1);
  } finally {
    mock.timers.reset();
  }
});

test('isRunning() reflects whether a timer is active', () => {
  mock.timers.enable({ apis: ['setInterval'] });
  try {
    const manager = new PollingManager();
    assert.equal(manager.isRunning(), false);

    manager.start(1000, () => {});
    assert.equal(manager.isRunning(), true);

    manager.stop();
    assert.equal(manager.isRunning(), false);
  } finally {
    mock.timers.reset();
  }
});

test('start() while already running restarts cleanly without double-ticking', () => {
  mock.timers.enable({ apis: ['setInterval'] });
  try {
    const manager = new PollingManager();
    let calls = 0;
    manager.start(1000, () => {
      calls++;
    });

    mock.timers.tick(500);
    manager.start(1000, () => {
      calls++;
    });

    mock.timers.tick(1000);
    assert.equal(calls, 1);
  } finally {
    mock.timers.reset();
  }
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test`
Expected: FAIL — TypeScript compile error `Cannot find module '../polling/pollingManager'`.

- [ ] **Step 3: Create `src/polling/pollingManager.ts`**

```typescript
export class PollingManager {
  private timer: NodeJS.Timeout | undefined;

  start(intervalMs: number, tick: () => void): void {
    this.stop();
    this.timer = setInterval(tick, intervalMs);
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
  }

  isRunning(): boolean {
    return this.timer !== undefined;
  }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 56`, `# pass 56`, `# fail 0` (52 existing + 4 new).

- [ ] **Step 5: Commit**

```bash
git add src/polling/pollingManager.ts src/test/pollingManager.test.ts
git commit -m "feat: add PollingManager for the Lag Dashboard's auto-refresh timer"
```

---

## Task 2: `toDashboardData` aggregation

**Files:**
- Create: `src/webviews/lagDashboardPanel.ts`
- Test: `src/test/lagDashboardPanel.test.ts`

- [ ] **Step 1: Write the failing tests**

Create `src/test/lagDashboardPanel.test.ts`:

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { toDashboardData } from '../webviews/lagDashboardPanel';
import { TopicLag } from '../kafka/lag';
import { Thresholds } from '../connection/profileStore';

const thresholds: Thresholds = { warning: 100, critical: 1000 };

test('toDashboardData computes percentConsumed and severity for a partially-consumed partition', () => {
  const topicLags: TopicLag[] = [
    {
      topic: 'orders.events',
      totalLag: 199,
      partitions: [{ partition: 0, currentOffset: 401, endOffset: 600, lag: 199, status: 'lag' }],
    },
  ];

  const data = toDashboardData('order-service', topicLags, thresholds);

  assert.equal(data.topics[0].partitions[0].percentConsumed, 67);
  assert.equal(data.topics[0].partitions[0].severity, 'warning');
});

test('toDashboardData computes 100% and severity none for a fully-consumed partition', () => {
  const topicLags: TopicLag[] = [
    {
      topic: 'orders.events',
      totalLag: 0,
      partitions: [{ partition: 1, currentOffset: 600, endOffset: 600, lag: 0, status: 'ok' }],
    },
  ];

  const data = toDashboardData('order-service', topicLags, thresholds);

  assert.equal(data.topics[0].partitions[0].percentConsumed, 100);
  assert.equal(data.topics[0].partitions[0].severity, 'none');
});

test('toDashboardData computes 0% for a partition that has not started consuming', () => {
  const topicLags: TopicLag[] = [
    {
      topic: 'orders.events',
      totalLag: 600,
      partitions: [{ partition: 2, currentOffset: 0, endOffset: 600, lag: 600, status: 'not-started' }],
    },
  ];

  const data = toDashboardData('order-service', topicLags, thresholds);

  assert.equal(data.topics[0].partitions[0].percentConsumed, 0);
  assert.equal(data.topics[0].partitions[0].severity, 'warning');
});

test('toDashboardData treats an empty partition (endOffset 0) as 100% consumed', () => {
  const topicLags: TopicLag[] = [
    {
      topic: 'orders.events',
      totalLag: 0,
      partitions: [{ partition: 3, currentOffset: 0, endOffset: 0, lag: 0, status: 'ok' }],
    },
  ];

  const data = toDashboardData('order-service', topicLags, thresholds);

  assert.equal(data.topics[0].partitions[0].percentConsumed, 100);
  assert.equal(data.topics[0].partitions[0].severity, 'none');
});

test('toDashboardData applies severity boundaries per partition', () => {
  const topicLags: TopicLag[] = [
    {
      topic: 'orders.events',
      totalLag: 99 + 100 + 1000,
      partitions: [
        { partition: 0, currentOffset: 901, endOffset: 1000, lag: 99, status: 'lag' },
        { partition: 1, currentOffset: 900, endOffset: 1000, lag: 100, status: 'lag' },
        { partition: 2, currentOffset: 0, endOffset: 1000, lag: 1000, status: 'not-started' },
      ],
    },
  ];

  const data = toDashboardData('order-service', topicLags, thresholds);

  assert.equal(data.topics[0].partitions[0].severity, 'none');
  assert.equal(data.topics[0].partitions[1].severity, 'warning');
  assert.equal(data.topics[0].partitions[2].severity, 'critical');
});

test('toDashboardData sums totalLag across topics, counts over-threshold partitions, and derives overall severity', () => {
  const topicLags: TopicLag[] = [
    {
      topic: 'orders.events',
      totalLag: 199,
      partitions: [
        { partition: 0, currentOffset: 401, endOffset: 600, lag: 199, status: 'lag' },
        { partition: 1, currentOffset: 600, endOffset: 600, lag: 0, status: 'ok' },
      ],
    },
    {
      topic: 'payments.events',
      totalLag: 1000,
      partitions: [{ partition: 0, currentOffset: 0, endOffset: 1000, lag: 1000, status: 'not-started' }],
    },
  ];

  const data = toDashboardData('order-service', topicLags, thresholds);

  assert.equal(data.groupId, 'order-service');
  assert.equal(data.totalLag, 1199);
  assert.equal(data.severity, 'critical');
  assert.equal(data.overThresholdCount, 2);
  assert.equal(data.topics.length, 2);
  assert.equal(data.topics[0].topic, 'orders.events');
  assert.equal(data.topics[0].totalLag, 199);
  assert.equal(data.topics[1].topic, 'payments.events');
});

test('toDashboardData handles an empty topic list', () => {
  const data = toDashboardData('order-service', [], thresholds);

  assert.equal(data.totalLag, 0);
  assert.equal(data.severity, 'none');
  assert.equal(data.overThresholdCount, 0);
  assert.deepEqual(data.topics, []);
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test`
Expected: FAIL — TypeScript compile error `Cannot find module '../webviews/lagDashboardPanel'`.

- [ ] **Step 3: Create `src/webviews/lagDashboardPanel.ts`**

```typescript
import { LagSeverity, TopicLag, lagSeverity } from '../kafka/lag';
import { Thresholds } from '../connection/profileStore';

export interface PartitionLagView {
  partition: number;
  currentOffset: number;
  endOffset: number;
  lag: number;
  percentConsumed: number;
  severity: LagSeverity;
}

export interface TopicLagView {
  topic: string;
  totalLag: number;
  partitions: PartitionLagView[];
}

export interface LagDashboardData {
  groupId: string;
  totalLag: number;
  severity: LagSeverity;
  overThresholdCount: number;
  topics: TopicLagView[];
}

export function toDashboardData(groupId: string, topicLags: TopicLag[], thresholds: Thresholds): LagDashboardData {
  let totalLag = 0;
  let overThresholdCount = 0;

  const topics: TopicLagView[] = topicLags.map((topicLag) => {
    const partitions: PartitionLagView[] = topicLag.partitions.map((p) => {
      const percentConsumed = p.endOffset === 0 ? 100 : Math.round((p.currentOffset / p.endOffset) * 100);
      const severity = lagSeverity(p.lag, thresholds.warning, thresholds.critical);
      if (severity !== 'none') {
        overThresholdCount++;
      }
      totalLag += p.lag;
      return {
        partition: p.partition,
        currentOffset: p.currentOffset,
        endOffset: p.endOffset,
        lag: p.lag,
        percentConsumed,
        severity,
      };
    });
    return { topic: topicLag.topic, totalLag: topicLag.totalLag, partitions };
  });

  return {
    groupId,
    totalLag,
    severity: lagSeverity(totalLag, thresholds.warning, thresholds.critical),
    overThresholdCount,
    topics,
  };
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 63`, `# pass 63`, `# fail 0` (56 from Task 1, plus the 7 new tests above).

- [ ] **Step 5: Commit**

```bash
git add src/webviews/lagDashboardPanel.ts src/test/lagDashboardPanel.test.ts
git commit -m "feat: add toDashboardData aggregation for the Lag Dashboard"
```

---

## Task 3: `renderLagDashboardHtml`

**Files:**
- Modify: `src/webviews/lagDashboardPanel.ts`
- Modify: `src/test/lagDashboardPanel.test.ts`

- [ ] **Step 1: Write the failing tests**

In `src/test/lagDashboardPanel.test.ts`, change the import line from:

```typescript
import { toDashboardData } from '../webviews/lagDashboardPanel';
```

to:

```typescript
import { renderLagDashboardHtml, toDashboardData } from '../webviews/lagDashboardPanel';
```

Then append these tests at the end of the file:

```typescript

test('renderLagDashboardHtml includes the control element ids and the serialized initial data', () => {
  const data = toDashboardData('order-service', [], thresholds);
  const html = renderLagDashboardHtml('order-service', data, 10);

  assert.match(html, /id="groupTitle"/);
  assert.match(html, /id="refresh"/);
  assert.match(html, /id="autopoll"/);
  assert.match(html, /id="banner"/);
  assert.match(html, /id="totalLag"/);
  assert.match(html, /id="status"/);
  assert.match(html, /id="overThreshold"/);
  assert.match(html, /id="topics"/);
  assert.match(html, /<script>[\s\S]*const initialData = \{[\s\S]*"groupId":"order-service"[\s\S]*\}[\s\S]*<\/script>/);
});

test('renderLagDashboardHtml includes the poll interval in the auto-poll label', () => {
  const data = toDashboardData('order-service', [], thresholds);
  const html = renderLagDashboardHtml('order-service', data, 15);

  assert.match(html, /Auto-refresh every 15s/);
});

test('renderLagDashboardHtml wires the refresh button and autopoll checkbox to postMessage', () => {
  const data = toDashboardData('order-service', [], thresholds);
  const html = renderLagDashboardHtml('order-service', data, 10);

  assert.match(html, /postMessage\(\{\s*type:\s*'refresh'\s*\}\)/);
  assert.match(html, /postMessage\(\{\s*type:\s*'setAutoPoll'/);
});

test('renderLagDashboardHtml handles update and pollError messages with a reconnect hint', () => {
  const data = toDashboardData('order-service', [], thresholds);
  const html = renderLagDashboardHtml('order-service', data, 10);

  assert.match(html, /'update'/);
  assert.match(html, /'pollError'/);
  assert.match(html, /Kafka: Reconnect/);
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `npm test`
Expected: FAIL — TypeScript compile error, e.g. `Module '"../webviews/lagDashboardPanel"' has no exported member 'renderLagDashboardHtml'`.

- [ ] **Step 3: Add `renderLagDashboardHtml` to `src/webviews/lagDashboardPanel.ts`**

Change the import line at the top of `src/webviews/lagDashboardPanel.ts` from:

```typescript
import { LagSeverity, TopicLag, lagSeverity } from '../kafka/lag';
import { Thresholds } from '../connection/profileStore';
```

to:

```typescript
import { LagSeverity, TopicLag, lagSeverity } from '../kafka/lag';
import { Thresholds } from '../connection/profileStore';
import { escapeHtml } from './topicMetadataPanel';
```

Then append this function at the end of the file:

```typescript

export function renderLagDashboardHtml(groupId: string, data: LagDashboardData, pollIntervalSeconds: number): string {
  const safeGroupId = escapeHtml(groupId);
  const initialData = JSON.stringify(data);
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Lag: ${safeGroupId}</title>
<style>
  body { font-family: var(--vscode-font-family, sans-serif); padding: 0 16px; }
  .summary { display: flex; gap: 16px; margin: 12px 0; }
  .card { border: 1px solid var(--vscode-panel-border, #ccc); padding: 8px 12px; border-radius: 4px; }
  #banner { background: var(--vscode-inputValidation-warningBackground, #5a3d00); border: 1px solid var(--vscode-inputValidation-warningBorder, #b89500); padding: 8px; margin: 12px 0; }
  section { margin-bottom: 16px; }
  .partition-row { display: flex; align-items: center; gap: 8px; margin: 4px 0; }
  .partition-row span { min-width: 220px; }
  .bar { flex: 1; height: 10px; border: 1px solid var(--vscode-panel-border, #ccc); position: relative; }
  .bar[data-severity="warning"] { border-color: var(--vscode-editorWarning-foreground, #cca700); }
  .bar[data-severity="critical"] { border-color: var(--vscode-editorError-foreground, #f14c4c); }
  .bar-fill { height: 100%; background: var(--vscode-progressBar-background, #0e70c0); }
  .status-none { color: var(--vscode-foreground); }
  .status-warning { color: var(--vscode-editorWarning-foreground, #cca700); }
  .status-critical { color: var(--vscode-editorError-foreground, #f14c4c); }
</style>
</head>
<body>
<h2 id="groupTitle"></h2>
<button id="refresh">Refresh</button>
<label><input type="checkbox" id="autopoll"> Auto-refresh every ${pollIntervalSeconds}s</label>
<div id="banner" style="display:none"></div>
<div class="summary">
  <div class="card">Total Lag: <span id="totalLag"></span></div>
  <div class="card">Status: <span id="status"></span></div>
  <div class="card">Partitions over threshold: <span id="overThreshold"></span></div>
</div>
<div id="topics"></div>
<script>
  const vscode = acquireVsCodeApi();
  const initialData = ${initialData};

  function render(data) {
    document.getElementById('groupTitle').textContent = 'Lag: ' + data.groupId;
    document.getElementById('totalLag').textContent = String(data.totalLag);
    const status = document.getElementById('status');
    status.textContent = data.severity;
    status.className = 'status-' + data.severity;
    document.getElementById('overThreshold').textContent = String(data.overThresholdCount);

    const topicsEl = document.getElementById('topics');
    topicsEl.textContent = '';

    if (data.topics.length === 0) {
      const p = document.createElement('p');
      p.textContent = 'No topic lag data.';
      topicsEl.appendChild(p);
      return;
    }

    for (const topic of data.topics) {
      const section = document.createElement('section');
      const h3 = document.createElement('h3');
      h3.textContent = topic.topic + ' — total lag: ' + topic.totalLag;
      section.appendChild(h3);

      for (const partition of topic.partitions) {
        const row = document.createElement('div');
        row.className = 'partition-row';

        const label = document.createElement('span');
        label.textContent = 'p' + partition.partition + ': ' + partition.currentOffset + '/' + partition.endOffset + ' (lag ' + partition.lag + ')';
        row.appendChild(label);

        const bar = document.createElement('div');
        bar.className = 'bar';
        bar.dataset.severity = partition.severity;

        const fill = document.createElement('div');
        fill.className = 'bar-fill';
        fill.style.width = partition.percentConsumed + '%';
        bar.appendChild(fill);

        row.appendChild(bar);
        section.appendChild(row);
      }

      topicsEl.appendChild(section);
    }
  }

  render(initialData);

  window.addEventListener('message', (event) => {
    const message = event.data;
    if (message.type === 'update') {
      document.getElementById('banner').style.display = 'none';
      render(message.data);
    } else if (message.type === 'pollError') {
      const banner = document.getElementById('banner');
      banner.textContent = message.showReconnectHint
        ? message.message + ' Use Kafka: Reconnect on this connection in the sidebar.'
        : message.message;
      banner.style.display = 'block';
    }
  });

  document.getElementById('refresh').addEventListener('click', () => {
    vscode.postMessage({ type: 'refresh' });
  });

  document.getElementById('autopoll').addEventListener('change', (event) => {
    vscode.postMessage({ type: 'setAutoPoll', enabled: event.target.checked });
  });
</script>
</body>
</html>`;
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `npm test 2>&1 | tail -8`
Expected: PASS — `# tests 67`, `# pass 67`, `# fail 0` (63 from Task 2, plus the 4 new tests above).

- [ ] **Step 5: Commit**

```bash
git add src/webviews/lagDashboardPanel.ts src/test/lagDashboardPanel.test.ts
git commit -m "feat: add renderLagDashboardHtml for the Lag Dashboard webview"
```

---

## Task 4: `LagDashboardPanel` controller

**Files:**
- Create: `src/webviews/lagDashboardPanelController.ts`

No new unit tests — `LagDashboardPanel` is a vscode `WebviewPanel` glue class, matching the established compile-only treatment of `topicMetadataPanelController.ts`. The pure functions it calls are already covered by Tasks 2 and 3.

- [ ] **Step 1: Create `src/webviews/lagDashboardPanelController.ts`**

```typescript
import * as vscode from 'vscode';
import { ConnectionManager } from '../connection/connectionManager';
import { Thresholds } from '../connection/profileStore';
import { ConnectionProfile } from '../connection/types';
import { PollingManager } from '../polling/pollingManager';
import { renderLagDashboardHtml, toDashboardData } from './lagDashboardPanel';
import { renderErrorHtml } from './topicMetadataPanel';

export class LagDashboardPanel {
  private static currentPanel: LagDashboardPanel | undefined;

  private profileName = '';
  private groupId = '';
  private generation = 0;
  private autoPollEnabled = false;
  private consecutiveFailures = 0;
  private readonly polling = new PollingManager();

  private constructor(
    private readonly panel: vscode.WebviewPanel,
    private readonly connectionManager: ConnectionManager,
    private readonly thresholds: Thresholds,
    private readonly pollIntervalSeconds: number,
  ) {
    this.panel.webview.onDidReceiveMessage((message: { type: string; enabled?: boolean }) => {
      if (message.type === 'refresh') void this.refresh();
      else if (message.type === 'setAutoPoll') this.setAutoPoll(message.enabled === true);
    });
    this.panel.onDidChangeViewState((e) => {
      if (!e.webviewPanel.visible) this.polling.stop();
      else if (this.autoPollEnabled) this.polling.start(this.pollIntervalSeconds * 1000, () => this.pollTick());
    });
    this.panel.onDidDispose(() => {
      this.polling.stop();
      LagDashboardPanel.currentPanel = undefined;
    });
  }

  static async show(
    connectionManager: ConnectionManager,
    profile: ConnectionProfile,
    groupId: string,
    thresholds: Thresholds,
    pollIntervalSeconds: number,
  ): Promise<void> {
    let instance = LagDashboardPanel.currentPanel;
    if (instance) {
      instance.panel.reveal();
    } else {
      const panel = vscode.window.createWebviewPanel('kafkaLagDashboard', 'Lag Dashboard', vscode.ViewColumn.Active, {
        enableScripts: true,
        retainContextWhenHidden: true,
      });
      instance = new LagDashboardPanel(panel, connectionManager, thresholds, pollIntervalSeconds);
      LagDashboardPanel.currentPanel = instance;
    }
    instance.panel.title = `Lag: ${groupId}`;
    instance.profileName = profile.name;
    instance.groupId = groupId;
    instance.polling.stop();
    instance.autoPollEnabled = false;
    instance.consecutiveFailures = 0;
    await instance.renderFull();
  }

  private async renderFull(): Promise<void> {
    const gen = ++this.generation;
    const adminService = this.connectionManager.getAdminService(this.profileName);
    if (!adminService) {
      if (gen !== this.generation) return;
      this.panel.webview.html = renderErrorHtml('Not connected — expand the connection in the sidebar first.');
      return;
    }
    try {
      const topicLags = await adminService.getGroupLag(this.groupId);
      if (gen !== this.generation) return;
      const data = toDashboardData(this.groupId, topicLags, this.thresholds);
      this.panel.webview.html = renderLagDashboardHtml(this.groupId, data, this.pollIntervalSeconds);
    } catch (err) {
      if (gen !== this.generation) return;
      this.panel.webview.html = renderErrorHtml((err as Error).message);
    }
  }

  private async refresh(): Promise<void> {
    const gen = this.generation;
    const adminService = this.connectionManager.getAdminService(this.profileName);
    if (!adminService) return;
    try {
      const topicLags = await adminService.getGroupLag(this.groupId);
      if (gen !== this.generation) return;
      this.consecutiveFailures = 0;
      const data = toDashboardData(this.groupId, topicLags, this.thresholds);
      void this.panel.webview.postMessage({ type: 'update', data });
    } catch (err) {
      if (gen !== this.generation) return;
      this.consecutiveFailures++;
      void this.panel.webview.postMessage({
        type: 'pollError',
        message: (err as Error).message,
        showReconnectHint: this.consecutiveFailures >= 3,
      });
    }
  }

  private pollTick(): void {
    void this.refresh();
  }

  private setAutoPoll(enabled: boolean): void {
    this.autoPollEnabled = enabled;
    if (enabled) {
      this.consecutiveFailures = 0;
      this.polling.start(this.pollIntervalSeconds * 1000, () => this.pollTick());
    } else {
      this.polling.stop();
    }
  }
}
```

- [ ] **Step 2: Run the tests to verify nothing broke**

Run: `npm run compile && npm test 2>&1 | tail -8`
Expected: compile succeeds; `# tests 67`, `# pass 67`, `# fail 0` (unchanged from Task 3 — this task adds no new tests).

- [ ] **Step 3: Commit**

```bash
git add src/webviews/lagDashboardPanelController.ts
git commit -m "feat: add LagDashboardPanel controller with auto-poll and postMessage updates"
```

---

## Task 5: Tree view wiring — clicking a consumer group opens the dashboard

**Files:**
- Modify: `src/treeView/kafkaExplorerProvider.ts`

No new unit tests — `src/test/treeItems.test.ts` is unaffected (the `'group'` node's new `profile` field and `item.command` are additive; existing assertions on label/description/collapsible state still hold).

- [ ] **Step 1: Give the `'group'` tree node a `profile` field**

In `src/treeView/kafkaExplorerProvider.ts`, change the `KafkaTreeNode` union's `'group'` variant (line 14) from:

```typescript
  | { kind: 'group'; groupId: string; totalLag: number; topicLags: TopicLag[] }
```

to:

```typescript
  | { kind: 'group'; groupId: string; totalLag: number; topicLags: TopicLag[]; profile: ConnectionProfile }
```

- [ ] **Step 2: Pass `profile` when constructing `'group'` nodes**

In `getChildren`'s `'groupsFolder'` case, change:

```typescript
            nodes.push({ kind: 'group', groupId: group.groupId, totalLag, topicLags });
```

to:

```typescript
            nodes.push({ kind: 'group', groupId: group.groupId, totalLag, topicLags, profile: element.profile });
```

- [ ] **Step 3: Open the Lag Dashboard when a `'group'` node is clicked**

In `getTreeItem`'s `'group'` case, change:

```typescript
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
```

to:

```typescript
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
        item.command = {
          command: 'kafkaLagMonitor.showLagDashboard',
          title: 'Show Lag Dashboard',
          arguments: [element.profile, element.groupId],
        };
        return item;
      }
```

- [ ] **Step 4: Run the tests to verify nothing broke**

Run: `npm run compile && npm test 2>&1 | tail -8`
Expected: compile succeeds; `# tests 67`, `# pass 67`, `# fail 0` (unchanged — this task adds no new tests).

- [ ] **Step 5: Commit**

```bash
git add src/treeView/kafkaExplorerProvider.ts
git commit -m "feat: open the Lag Dashboard when a consumer group is clicked in the sidebar"
```

---

## Task 6: Register the command and wire up settings

**Files:**
- Modify: `src/extension.ts`
- Modify: `package.json:51`

No new unit tests — command registration is vscode glue, matching the established compile-only treatment of `extension.ts`.

- [ ] **Step 1: Import `LagDashboardPanel` in `extension.ts`**

In `src/extension.ts`, change the import block from:

```typescript
import { KafkaExplorerProvider } from './treeView/kafkaExplorerProvider';
import { TopicMetadataPanel } from './webviews/topicMetadataPanelController';
```

to:

```typescript
import { KafkaExplorerProvider } from './treeView/kafkaExplorerProvider';
import { LagDashboardPanel } from './webviews/lagDashboardPanelController';
import { TopicMetadataPanel } from './webviews/topicMetadataPanelController';
```

- [ ] **Step 2: Register `kafkaLagMonitor.showLagDashboard`**

In `src/extension.ts`, after the existing `kafkaLagMonitor.showTopicMetadata` registration:

```typescript
  context.subscriptions.push(
    vscode.commands.registerCommand(
      'kafkaLagMonitor.showTopicMetadata',
      async (profile: ConnectionProfile, topicName: string) => {
        await TopicMetadataPanel.show(connectionManager, profile.name, topicName);
      },
    ),
  );
}
```

add a new registration so the end of `activate()` reads:

```typescript
  context.subscriptions.push(
    vscode.commands.registerCommand(
      'kafkaLagMonitor.showTopicMetadata',
      async (profile: ConnectionProfile, topicName: string) => {
        await TopicMetadataPanel.show(connectionManager, profile.name, topicName);
      },
    ),
  );

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

- [ ] **Step 3: Mark `pollIntervalSeconds` as implemented in `package.json`**

In `package.json`, find the `kafkaLagMonitor.pollIntervalSeconds` property and change its `description` from:

```json
          "description": "Auto-refresh interval in seconds for the Lag Dashboard webview (not yet implemented)."
```

to:

```json
          "description": "Auto-refresh interval in seconds for the Lag Dashboard webview's auto-poll toggle."
```

- [ ] **Step 4: Verify compile, tests, and package.json validity**

Run: `npm run compile && npm test 2>&1 | tail -8`
Expected: compile succeeds; `# tests 67`, `# pass 67`, `# fail 0`.

Run: `node -e "JSON.parse(require('fs').readFileSync('package.json','utf8')); console.log('valid json')"`
Expected: `valid json`

- [ ] **Step 5: Commit**

```bash
git add src/extension.ts package.json
git commit -m "feat: register the Lag Dashboard command and enable the auto-poll interval setting"
```

---

## Task 7: Update README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Describe the Lag Dashboard in the "Status" section**

In `README.md`, replace the "Status" section paragraph (lines 9-19):

```markdown
**Phase 1 (this version):** an Explorer view showing, per configured
connection, the list of topics (with partition counts) and consumer groups
(with total lag and per-partition breakdown). Connections are managed with
the **Kafka: Add Connection** command (the `+` icon in the Explorer view
title bar) and the **Kafka: Edit Connection**, **Kafka: Remove Connection**,
and **Kafka: Reconnect** commands (right-click a connection), backed by VS
Code settings and SecretStorage. Clicking a topic
opens a Topic Metadata webview showing its partitions (leader, replicas, ISR)
and configuration. The Lag Dashboard, Message Browser, and Produce webviews
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
`kafkaLagMonitor.pollIntervalSeconds`). Message Browser and Produce webviews
are planned in follow-up phases (see
`docs/superpowers/specs/2026-06-13-kafka-lag-monitor-design.md`).
```

- [ ] **Step 2: Add a Lag Dashboard verification step to the manual integration test**

In `README.md`, replace the final paragraph of the "Manual integration test" section:

```markdown
Then `F5` the extension and expand `local-cluster` in the Explorer sidebar —
`orders.events` should show 3 partitions, and `order-service` should show a
total lag of 3.
```

with:

```markdown
Then `F5` the extension and expand `local-cluster` in the Explorer sidebar —
`orders.events` should show 3 partitions, and `order-service` should show a
total lag of 3. Clicking `order-service` opens the Lag Dashboard, which should
show a Total Lag of 3 with one `orders.events` section and per-partition
progress bars.
```

- [ ] **Step 3: Final verification**

Run: `npm run compile && npm test 2>&1 | tail -8`
Expected: compile succeeds; `# tests 67`, `# pass 67`, `# fail 0`.

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: document the Lag Dashboard webview and auto-poll setting"
```
