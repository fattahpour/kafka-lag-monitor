# Lag Dashboard Webview — Design

## Overview

Implements roadmap Phase 2 (see `docs/superpowers/specs/2026-06-13-kafka-lag-monitor-design.md`, "Phasing / Roadmap" item 2) on top of the merged Phase 1 foundation. Adds a "Lag Dashboard" webview, opened by clicking a consumer group in the sidebar, showing per-topic/per-partition lag with summary cards, color-coded progress bars, manual refresh, and an auto-poll toggle backed by a new `PollingManager`.

## Goals

- Clicking a consumer group node in the sidebar opens a "Lag Dashboard" webview for that group (singleton panel, like Topic Metadata — switching groups retitles/re-renders the same panel).
- Dashboard shows: total lag, overall severity (none/warning/critical via existing `lagSeverity`), count of partitions over the warning threshold, and one section per topic the group consumes, each with a horizontal progress bar per partition (consumed vs. lag, colored by per-partition severity).
- Manual "Refresh" button and an "Auto-refresh every Ns" checkbox (`N` = `kafkaLagMonitor.pollIntervalSeconds`). Refresh and poll ticks update the webview via `postMessage` (no full page reload/flicker).
- A failed poll tick shows an inline warning banner but does not stop the timer; after 3 consecutive failures the banner additionally suggests using `Kafka: Reconnect`.
- `kafkaLagMonitor.pollIntervalSeconds` setting (already declared in `package.json`, marked "not yet implemented") becomes live.
- `README.md` "Status" section updated: Lag Dashboard moves from "planned in follow-up phases" to implemented, with a short description; manual integration test section gains a Lag Dashboard verification step.

## Non-Goals

- Message Browser, Produce webviews (later roadmap phases).
- Per-connection or per-group persistence of the auto-poll toggle — it always starts off and resets to off when switching groups or reopening the panel.
- A clickable "Reconnect" button inside the dashboard — the 3-failure banner is text-only, pointing at the existing sidebar `Kafka: Reconnect` command.
- Reusing `getGroupLag`'s totals for the tree's existing lag badges — the dashboard recomputes via its own `toDashboardData`, no changes to `kafkaExplorerProvider`'s existing badge logic beyond adding `profile` to the `'group'` node.

## Architecture & Components

### `PollingManager` — `src/polling/pollingManager.ts` (new)

Pure, unit-tested (no vscode import):

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

`start()` always stops any existing timer first (idempotent restart). One instance per `LagDashboardPanel`.

### Lag Dashboard data + render — `src/webviews/lagDashboardPanel.ts` (new)

Pure, unit-tested (no vscode import), mirrors `topicMetadataPanel.ts`. Imports `LagSeverity`, `TopicLag`, `lagSeverity` from `../kafka/lag` and `Thresholds` from `../connection/profileStore`. Reuses `escapeHtml`/`renderErrorHtml` from `./topicMetadataPanel` (import, no new shared module).

**Types:**

```typescript
export interface PartitionLagView {
  partition: number;
  currentOffset: number;
  endOffset: number;
  lag: number;
  percentConsumed: number; // 0-100
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
```

**`toDashboardData(groupId: string, topicLags: TopicLag[], thresholds: Thresholds): LagDashboardData`**

- For each partition: `percentConsumed = endOffset === 0 ? 100 : Math.round((currentOffset / endOffset) * 100)`; `severity = lagSeverity(lag, thresholds.warning, thresholds.critical)`.
- `totalLag = sum of all partition.lag across all topics` (same as `kafkaExplorerProvider`'s existing `topicLags.reduce((sum, t) => sum + t.totalLag, 0)`).
- `severity = lagSeverity(totalLag, thresholds.warning, thresholds.critical)`.
- `overThresholdCount = count of partitions where severity !== 'none'` across all topics.
- `topics`: one `TopicLagView` per input `TopicLag`, in the same order.

**`renderLagDashboardHtml(groupId: string, data: LagDashboardData, pollIntervalSeconds: number): string`**

Full HTML document:

```html
<h2 id="groupTitle"></h2>
<button id="refresh">Refresh</button>
<label><input type="checkbox" id="autopoll"> Auto-refresh every {pollIntervalSeconds}s</label>
<div id="banner" style="display:none"></div>
<div class="summary">
  <div class="card">Total Lag: <span id="totalLag"></span></div>
  <div class="card">Status: <span id="status"></span></div>
  <div class="card">Partitions over threshold: <span id="overThreshold"></span></div>
</div>
<div id="topics"></div>
```

Plus an inline `<script>`:
- `const initialData = {JSON-serialized LagDashboardData};`
- `const vscode = acquireVsCodeApi();`
- `function render(data) { ... }` — clears and rebuilds `#groupTitle`, `#totalLag`, `#status`, `#overThreshold`, and `#topics` using `document.createElement`/`textContent` only (no `innerHTML`, inherently safe from injection — no `escapeHtml` needed client-side).
  - Per topic: a `<section>` with an `<h3>` (`"{topic} — total lag: {totalLag}"`) and one `.partition-row` per partition: a label span (`"p{partition}: {currentOffset}/{endOffset} (lag {lag})"`) + a `.bar` div containing a `.bar-fill` div with `style.width = "{percentConsumed}%"`. `.bar` gets `dataset.severity = partition.severity`, used by CSS to color the unfilled (lag) portion via `--vscode-editorWarning-foreground` (warning) / `--vscode-editorError-foreground` (critical) / default border color (none).
  - `#topics` empty → renders a single `<p>No topic lag data.</p>`.
  - `#status` text = `data.severity` (`none`/`warning`/`critical`), with a CSS class for color.
- `render(initialData)` called on load.
- `window.addEventListener('message', (event) => { ... })`:
  - `{type: 'update', data}` → hide `#banner`, `render(data)`.
  - `{type: 'pollError', message, showReconnectHint}` → show `#banner` with `message` (+ `" Use Kafka: Reconnect on this connection in the sidebar."` appended if `showReconnectHint`).
- `#refresh` click → `vscode.postMessage({type: 'refresh'})`.
- `#autopoll` change → `vscode.postMessage({type: 'setAutoPoll', enabled: checkbox.checked})`.

### Controller — `src/webviews/lagDashboardPanelController.ts` (new)

vscode glue (compile-only, no unit tests — matches `topicMetadataPanelController.ts` precedent). Singleton panel, same shape as `TopicMetadataPanel`:

```typescript
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
      else if (this.autoPollEnabled) this.polling.start(this.pollIntervalSeconds * 1000, () => void this.pollTick());
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

- `renderFull()` is used only by `show()` (initial open / group switch) — full `webview.html` re-render, which naturally resets the checkbox to unchecked (matches `autoPollEnabled = false` reset in `show()`).
- `refresh()` is used by both the manual refresh button and poll ticks — `postMessage` only, no `webview.html` reassignment.
- `generation` guards both paths against races (e.g., user switches group while a `refresh()` from the old group is in flight).
- `retainContextWhenHidden: true` keeps the webview's DOM/JS (and thus checkbox state + last-rendered data) alive across tab hide/show, so no resync is needed when `onDidChangeViewState` restarts polling on becoming visible again.

### Tree view wiring — `src/treeView/kafkaExplorerProvider.ts`

- `KafkaTreeNode`'s `'group'` variant gains `profile: ConnectionProfile`:
  ```typescript
  | { kind: 'group'; groupId: string; totalLag: number; topicLags: TopicLag[]; profile: ConnectionProfile }
  ```
- `getChildren`'s `'groupsFolder'` case passes `profile: element.profile` when constructing each `'group'` node (the `groupsFolder` node already carries `profile`).
- `getTreeItem`'s `'group'` case adds:
  ```typescript
  item.command = {
    command: 'kafkaLagMonitor.showLagDashboard',
    title: 'Show Lag Dashboard',
    arguments: [element.profile, element.groupId],
  };
  ```
  This does not change `item.collapsibleState` — the node remains expandable (chevron) for its existing `groupTopic`/`partition` children, while the row itself opens the dashboard (same coexistence pattern as VS Code's built-in tree views).

### `src/extension.ts`

- New import: `import { LagDashboardPanel } from './webviews/lagDashboardPanelController';`
- After the existing `kafkaLagMonitor.showTopicMetadata` registration:
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
  ```
  `thresholds` is the existing `Thresholds` object already constructed via `getLagThresholds()` for the `KafkaExplorerProvider`.

### `package.json`

- New entry under `contributes.commands`: `kafkaLagMonitor.showLagDashboard` is **not** added — same precedent as `showTopicMetadata` (invoked only via the tree item's `command`, not meaningful from the Command Palette without a selected group).
- `kafkaLagMonitor.pollIntervalSeconds` description updated to drop "(not yet implemented)":
  ```jsonc
  "description": "Auto-refresh interval in seconds for the Lag Dashboard webview's auto-poll toggle."
  ```

### `README.md`

- "Status" section: replace "The Lag Dashboard, Message Browser, and Produce webviews are planned in follow-up phases" with a sentence describing the implemented Lag Dashboard (consumer group click → summary cards + per-partition progress bars, manual refresh, auto-poll toggle), and adjust the remaining-phases list to "Message Browser and Produce webviews are planned in follow-up phases".
- "Manual integration test" section: after the existing `kafka-console-consumer.sh` step, add a step noting that clicking `order-service` in the Explorer sidebar opens the Lag Dashboard, which should show total lag matching the sidebar badge (3) with one `orders.events` section and per-partition bars.

## Error Handling

- Not connected (no `AdminService` for the profile) on `show()`: `renderErrorHtml('Not connected — expand the connection in the sidebar first.')`, same wording/pattern as Topic Metadata.
- `getGroupLag` failure on initial `show()`: `renderErrorHtml(err.message)` — full error page, same as Topic Metadata.
- `getGroupLag` failure on `refresh()` (manual or poll): `{type: 'pollError', message, showReconnectHint}` banner; timer is **not** stopped (per `docs/superpowers/specs/2026-06-13-kafka-lag-monitor-design.md` "Polling & Refresh": "A failed poll tick ... does not stop the timer"). `showReconnectHint` becomes `true` once `consecutiveFailures >= 3`; resets to `0` on the next successful `refresh()`.
- Manual refresh while not connected (admin service becomes unavailable mid-session, e.g. after a disconnect): `refresh()` returns early without posting a message — banner state is left as-is. (Edge case: if this happens, the next reconnect + manual refresh resyncs normally; no special UI needed since the underlying connection-level error already surfaces in the tree.)

## Testing Strategy

Unit-tested (pure logic, `node:test`):
- `src/test/pollingManager.test.ts` (new) — `start()` invokes `tick` repeatedly at the given interval (using `node:test`'s `mock.timers`); `stop()` halts further ticks; `isRunning()` reflects state; calling `start()` while already running restarts cleanly (no double-ticking).
- `src/test/lagDashboardPanel.test.ts` (new):
  - `toDashboardData`: per-partition `severity` at and around the warning/critical boundaries; `percentConsumed` for normal (`current < end`), fully-consumed (`current === end`), not-started (`current === 0, end > 0`), and empty-topic (`end === 0`) cases; `totalLag` sums correctly across multiple topics; `overThresholdCount` counts only `warning`/`critical` partitions across all topics; overall `severity` derived from `totalLag`.
  - `renderLagDashboardHtml`: output contains the `groupId`, a `<script>` tag with the serialized `initialData`, and the `#totalLag`/`#status`/`#overThreshold`/`#topics`/`#refresh`/`#autopoll`/`#banner` element ids.

Compile-only (vscode glue, no unit tests — matches `topicMetadataPanelController.ts` precedent):
- `src/polling` usage inside `lagDashboardPanelController.ts`, `lagDashboardPanelController.ts` itself, `extension.ts` command registration, `package.json` wiring.

Existing tests unaffected: `src/test/treeItems.test.ts` and `src/test/lag.test.ts` are not modified (the `'group'` node's new `profile` field and `item.command` are additive; existing assertions on label/description/collapsible state still hold).
