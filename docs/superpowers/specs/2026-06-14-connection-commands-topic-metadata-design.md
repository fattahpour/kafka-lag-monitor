# Connection Commands + Topic Metadata Webview — Design

## Overview

Finishes roadmap Phase 1 (see `docs/superpowers/specs/2026-06-13-kafka-lag-monitor-design.md`, "Phasing / Roadmap" item 1) on top of the merged Phase 1 Foundation. Adds the connection-management commands (Add/Edit/Remove/Reconnect), wires SASL credentials end-to-end into the kafkajs client, hardens `ConnectionManager` against connect/reconnect races, and adds the Topic Metadata webview reachable by clicking a topic in the sidebar.

## Goals

- `Kafka: Add Connection` / `Edit Connection` / `Remove Connection` commands backed by a QuickInput wizard, persisting to `kafkaLagMonitor.connections` (Global settings) and `SecretStorage`.
- `Kafka: Reconnect` command that re-creates a connection's cached client.
- SASL (PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512) connections work end-to-end: credentials collected by the wizard, stored in `SecretStorage`, read back and passed to kafkajs on connect.
- `ConnectionManager` generation-counter guard so a stale `connect()` resolution can never clobber a newer `reconnect()`'s state.
- Clicking a topic in the sidebar opens a "Topic Metadata" webview (partitions table + config table), reusing a single panel.

## Non-Goals

- mTLS / SSL key passphrase support (current `ConnectionProfile.ssl` is a boolean; no client-cert fields exist or are added here).
- Lag Dashboard, Message Browser, Produce webviews (later roadmap phases).
- Wiring `LagSeverity` into tree item icons (carried-over M1 from prior review — separate small follow-up, not blocking this phase).

## Architecture & Components

### Connection profile persistence — `src/connection/connectionStore.ts` (new)

- `saveConnectionProfiles(profiles: ConnectionProfile[]): Promise<void>` — `vscode.workspace.getConfiguration('kafkaLagMonitor').update('connections', profiles, vscode.ConfigurationTarget.Global)`. Global scope: connection profiles are available across all workspaces, matching typical cluster-connection-manager UX (DB extensions, etc).
- vscode glue, compile-only (no unit tests), matching `profileStore.ts`/`secretStore.ts` pattern.

### Connection wizard — `src/connection/connectionWizard.ts` (new)

Pure, unit-tested validators (no vscode import):
- `validateProfileName(name: string, existingNames: string[]): string | null` — returns an error string or `null` if valid. Rejects empty, names containing `.` (avoids `secretKey()` collision — fixes carried-over M5), and duplicates of `existingNames`.
- `parseBrokerList(input: string): { brokers: string[]; errors: string[] }` — splits on `,`, trims each entry, validates against the existing `BROKER_PATTERN` regex from `profileValidation.ts` (currently module-private; export it so `connectionWizard.ts` can reuse it without duplicating the pattern).

vscode glue (compile-only): multi-step wizard using `vscode.window.showQuickPick` / `showInputBox`:

1. Name (`showInputBox`, `validateInput: validateProfileName`)
2. Brokers (`showInputBox`, comma-separated, `validateInput` via `parseBrokerList`)
3. SSL — QuickPick Yes/No
4. Auth type — QuickPick: None / PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512
5. If auth ≠ None: Username (`showInputBox`), Password (`showInputBox({password: true})`)
6. Client ID (`showInputBox`, optional, default `kafka-lag-monitor`)

On completion, builds a raw object and runs it through the existing `validateProfile()` (from `profileValidation.ts`) before saving — reusing the same validation the config loader uses, so wizard output and `settings.json`-edited output are held to the same standard.

**Add flow**: append new profile to the array from `getConnectionProfiles()`, `saveConnectionProfiles(...)`; if SASL chosen, `setCredential(secrets, name, 'username', ...)` + `setCredential(secrets, name, 'password', ...)`.

**Edit flow**: QuickPick over existing profile names → pre-fill wizard steps with the selected profile's current values. Username/Password steps: **blank input = keep existing stored secret** (no `setCredential` call); non-blank input overwrites it. Replace the profile in the array (by name) and save.

**Remove flow**: QuickPick over existing profile names → `vscode.window.showWarningMessage(\`Remove connection "${name}" and its stored credentials?\`, {modal: true}, 'Remove')` → on confirm: `connectionManager.disconnect(name)`, `deleteCredentials(secrets, name, ['username', 'password'])`, remove from array, save.

**Reconnect flow**: QuickPick over existing profile names (label shows current `STATUS_ICONS` status) → `connectionManager.reconnect(profile)`.

All four flows call `explorer.refresh()` on completion.

### SASL wiring — `src/extension.ts`

The `AdminClientFactory` passed to `ConnectionManager` becomes `(profile: ConnectionProfile) => Promise<KafkaAdminClient>` (credential lookup from `SecretStorage` is async). The factory:

```ts
const sasl = profile.sasl
  ? {
      mechanism: profile.sasl.mechanism,
      username: (await getCredential(context.secrets, profile.name, 'username')) ?? '',
      password: (await getCredential(context.secrets, profile.name, 'password')) ?? '',
    }
  : undefined;
const kafka = new Kafka({
  clientId: profile.clientId,
  brokers: profile.brokers,
  ssl: profile.ssl,
  sasl,
  logCreator: createKafkaLogCreator((line) => output.appendLine(line)),
});
return createKafkaAdminClient(kafka.admin());
```

The previous `if (profile.sasl) throw new Error('SASL authentication is not supported yet...')` is removed.

### `ConnectionManager` — generation-counter guard + `reconnect()`

`src/connection/connectionManager.ts` adds a `generations: Map<string, number>` and two private helpers:

```ts
private nextGeneration(profileName: string): number {
  const gen = (this.generations.get(profileName) ?? 0) + 1;
  this.generations.set(profileName, gen);
  return gen;
}

private isCurrentGeneration(profileName: string, gen: number): boolean {
  return this.generations.get(profileName) === gen;
}
```

`connect(profile)`: captures `gen = nextGeneration(profile.name)` at the start. After `await client.connect()` resolves (success or error), only writes to `states` if `isCurrentGeneration(profile.name, gen)` is true. The `throw` on error is unconditional (the caller of *this* `connect()` call still needs to know *this* call failed), but the shared `states` map is only updated by the most-recently-started call.

`reconnect(profile)`: same generation-guard pattern. Disconnects and discards any existing cached client (`client.disconnect()` then `clients.delete(name)`), creates a fresh client via `createAdminClient(profile)` (now `await`ed since the factory is async), connects, and writes state under the generation guard.

`createAdminClient` now returns `Promise<KafkaAdminClient>`, so `connect()`'s `client = this.createAdminClient(profile)` becomes `client = await this.createAdminClient(profile)`.

This closes carried-over I4/M4: a `getChildren`-triggered `connect()` that is still in flight when the user fires `Kafka: Reconnect` can no longer overwrite the reconnect's outcome, because the connect's generation is stale by the time it resolves.

### Topic Metadata webview — `src/webviews/topicMetadataPanel.ts` (new)

Pure, unit-tested render functions (no vscode import):
- `renderTopicMetadataHtml(topicName: string, metadata: TopicMetadata, configEntries: ConfigEntry[]): string` — full HTML document with two tables:
  - **Partitions**: Partition | Leader | Replicas | ISR
  - **Config**: Name | Value | Default?
  - Includes a "Refresh" `<button>` wired to `acquireVsCodeApi().postMessage({type: 'refresh'})`.
- `renderErrorHtml(message: string): string` — single-message error body, used for "not connected" and fetch failures.

vscode glue (compile-only): `TopicMetadataPanel` class with `static currentPanel: TopicMetadataPanel | undefined`.
- `static async show(context, profileName, topicName, adminService)`:
  - Reuses `currentPanel` if set (`panel.reveal()`), else `vscode.window.createWebviewPanel('kafkaTopicMetadata', 'Topic Metadata', vscode.ViewColumn.Active, {enableScripts: true})`.
  - Sets `panel.title = \`Topic: ${topicName}\``.
  - If `adminService` is `undefined`: `panel.webview.html = renderErrorHtml('Not connected — expand the connection in the sidebar first.')`.
  - Else: `Promise.all([adminService.getTopicMetadata(topicName), adminService.getTopicConfig(topicName)])` → `renderTopicMetadataHtml(...)`; on rejection, `renderErrorHtml(err.message)`.
  - `panel.webview.onDidReceiveMessage` handles `{type: 'refresh'}` by re-running the fetch+render.
  - `panel.onDidDispose` clears `currentPanel`.

### Tree view wiring — `src/treeView/kafkaExplorerProvider.ts`, `src/treeView/treeItems.ts`

- `{kind: 'topic', ...}` variant of `KafkaTreeNode` gains a `profile: ConnectionProfile` field (set when `topicsFolder` builds its children).
- `'topic'` case in `getTreeItem` sets `item.command = {command: 'kafkaLagMonitor.showTopicMetadata', title: 'Show Topic Metadata', arguments: [element.profile, element.topic.name]}`.
- `buildConnectionNode` (or the `'connection'` case in `getTreeItem`) sets `item.contextValue = 'kafkaConnection'` for context-menu targeting.

### `package.json`

New entries under `contributes.commands`:
- `kafkaLagMonitor.addConnection` — "Kafka: Add Connection", `icon: "$(add)"`
- `kafkaLagMonitor.editConnection` — "Kafka: Edit Connection"
- `kafkaLagMonitor.removeConnection` — "Kafka: Remove Connection"
- `kafkaLagMonitor.reconnect` — "Kafka: Reconnect"

`kafkaLagMonitor.showTopicMetadata` is registered via `vscode.commands.registerCommand` only — not listed in `contributes.commands` (invoked solely with arguments from the topic TreeItem's `command`, not meaningful from the Command Palette).

New `contributes.menus`:
- `view/title`: `kafkaLagMonitor.addConnection`, `when: view == kafkaLagMonitor.explorer`, group `navigation`.
- `view/item/context`: `kafkaLagMonitor.editConnection`, `kafkaLagMonitor.removeConnection`, `kafkaLagMonitor.reconnect`, `when: view == kafkaLagMonitor.explorer && viewItem == kafkaConnection`.

## Error Handling

- Wizard: each `showInputBox` step uses `validateInput` to block "Enter" on invalid values (inline error shown by VS Code). Final `validateProfile()` call is a defense-in-depth check before persisting; if it fails, `showErrorMessage` with the joined error list and abort the save (profile not added/edited).
- Remove: modal confirmation via `showWarningMessage`; cancel aborts with no changes.
- Reconnect / Add / Edit / Remove failures (e.g. `saveConnectionProfiles` rejecting, `connect()` throwing): caught and surfaced via `vscode.window.showErrorMessage(err.message)`. Connection-level errors additionally land in `ConnectionState.error`, already surfaced via the tree item description/tooltip.
- Topic Metadata webview: fetch failures render `renderErrorHtml(err.message)` inside the panel rather than throwing — consistent with `kafkaExplorerProvider`'s existing `{kind: 'message', text: ...}` pattern for the sidebar.

## Testing Strategy

Unit-tested (pure logic, `node:test`):
- `src/test/connectionWizard.test.ts` (new) — `validateProfileName` (empty / `.` in name / duplicate), `parseBrokerList` (valid and invalid `host:port` entries).
- `src/test/connectionManager.test.ts` (extend) — `reconnect()` replaces the cached client and resets state to `connected`/`error`; generation-guard test simulating a stale `connect()` resolving *after* a `reconnect()` has started, asserting final state reflects the reconnect's outcome.
- `src/test/topicMetadataPanel.test.ts` (new) — `renderTopicMetadataHtml` produces expected table rows for given metadata/config fixtures; `renderErrorHtml` produces the error message.

Compile-only (vscode glue, no unit tests — matches `profileStore.ts`/`secretStore.ts`/`kafkaAdminAdapter.ts` precedent):
- `connectionStore.ts`, `connectionWizard.ts` (QuickInput orchestration), `topicMetadataPanel.ts` (panel lifecycle), new command registrations in `extension.ts`, `package.json` contribution wiring.

Existing `src/test/connectionManager.test.ts` fake factories update from `(profile) => createFakeAdminClient(...)` to `async (profile) => createFakeAdminClient(...)` to match the new `AdminClientFactory` signature.
