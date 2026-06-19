# Move Kafka connections config to a .vscode file

## Problem

`kafkaLagMonitor.connections` today lives in the VS Code `kafkaLagMonitor.connections` setting (read via `vscode.workspace.getConfiguration`, written via `ConfigurationTarget.Global`). It's an array of objects, which is awkward to hand-edit through the Settings UI, and this repo's own `.vscode/settings.json` currently embeds the dev/test connection profile inline alongside unrelated editor settings.

## Goal

Move connection profile storage out of VS Code settings into a dedicated JSON file inside the workspace's `.vscode/` folder, and provide a sensible default profile when no file/profiles exist yet.

## Storage

- Path: `<workspaceFolder>/.vscode/kafka-lag-monitor.connections.json`, where `<workspaceFolder>` is `vscode.workspace.workspaceFolders[0]`.
- Multi-root workspaces: only the first folder is used. This is a known limitation, not handled in this change.
- File shape:
  ```json
  {
    "connections": [
      {
        "name": "local-cluster",
        "brokers": ["localhost:9092"],
        "sasl": null,
        "ssl": false,
        "clientId": "kafka-lag-monitor"
      }
    ]
  }
  ```
  Each entry in `connections` has the same shape validated today by `parseConnectionProfiles`/`validateProfile` — no validation logic changes.

## Default profile

When there's no file to read profiles from, use this default:

```json
{ "name": "local-cluster", "brokers": ["localhost:9092"], "sasl": null, "ssl": false, "clientId": "kafka-lag-monitor" }
```

Defined once as a constant (e.g. in `profileStore.ts`) so it isn't duplicated.

## Read path: `src/connection/profileStore.ts`

`getConnectionProfiles(onError)` stops calling `vscode.workspace.getConfiguration('kafkaLagMonitor').get('connections')`. New behavior:

1. No workspace folder open (`vscode.workspace.workspaceFolders` undefined/empty) → call `onError('Open a workspace folder to manage Kafka connections')`, return `[DEFAULT_PROFILE]`.
2. File does not exist at the computed path → return `[DEFAULT_PROFILE]` (no error — this is the expected first-run state).
3. File exists, but JSON.parse fails, or top-level shape isn't `{ connections: [...] }`, or `parseConnectionProfiles` reports per-entry errors → call `onError` with the same per-entry messages as today (`kafkaLagMonitor.connections[${index}]: ...`) for entry errors, or a parse-error message for unparseable JSON/bad shape; return only the profiles that did parse successfully (or `[]` if nothing parsed). Do not fall back to `DEFAULT_PROFILE` here — a broken file should surface as an error, not be silently masked.
4. File exists and parses cleanly → return the parsed profiles (can be `[]` if the user has an empty `connections` array — that's a valid "no connections configured" state, distinct from "file doesn't exist").

`getLagThresholds()` is unchanged — it keeps reading `lagWarningThreshold`/`lagCriticalThreshold` from `vscode.workspace.getConfiguration('kafkaLagMonitor')`, since those remain plain VS Code settings.

## Write path: `src/connection/connectionStore.ts`

`saveConnectionProfiles(profiles)`:

1. No workspace folder open → `throw new Error('Open a workspace folder to manage Kafka connections')`. The existing try/catch in each command handler in `connectionCommands.ts` already calls `vscode.window.showErrorMessage((err as Error).message)` on throw — no changes needed there.
2. Otherwise: `fs.mkdir(path.join(folder, '.vscode'), { recursive: true })`, then `fs.writeFile` the target path with `JSON.stringify({ connections: profiles }, null, 2)`.

Use `fs/promises` (function is already `async`).

## `package.json`

- Remove the `kafkaLagMonitor.connections` property from `contributes.configuration.properties` — nothing reads it anymore, leaving it would show a dead setting in the Settings UI.
- `lagWarningThreshold`, `lagCriticalThreshold`, `pollIntervalSeconds` properties are unchanged.

## This repo's dev `.vscode/` folder

- Remove the `kafkaLagMonitor.connections` block from `.vscode/settings.json`.
- Create `.vscode/kafka-lag-monitor.connections.json` with the equivalent local-cluster data currently in `settings.json` (3 brokers: `localhost:9091`, `localhost:9092`, `localhost:9095`), so F5 Extension Development Host testing keeps working unchanged.

## Testing

Unit tests (`node --test`, matching existing style in `src/test/`):

**`profileStore.test.ts`**
- No workspace folder → returns `[DEFAULT_PROFILE]`, calls `onError`.
- File missing → returns `[DEFAULT_PROFILE]`, does not call `onError`.
- File contains invalid JSON → returns `[]`, calls `onError`.
- File contains `{ connections: [] }` → returns `[]`, no error (distinguishing from "missing file").
- File contains one valid + one invalid entry → returns the valid one only, calls `onError` with an index-qualified message for the invalid one.
- File contains valid profiles → returns them as-is.

**`connectionStore.test.ts`**
- No workspace folder → `saveConnectionProfiles` rejects with the expected error message.
- Workspace folder open, `.vscode/` doesn't exist yet → creates it and writes the file.
- Workspace folder open, `.vscode/` exists → writes/overwrites the file with `{ connections: [...] }`.

Both test files need to stub `vscode.workspace.workspaceFolders` and use a temp directory (e.g. `node:fs` + `node:os.tmpdir()` + cleanup in test teardown) as the fake workspace folder, since these functions hit the real filesystem.

## Out of scope

- Multi-root workspace support beyond "use the first folder."
- Live file-watching (today, editing the connections source by hand requires the same "reload window" step as before — this is unchanged behavior, not a regression).
- `contributes.jsonValidation` schema for IntelliSense on the new file (explicitly declined).
