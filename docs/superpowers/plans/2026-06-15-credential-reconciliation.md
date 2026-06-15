# Phase 6: Credential Reconciliation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix stale `username`/`password`/`tlsKeyPassphrase` secrets left in SecretStorage after editing a connection (SASL downgraded to "None", mTLS removed, or mTLS passphrase removed), via a single pure reconciliation function.

**Architecture:** A new pure module `src/connection/credentialReconciliation.ts` exports `WizardResult`, `CredentialChanges`, and `planCredentialChanges(result): CredentialChanges`, computing which credential fields to `set` vs `delete` from a completed wizard result. `connectionCommands.ts` becomes a thin consumer: a small `applyCredentialChanges` helper iterates `set`/`delete` using the existing `setCredential`/`deleteCredentials` from `secretStore.ts`, replacing the duplicated ad-hoc credential-storage blocks in `addConnection` and `editConnection`.

**Tech Stack:** TypeScript, `node:test` + `node:assert/strict` (existing test setup, see `src/test/sslConfig.test.ts` for style), VS Code Extension API.

---

## File Structure

- **Create** `src/connection/credentialReconciliation.ts` — pure module: `WizardResult` interface (moved from `connectionCommands.ts`), `CredentialChanges` interface, `planCredentialChanges` function. No `vscode` or `fs` imports.
- **Create** `src/test/credentialReconciliation.test.ts` — table-driven unit tests for `planCredentialChanges`.
- **Modify** `src/connection/connectionCommands.ts`:
  - Remove the local `WizardResult` interface; import `WizardResult`, `CredentialChanges`, `planCredentialChanges` from `./credentialReconciliation`.
  - Add a new `applyCredentialChanges` helper function.
  - Replace the credential-storage blocks in `addConnection` and `editConnection` with a single call to `applyCredentialChanges`.

---

## Task 1: `credentialReconciliation.ts` pure module + tests

**Files:**
- Create: `src/connection/credentialReconciliation.ts`
- Test: `src/test/credentialReconciliation.test.ts`

- [ ] **Step 1: Write the failing test file**

Create `src/test/credentialReconciliation.test.ts`:

```typescript
import assert from 'node:assert/strict';
import test from 'node:test';
import { planCredentialChanges, WizardResult } from '../connection/credentialReconciliation';

const baseProfile = {
  name: 'test-connection',
  brokers: ['broker:9092'],
  clientId: 'kafka-lag-monitor',
};

test('planCredentialChanges sets username and password when SASL is enabled and both are provided', () => {
  const result: WizardResult = {
    profile: { ...baseProfile, sasl: { mechanism: 'plain' }, ssl: false },
    username: 'alice',
    password: 'secret',
  };

  assert.deepEqual(planCredentialChanges(result), {
    set: { username: 'alice', password: 'secret' },
    delete: ['tlsKeyPassphrase'],
  });
});

test('planCredentialChanges keeps existing username/password when SASL is enabled but fields left blank', () => {
  const result: WizardResult = {
    profile: { ...baseProfile, sasl: { mechanism: 'plain' }, ssl: false },
    username: '',
    password: '',
  };

  assert.deepEqual(planCredentialChanges(result), {
    set: {},
    delete: ['tlsKeyPassphrase'],
  });
});

test('planCredentialChanges deletes username and password when SASL mechanism is downgraded to None', () => {
  const result: WizardResult = {
    profile: { ...baseProfile, sasl: null, ssl: false },
  };

  assert.deepEqual(planCredentialChanges(result), {
    set: {},
    delete: ['username', 'password', 'tlsKeyPassphrase'],
  });
});

test('planCredentialChanges sets tlsKeyPassphrase when mTLS is configured with a new passphrase', () => {
  const result: WizardResult = {
    profile: { ...baseProfile, sasl: null, ssl: { cert: '/certs/client.crt', key: '/certs/client.key' } },
    tlsPassphrase: 'p4ss',
  };

  assert.deepEqual(planCredentialChanges(result), {
    set: { tlsKeyPassphrase: 'p4ss' },
    delete: ['username', 'password'],
  });
});

test('planCredentialChanges keeps existing tlsKeyPassphrase when the passphrase prompt is left blank', () => {
  const result: WizardResult = {
    profile: { ...baseProfile, sasl: null, ssl: { cert: '/certs/client.crt', key: '/certs/client.key' } },
    tlsPassphrase: '',
  };

  assert.deepEqual(planCredentialChanges(result), {
    set: {},
    delete: ['username', 'password'],
  });
});

test('planCredentialChanges deletes tlsKeyPassphrase when "Does the private key have a passphrase?" is answered No', () => {
  const result: WizardResult = {
    profile: { ...baseProfile, sasl: null, ssl: { cert: '/certs/client.crt', key: '/certs/client.key' } },
  };

  assert.deepEqual(planCredentialChanges(result), {
    set: {},
    delete: ['username', 'password', 'tlsKeyPassphrase'],
  });
});

test('planCredentialChanges deletes tlsKeyPassphrase when ssl is downgraded from mTLS to a boolean', () => {
  const result: WizardResult = {
    profile: { ...baseProfile, sasl: null, ssl: true },
  };

  assert.deepEqual(planCredentialChanges(result), {
    set: {},
    delete: ['username', 'password', 'tlsKeyPassphrase'],
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test`
Expected: FAIL — `npm run compile` (the `pretest` step) errors with a TypeScript error similar to:
```
src/test/credentialReconciliation.test.ts:3:46 - error TS2307: Cannot find module '../connection/credentialReconciliation' or its corresponding type declarations.
```

- [ ] **Step 3: Implement the pure module**

Create `src/connection/credentialReconciliation.ts`:

```typescript
import { ConnectionProfile } from './types';

export interface WizardResult {
  profile: ConnectionProfile;
  username?: string;
  password?: string;
  tlsPassphrase?: string;
}

export interface CredentialChanges {
  set: Record<string, string>;
  delete: string[];
}

export function planCredentialChanges(result: WizardResult): CredentialChanges {
  const set: Record<string, string> = {};
  const del: string[] = [];

  if (result.profile.sasl) {
    if (result.username) set.username = result.username;
    if (result.password) set.password = result.password;
  } else {
    del.push('username', 'password');
  }

  if (typeof result.profile.ssl === 'object') {
    if (result.tlsPassphrase) {
      set.tlsKeyPassphrase = result.tlsPassphrase;
    } else if (result.tlsPassphrase === undefined) {
      del.push('tlsKeyPassphrase');
    }
  } else {
    del.push('tlsKeyPassphrase');
  }

  return { set, delete: del };
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test`
Expected: PASS — all 7 new tests pass, plus the existing 113 tests (120 total).

- [ ] **Step 5: Commit**

```bash
git add src/connection/credentialReconciliation.ts src/test/credentialReconciliation.test.ts
git commit -m "feat: add credential reconciliation pure module"
```

---

## Task 2: Wire `planCredentialChanges` into `connectionCommands.ts`

**Files:**
- Modify: `src/connection/connectionCommands.ts`

- [ ] **Step 1: Update imports and remove the local `WizardResult` interface**

In `src/connection/connectionCommands.ts`, replace lines 1-26:

```typescript
import * as vscode from 'vscode';
import { ConnectionManager } from './connectionManager';
import { saveConnectionProfiles } from './connectionStore';
import { parseBrokerList, validateProfileName } from './connectionWizard';
import { getConnectionProfiles } from './profileStore';
import { validateProfile } from './profileValidation';
import { deleteCredentials, setCredential } from './secretStore';
import { ConnectionProfile, MtlsConfig, SaslMechanism } from './types';
import { KafkaExplorerProvider } from '../treeView/kafkaExplorerProvider';
import { STATUS_ICONS } from '../treeView/treeItems';

const AUTH_TYPES: Array<{ label: string; mechanism: SaslMechanism | null }> = [
  { label: 'None', mechanism: null },
  { label: 'PLAIN', mechanism: 'plain' },
  { label: 'SCRAM-SHA-256', mechanism: 'scram-sha-256' },
  { label: 'SCRAM-SHA-512', mechanism: 'scram-sha-512' },
];

const SSL_CHOICES = ['No', 'Yes', 'Yes (with client certificate)'];

interface WizardResult {
  profile: ConnectionProfile;
  username?: string;
  password?: string;
  tlsPassphrase?: string;
}
```

with:

```typescript
import * as vscode from 'vscode';
import { ConnectionManager } from './connectionManager';
import { saveConnectionProfiles } from './connectionStore';
import { parseBrokerList, validateProfileName } from './connectionWizard';
import { CredentialChanges, planCredentialChanges, WizardResult } from './credentialReconciliation';
import { getConnectionProfiles } from './profileStore';
import { validateProfile } from './profileValidation';
import { deleteCredentials, setCredential } from './secretStore';
import { ConnectionProfile, MtlsConfig, SaslMechanism } from './types';
import { KafkaExplorerProvider } from '../treeView/kafkaExplorerProvider';
import { STATUS_ICONS } from '../treeView/treeItems';

const AUTH_TYPES: Array<{ label: string; mechanism: SaslMechanism | null }> = [
  { label: 'None', mechanism: null },
  { label: 'PLAIN', mechanism: 'plain' },
  { label: 'SCRAM-SHA-256', mechanism: 'scram-sha-256' },
  { label: 'SCRAM-SHA-512', mechanism: 'scram-sha-512' },
];

const SSL_CHOICES = ['No', 'Yes', 'Yes (with client certificate)'];
```

(`ConnectionProfile` stays imported — it's still used as the type of `runConnectionWizard`'s `initial?: ConnectionProfile` parameter. `MtlsConfig` stays imported — still used inside `runConnectionWizard`.)

- [ ] **Step 2: Add the `applyCredentialChanges` helper**

Immediately after `runConnectionWizard`'s closing brace and before `export function registerConnectionCommands`, find:

```typescript
  return { profile, username, password, tlsPassphrase };
}

export function registerConnectionCommands(
```

Replace with:

```typescript
  return { profile, username, password, tlsPassphrase };
}

async function applyCredentialChanges(
  secrets: vscode.SecretStorage,
  profileName: string,
  changes: CredentialChanges,
): Promise<void> {
  for (const [field, value] of Object.entries(changes.set)) {
    await setCredential(secrets, profileName, field, value);
  }
  if (changes.delete.length > 0) {
    await deleteCredentials(secrets, profileName, changes.delete);
  }
}

export function registerConnectionCommands(
```

- [ ] **Step 3: Replace the credential-storage block in `addConnection`**

Find:

```typescript
        await saveConnectionProfiles([...existing, result.profile]);
        if (result.profile.sasl) {
          if (result.username) await setCredential(context.secrets, result.profile.name, 'username', result.username);
          if (result.password) await setCredential(context.secrets, result.profile.name, 'password', result.password);
        }
        if (result.tlsPassphrase) {
          await setCredential(context.secrets, result.profile.name, 'tlsKeyPassphrase', result.tlsPassphrase);
        }
```

Replace with:

```typescript
        await saveConnectionProfiles([...existing, result.profile]);
        await applyCredentialChanges(context.secrets, result.profile.name, planCredentialChanges(result));
```

- [ ] **Step 4: Replace the credential-storage block in `editConnection`**

Find:

```typescript
        await saveConnectionProfiles(existing.map((p) => (p.name === current.name ? result.profile : p)));
        if (result.profile.sasl) {
          if (result.username) await setCredential(context.secrets, result.profile.name, 'username', result.username);
          if (result.password) await setCredential(context.secrets, result.profile.name, 'password', result.password);
        }
        if (result.tlsPassphrase) {
          await setCredential(context.secrets, result.profile.name, 'tlsKeyPassphrase', result.tlsPassphrase);
        }
```

Replace with:

```typescript
        await saveConnectionProfiles(existing.map((p) => (p.name === current.name ? result.profile : p)));
        await applyCredentialChanges(context.secrets, result.profile.name, planCredentialChanges(result));
```

- [ ] **Step 5: Compile and run the full test suite**

Run: `npm test`
Expected: PASS — `tsc -p ./` compiles cleanly, all 120 tests pass (113 existing + 7 new from Task 1).

- [ ] **Step 6: Commit**

```bash
git add src/connection/connectionCommands.ts
git commit -m "refactor: reconcile stored credentials with wizard result on add/edit"
```

---

## Manual Verification (optional, post-implementation)

Per the design's goals, this can be spot-checked in the Extension Development Host (`F5`):

1. Add a connection with SASL PLAIN (username/password) and no SSL.
2. **Kafka: Edit Connection** that connection, change Authentication to "None". Save.
3. Re-run **Kafka: Edit Connection**, switch Authentication back to PLAIN, leave username/password blank.
   - Before this fix: the old username/password would silently still be used (stale secrets never deleted).
   - After this fix: connecting should fail with "Missing SASL credentials" (since step 2 deleted them) — confirming the deletion took effect.
4. Similarly for mTLS: configure mTLS with a passphrase, then edit to answer "Does the private key have a passphrase?" → "No", and confirm the connection now fails/succeeds appropriately based on whether the key actually needs a passphrase (i.e. the stale passphrase is no longer being read).

---

## Self-Review Notes

- **Spec coverage:** `CredentialChanges`/`WizardResult`/`planCredentialChanges` (Task 1) match the design's data model exactly, including the full truth table (7 test cases cover all 7 rows across the two tables). `applyCredentialChanges` + the two replaced blocks (Task 2) match the design's wiring section exactly. `removeConnection` already deletes `tlsKeyPassphrase` — no change needed, per design's non-goals/explicit statement.
- **Placeholder scan:** none found — all steps have complete code.
- **Type consistency:** `WizardResult` (with `profile`, `username?`, `password?`, `tlsPassphrase?`) and `CredentialChanges` (with `set: Record<string,string>`, `delete: string[]`) are defined once in Task 1 and used identically in Task 2's imports and `applyCredentialChanges` signature.
