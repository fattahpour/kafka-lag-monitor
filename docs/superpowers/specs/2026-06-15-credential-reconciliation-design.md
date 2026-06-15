# Phase 6: Credential Reconciliation — Design

## Overview

Phase 5's final review flagged two **Important (non-blocking)** issues in
`connectionCommands.ts`'s `editConnection` handler:

1. Editing a connection from mTLS to plain SSL/no-SSL (or removing a
   passphrase while staying on mTLS) leaves a stale `tlsKeyPassphrase`
   secret in SecretStorage.
2. This mirrors a **pre-existing** gap: editing a connection's
   authentication from SASL to "None" leaves stale `username`/`password`
   secrets in SecretStorage.

This phase fixes both, uniformly, via a single new pure module.

## Goals

- `editConnection` (and, for symmetry, `addConnection`) correctly delete
  stale `username`/`password`/`tlsKeyPassphrase` secrets when the wizard
  result no longer calls for them.
- Existing "leave blank to keep existing" UX for all three credential
  fields continues to work unchanged.
- The reconciliation logic is a pure, unit-tested function — no `vscode`
  or `fs` imports — following the precedent set by `sslConfig.ts`.

## Non-goals

- Renaming a connection during edit leaves orphaned secrets under the OLD
  profile name in SecretStorage. This is a separate, lower-severity issue,
  not raised by the Phase 5 review, and is explicitly out of scope here.

## Data Model & Module (`src/connection/credentialReconciliation.ts`, new file)

A pure module, mirroring `sslConfig.ts`'s style. `WizardResult` (currently
defined in `connectionCommands.ts`) moves here, since it's the type this
module operates on; `connectionCommands.ts` imports it back.

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
    // result.tlsPassphrase === '' → no-op, keep existing
  } else {
    del.push('tlsKeyPassphrase');
  }

  return { set, delete: del };
}
```

## Truth Table

| `result.profile.sasl` | `username` / `password` | → |
|---|---|---|
| truthy | non-empty | `set.<field> = <value>` |
| truthy | empty string / `undefined` | no-op (keep existing secret) |
| `null` (mechanism = "None") | (n/a) | `delete: [..., 'username', 'password']` |

| `result.profile.ssl` | `tlsPassphrase` | → |
|---|---|---|
| `MtlsConfig` (object) | non-empty string | `set.tlsKeyPassphrase = <value>` |
| `MtlsConfig` (object) | `''` (blank input, "Yes" chosen) | no-op (keep existing secret) |
| `MtlsConfig` (object) | `undefined` ("Has passphrase?" = "No") | `delete: [..., 'tlsKeyPassphrase']` |
| `boolean` (plain SSL on/off) | (n/a) | `delete: [..., 'tlsKeyPassphrase']` |

This relies on the wizard's existing `showInputBox` semantics: cancel
(`Escape`) already causes `runConnectionWizard` to early-return `undefined`
for the whole result, so a defined `WizardResult` only ever has
`tlsPassphrase` as `undefined` (prompt not shown, i.e. "Has passphrase?" =
"No"), `''` (prompt shown, submitted blank), or a non-empty string.

## Wiring (`src/connection/connectionCommands.ts`)

- `WizardResult` is removed from this file and imported from
  `credentialReconciliation.ts` instead.
- A new local (unexported) helper:

```typescript
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
```

- In both `addConnection` and `editConnection`, the existing
  SASL/mTLS-specific credential-storage blocks (currently duplicated,
  lines ~162-168 and ~192-198) are replaced with a single call:

```typescript
await applyCredentialChanges(context.secrets, result.profile.name, planCredentialChanges(result));
```

placed where the existing credential-storage block was (after
`saveConnectionProfiles`, inside the same `try`).

- `removeConnection`'s existing
  `deleteCredentials(context.secrets, target, ['username', 'password', 'tlsKeyPassphrase'])`
  is unchanged — already correct.

## Testing

**`src/test/credentialReconciliation.test.ts`** (new): table-driven unit
tests for `planCredentialChanges`, covering:

- SASL with new username/password → both set, `tlsKeyPassphrase` deleted
  (non-mTLS `ssl`).
- SASL with blank username/password → no-op for those fields (keep
  existing).
- SASL downgraded to "None" (`sasl: null`) → `username`/`password` deleted.
- mTLS with new passphrase → `tlsKeyPassphrase` set.
- mTLS with passphrase prompt shown but left blank (`tlsPassphrase === ''`)
  → no-op (keep existing).
- mTLS with "Has passphrase?" = "No" (`tlsPassphrase === undefined`) →
  `tlsKeyPassphrase` deleted.
- `ssl` downgraded from `MtlsConfig` to `boolean` → `tlsKeyPassphrase`
  deleted.

**`connectionCommands.ts`**: no new tests, consistent with its existing
no-unit-test convention for `vscode`-API glue — compile-checking only.

## Documentation

No user-facing behavior changes beyond bug fixes (stale secrets are now
cleaned up as users would expect). No README/`package.json` changes
needed.
