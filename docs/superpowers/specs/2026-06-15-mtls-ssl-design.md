# Phase 5: mTLS / Client-Certificate SSL — Design

## Overview

Today `ConnectionProfile.ssl` is a plain `boolean`: `true` enables TLS with
Node's default trust store and no client certificate, `false` disables TLS.
The README explicitly calls out that "mTLS / client-certificate SSL is not
yet supported."

This phase adds mutual TLS support: a connection can supply a client
certificate + private key (and optionally a custom CA certificate), all as
file paths to PEM files on disk, with an optional passphrase for an
encrypted private key stored in SecretStorage.

## Goals

- A connection profile can specify a client certificate + key (mTLS),
  optionally a custom CA certificate, and optionally a passphrase for an
  encrypted private key.
- The **Add/Edit Connection** wizard supports configuring mTLS.
- Existing `ssl: true` / `ssl: false` profiles continue to work unchanged
  (backward compatible).
- Clear, actionable errors when a configured cert/key/CA file can't be read.

## Non-goals

- No `rejectUnauthorized: false` / "ignore cert errors" toggle.
- No support for PKCS#12 (`.p12`/`.pfx`) bundles — PEM files only.
- No file-picker dialog (`showOpenDialog`) — paths are typed/pasted into
  input boxes, consistent with the rest of the wizard.

## Data Model (`src/connection/types.ts`)

```typescript
export interface MtlsConfig {
  /** Path to a PEM-encoded CA certificate file. Optional — if omitted, Node's default trust store is used. */
  ca?: string;
  /** Path to a PEM-encoded client certificate file. */
  cert: string;
  /** Path to a PEM-encoded client private key file. */
  key: string;
}

export interface ConnectionProfile {
  name: string;
  brokers: string[];
  sasl: SaslConfig | null;
  ssl: boolean | MtlsConfig;
  clientId: string;
}
```

`ssl: true` / `ssl: false` keep their current meaning (plain TLS on/off, no
client cert). `ssl: { cert, key, ca? }` means mTLS.

## Validation (`src/connection/profileValidation.ts`)

`validateProfile` currently does:

```typescript
const ssl = obj.ssl === true;
```

New logic:

```typescript
let ssl: ConnectionProfile['ssl'] = false;
if (obj.ssl === true || obj.ssl === false || obj.ssl === undefined || obj.ssl === null) {
  ssl = obj.ssl === true;
} else if (typeof obj.ssl === 'object') {
  const sslObj = obj.ssl as Record<string, unknown>;
  const cert = sslObj.cert;
  const key = sslObj.key;
  const ca = sslObj.ca;
  if (typeof cert !== 'string' || cert.trim() === '') {
    errors.push('"ssl.cert" must be a non-empty string (path to a PEM file)');
  }
  if (typeof key !== 'string' || key.trim() === '') {
    errors.push('"ssl.key" must be a non-empty string (path to a PEM file)');
  }
  if (ca !== undefined && (typeof ca !== 'string' || ca.trim() === '')) {
    errors.push('"ssl.ca" must be a non-empty string (path to a PEM file) when present');
  }
  if (typeof cert === 'string' && cert.trim() !== '' && typeof key === 'string' && key.trim() !== '') {
    ssl = { cert, key, ...(typeof ca === 'string' && ca.trim() !== '' ? { ca } : {}) };
  }
} else {
  errors.push('"ssl" must be a boolean or an object with "cert" and "key" (and optional "ca")');
}
```

This follows the existing pattern: collect errors into the `errors` array;
`validateProfile` already returns `{ profile: null, errors }` if `errors.length > 0`,
so a partially-built `ssl` object is never returned as part of a profile.

## SSL-building module (`src/connection/sslConfig.ts`, new file)

A small pure module, mirroring the existing `connectionWizard.ts` /
`profileValidation.ts` style — no `vscode` or `fs` imports, so it's directly
unit-testable.

```typescript
import { MtlsConfig } from './types';

export interface TlsConnectionOptions {
  ca?: string;
  cert?: string;
  key?: string;
  passphrase?: string;
}

/**
 * Builds the value for kafkajs's `ssl` option.
 *
 * @param ssl The profile's `ssl` field (boolean or MtlsConfig).
 * @param readFile Reads a file's contents as utf-8 text. Injected for testability;
 *                  production code passes `(path) => fs.readFileSync(path, 'utf-8')`.
 * @param passphrase Optional passphrase for an encrypted private key (mTLS only).
 */
export function buildSslOptions(
  ssl: boolean | MtlsConfig,
  readFile: (path: string) => string,
  passphrase?: string,
): boolean | TlsConnectionOptions {
  if (typeof ssl === 'boolean') {
    return ssl;
  }

  const options: TlsConnectionOptions = {
    cert: readCertFile(readFile, ssl.cert, 'cert'),
    key: readCertFile(readFile, ssl.key, 'key'),
  };
  if (ssl.ca) {
    options.ca = readCertFile(readFile, ssl.ca, 'ca');
  }
  if (passphrase) {
    options.passphrase = passphrase;
  }
  return options;
}

function readCertFile(readFile: (path: string) => string, path: string, field: 'ca' | 'cert' | 'key'): string {
  try {
    return readFile(path);
  } catch (err) {
    throw new Error(`Failed to read TLS "${field}" file "${path}": ${(err as Error).message}`);
  }
}
```

## Wiring into `extension.ts`

`buildKafka` currently builds `sasl` and passes `ssl: profile.ssl` straight
through. New version:

```typescript
import * as fs from 'fs';
import { buildSslOptions } from './connection/sslConfig';

// ... inside buildKafka:
let ssl: boolean | TlsConnectionOptions;
try {
  let passphrase: string | undefined;
  if (typeof profile.ssl === 'object') {
    passphrase = await getCredential(context.secrets, profile.name, 'tlsKeyPassphrase');
  }
  ssl = buildSslOptions(profile.ssl, (path) => fs.readFileSync(path, 'utf-8'), passphrase);
} catch (err) {
  throw new Error(`${(err as Error).message} (connection "${profile.name}")`);
}

return new Kafka({
  clientId: profile.clientId,
  brokers: profile.brokers,
  ssl,
  sasl,
  logCreator: createKafkaLogCreator((line) => output.appendLine(line)),
});
```

This throw propagates exactly like the existing "Missing SASL credentials"
error — `ConnectionManager.connect()` already catches client-factory errors
and sets the connection's status to `'error'` with the message surfaced in
the Explorer tree.

## Wizard (`src/connection/connectionCommands.ts`)

The "Use SSL?" `showQuickPick(['No', 'Yes'], ...)` becomes:

```typescript
const SSL_CHOICES = ['No', 'Yes', 'Yes (with client certificate)'];

const sslChoice = await vscode.window.showQuickPick(SSL_CHOICES, {
  title: 'Use SSL?',
  placeHolder:
    typeof initial?.ssl === 'object' ? SSL_CHOICES[2] : initial?.ssl ? SSL_CHOICES[1] : SSL_CHOICES[0],
});
if (sslChoice === undefined) return undefined;
```

If `sslChoice === SSL_CHOICES[2]` ("Yes (with client certificate)"), prompt
for three more values, pre-filled from `initial.ssl` when it's an `MtlsConfig`:

```typescript
let mtls: MtlsConfig | undefined;
let tlsPassphrase: string | undefined;

if (sslChoice === SSL_CHOICES[2]) {
  const initialMtls = typeof initial?.ssl === 'object' ? initial.ssl : undefined;

  const ca = await vscode.window.showInputBox({
    title: 'CA certificate path (optional, leave blank for default trust store)',
    value: initialMtls?.ca ?? '',
  });
  if (ca === undefined) return undefined;

  const cert = await vscode.window.showInputBox({
    title: 'Client certificate path (PEM)',
    value: initialMtls?.cert ?? '',
    validateInput: (value) => (value.trim() === '' ? '"Client certificate path" must not be empty' : null),
  });
  if (cert === undefined) return undefined;

  const key = await vscode.window.showInputBox({
    title: 'Client private key path (PEM)',
    value: initialMtls?.key ?? '',
    validateInput: (value) => (value.trim() === '' ? '"Client private key path" must not be empty' : null),
  });
  if (key === undefined) return undefined;

  mtls = { cert, key, ...(ca.trim() !== '' ? { ca } : {}) };

  const hasPassphrase = await vscode.window.showQuickPick(['No', 'Yes'], {
    title: 'Does the private key have a passphrase?',
  });
  if (hasPassphrase === undefined) return undefined;

  if (hasPassphrase === 'Yes') {
    tlsPassphrase = await vscode.window.showInputBox({
      title: 'Private key passphrase (leave blank to keep existing)',
      password: true,
    });
    if (tlsPassphrase === undefined) return undefined;
  }
}
```

`runConnectionWizard`'s call to `validateProfile` passes
`ssl: mtls ?? (sslChoice === SSL_CHOICES[1])`, and `WizardResult` gains an
optional `tlsPassphrase?: string` field alongside `username`/`password`.

In `addConnection`/`editConnection`, after `saveConnectionProfiles`, alongside
the existing SASL-credential `setCredential` calls:

```typescript
if (result.tlsPassphrase) {
  await setCredential(context.secrets, result.profile.name, 'tlsKeyPassphrase', result.tlsPassphrase);
}
```

## Secret cleanup (`removeConnection`)

```typescript
await deleteCredentials(context.secrets, target, ['username', 'password', 'tlsKeyPassphrase']);
```

(`deleteCredentials` already iterates a field list and is a no-op for absent
keys, so adding `'tlsKeyPassphrase'` unconditionally is safe and consistent
with current behavior.)

## Testing

- **`src/test/sslConfig.test.ts`** (new): unit tests for `buildSslOptions`
  using a fake `readFile` function (a `Map<string, string>` lookup) — no real
  filesystem needed:
  - `ssl: true` / `ssl: false` pass through unchanged.
  - `ssl: { cert, key }` (no `ca`) → `{ cert: <contents>, key: <contents> }`,
    no `ca`/`passphrase` keys.
  - `ssl: { cert, key, ca }` → includes `ca: <contents>`.
  - `passphrase` provided → included in output; omitted when not provided.
  - A `readFile` that throws for `key` → `buildSslOptions` throws an `Error`
    whose message includes `"key"` and the offending path.

- **`src/test/profileValidation.test.ts`**: add cases for
  `ssl: { cert: '...', key: '...' }` (valid), `ssl: { cert: '...' }` (missing
  key → error), `ssl: { cert: '', key: '/x' }` (empty cert → error),
  `ssl: { cert: '/x', key: '/y', ca: '/z' }` (valid with ca), and
  `ssl: "yes"` (invalid type → error).

- **Wizard**: `connectionCommands.ts` is not currently unit-tested (it's
  `vscode`-API glue, like the rest of that file) — no new tests needed beyond
  compile-checking, consistent with existing coverage.

## Documentation

- **`README.md`**:
  - Remove "mTLS / client-certificate SSL is not yet supported."
  - Add a settings example showing the mTLS object shape, e.g.:
    ```jsonc
    {
      "name": "mtls-cluster",
      "brokers": ["broker1:9093"],
      "sasl": null,
      "ssl": { "ca": "/etc/kafka/ca.pem", "cert": "/etc/kafka/client-cert.pem", "key": "/etc/kafka/client-key.pem" },
      "clientId": "kafka-lag-monitor"
    }
    ```
  - Briefly describe the wizard's "Yes (with client certificate)" option and
    the key-passphrase prompt (stored in SecretStorage).

- **`package.json`**: update `kafkaLagMonitor.connections`'s `description` to
  mention `ssl` can be `true`/`false`/`{ ca?, cert, key }`.

## Error Handling Summary

| Failure | Surfaced as |
|---|---|
| `ssl.cert`/`ssl.key` missing in settings | Validation error at profile-load time (existing `onConfigError` → output channel), profile skipped — same as any other invalid profile today. |
| Cert/key/CA file unreadable at connect time | `Error` thrown from `buildKafka`, caught by `ConnectionManager.connect()`, connection status → `'error'`, message shown in Explorer tree tooltip — same path as missing SASL credentials today. |
| Wrong/missing passphrase for an encrypted key | Surfaces as a kafkajs/Node TLS connection error (existing error-surfacing path) — not specially handled. |
