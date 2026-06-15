import assert from 'node:assert/strict';
import test from 'node:test';
import { buildSslOptions } from '../connection/sslConfig';

function fakeReadFile(files: Record<string, string>): (path: string) => string {
  return (path: string) => {
    const content = files[path];
    if (content === undefined) {
      throw new Error(`ENOENT: no such file, open '${path}'`);
    }
    return content;
  };
}

test('buildSslOptions passes through ssl: true unchanged', () => {
  assert.equal(buildSslOptions(true, fakeReadFile({})), true);
});

test('buildSslOptions passes through ssl: false unchanged', () => {
  assert.equal(buildSslOptions(false, fakeReadFile({})), false);
});

test('buildSslOptions reads cert and key files for an mTLS config without a CA', () => {
  const readFile = fakeReadFile({
    '/certs/client.crt': 'CERT-CONTENT',
    '/certs/client.key': 'KEY-CONTENT',
  });

  const result = buildSslOptions({ cert: '/certs/client.crt', key: '/certs/client.key' }, readFile);

  assert.deepEqual(result, { cert: 'CERT-CONTENT', key: 'KEY-CONTENT' });
});

test('buildSslOptions includes the CA file content when ssl.ca is set', () => {
  const readFile = fakeReadFile({
    '/certs/ca.crt': 'CA-CONTENT',
    '/certs/client.crt': 'CERT-CONTENT',
    '/certs/client.key': 'KEY-CONTENT',
  });

  const result = buildSslOptions(
    { ca: '/certs/ca.crt', cert: '/certs/client.crt', key: '/certs/client.key' },
    readFile,
  );

  assert.deepEqual(result, { ca: 'CA-CONTENT', cert: 'CERT-CONTENT', key: 'KEY-CONTENT' });
});

test('buildSslOptions includes a passphrase when provided', () => {
  const readFile = fakeReadFile({
    '/certs/client.crt': 'CERT-CONTENT',
    '/certs/client.key': 'KEY-CONTENT',
  });

  const result = buildSslOptions(
    { cert: '/certs/client.crt', key: '/certs/client.key' },
    readFile,
    'secret-passphrase',
  );

  assert.deepEqual(result, { cert: 'CERT-CONTENT', key: 'KEY-CONTENT', passphrase: 'secret-passphrase' });
});

test('buildSslOptions omits passphrase when not provided', () => {
  const readFile = fakeReadFile({
    '/certs/client.crt': 'CERT-CONTENT',
    '/certs/client.key': 'KEY-CONTENT',
  });

  const result = buildSslOptions({ cert: '/certs/client.crt', key: '/certs/client.key' }, readFile);

  assert.deepEqual(result, { cert: 'CERT-CONTENT', key: 'KEY-CONTENT' });
});

test('buildSslOptions throws a descriptive error when the cert file cannot be read', () => {
  const readFile = fakeReadFile({
    '/certs/client.key': 'KEY-CONTENT',
  });

  assert.throws(
    () => buildSslOptions({ cert: '/certs/missing.crt', key: '/certs/client.key' }, readFile),
    /Failed to read TLS "cert" file "\/certs\/missing\.crt"/,
  );
});

test('buildSslOptions throws a descriptive error when the key file cannot be read', () => {
  const readFile = fakeReadFile({
    '/certs/client.crt': 'CERT-CONTENT',
  });

  assert.throws(
    () => buildSslOptions({ cert: '/certs/client.crt', key: '/certs/missing.key' }, readFile),
    /Failed to read TLS "key" file "\/certs\/missing\.key"/,
  );
});

test('buildSslOptions throws a descriptive error when the CA file cannot be read', () => {
  const readFile = fakeReadFile({
    '/certs/client.crt': 'CERT-CONTENT',
    '/certs/client.key': 'KEY-CONTENT',
  });

  assert.throws(
    () =>
      buildSslOptions(
        { ca: '/certs/missing-ca.crt', cert: '/certs/client.crt', key: '/certs/client.key' },
        readFile,
      ),
    /Failed to read TLS "ca" file "\/certs\/missing-ca\.crt"/,
  );
});
