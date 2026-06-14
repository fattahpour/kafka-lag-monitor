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
