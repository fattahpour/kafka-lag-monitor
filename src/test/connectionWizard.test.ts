import assert from 'node:assert/strict';
import test from 'node:test';
import { parseBrokerList, validateProfileName } from '../connection/connectionWizard';

test('validateProfileName rejects an empty name', () => {
  assert.match(validateProfileName('', []) ?? '', /must not be empty/);
});

test('validateProfileName rejects a name containing a dot', () => {
  assert.match(validateProfileName('my.cluster', []) ?? '', /must not contain "\."/);
});

test('validateProfileName rejects a duplicate name', () => {
  assert.match(validateProfileName('local-cluster', ['local-cluster']) ?? '', /already exists/);
});

test('validateProfileName accepts a valid, unique name', () => {
  assert.equal(validateProfileName('local-cluster', ['other-cluster']), null);
});

test('parseBrokerList parses comma-separated host:port entries', () => {
  const result = parseBrokerList('localhost:9091, localhost:9092 ,localhost:9095');
  assert.deepEqual(result, {
    brokers: ['localhost:9091', 'localhost:9092', 'localhost:9095'],
    errors: [],
  });
});

test('parseBrokerList reports malformed entries but keeps valid ones', () => {
  const result = parseBrokerList('localhost:9091, not-a-broker');
  assert.deepEqual(result.brokers, ['localhost:9091']);
  assert.match(result.errors[0], /host:port/);
});

test('parseBrokerList reports an error for empty input', () => {
  const result = parseBrokerList('   ');
  assert.deepEqual(result.brokers, []);
  assert.match(result.errors[0], /At least one broker/);
});
