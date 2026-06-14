import assert from 'node:assert/strict';
import test from 'node:test';
import { secretKey } from '../connection/secretKey';

test('secretKey namespaces by profile name and field', () => {
  assert.equal(secretKey('local-cluster', 'password'), 'kafkaLagMonitor.connection.local-cluster.password');
});

test('secretKey keeps different profiles and fields distinct', () => {
  assert.notEqual(secretKey('local-cluster', 'password'), secretKey('staging-cluster', 'password'));
  assert.notEqual(secretKey('local-cluster', 'username'), secretKey('local-cluster', 'password'));
});
