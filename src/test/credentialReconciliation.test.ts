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
