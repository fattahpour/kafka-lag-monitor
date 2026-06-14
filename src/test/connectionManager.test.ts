import assert from 'node:assert/strict';
import test from 'node:test';
import { ConnectionManager } from '../connection/connectionManager';
import { KafkaAdminClient } from '../kafka/adminClient';
import { ConnectionProfile } from '../connection/types';

function createFakeAdminClient(overrides: Partial<KafkaAdminClient> = {}): KafkaAdminClient {
  const notImplemented = () => {
    throw new Error('not implemented in fake');
  };
  return {
    connect: async () => {},
    disconnect: async () => {},
    listTopics: notImplemented,
    fetchTopicMetadata: notImplemented,
    describeConfigs: notImplemented,
    listGroups: notImplemented,
    fetchOffsets: notImplemented,
    fetchTopicOffsets: notImplemented,
    ...overrides,
  } as KafkaAdminClient;
}

const profile: ConnectionProfile = {
  name: 'local-cluster',
  brokers: ['localhost:9091'],
  sasl: null,
  ssl: false,
  clientId: 'kafka-lag-monitor',
};

test('connect transitions idle -> connected and exposes an AdminService', async () => {
  const client = createFakeAdminClient();
  const manager = new ConnectionManager(async () => client);

  assert.equal(manager.getState(profile.name).status, 'idle');

  await manager.connect(profile);

  assert.equal(manager.getState(profile.name).status, 'connected');
  assert.ok(manager.getAdminService(profile.name));
});

test('connect sets status to error with the failure message when connect() rejects', async () => {
  const client = createFakeAdminClient({
    connect: async () => {
      throw new Error('ECONNREFUSED');
    },
  });
  const manager = new ConnectionManager(async () => client);

  await assert.rejects(() => manager.connect(profile), /ECONNREFUSED/);

  assert.deepEqual(manager.getState(profile.name), { status: 'error', error: 'ECONNREFUSED' });
  assert.equal(manager.getAdminService(profile.name), undefined);
});

test('disconnect resets status to idle and re-creates the client on the next connect', async () => {
  let createCount = 0;
  const manager = new ConnectionManager(async () => {
    createCount += 1;
    return createFakeAdminClient();
  });

  await manager.connect(profile);
  await manager.disconnect(profile.name);

  assert.equal(manager.getState(profile.name).status, 'idle');
  assert.equal(manager.getAdminService(profile.name), undefined);

  await manager.connect(profile);

  assert.equal(createCount, 2);
});

test('getState returns idle for a profile that has never been connected', () => {
  const manager = new ConnectionManager(async () => createFakeAdminClient());
  assert.deepEqual(manager.getState('never-seen'), { status: 'idle' });
});

test('reconnect discards the old client, creates a new one, and reconnects', async () => {
  let disconnectedOld = false;
  let createCount = 0;
  const manager = new ConnectionManager(async () => {
    createCount += 1;
    if (createCount === 1) {
      return createFakeAdminClient({
        disconnect: async () => {
          disconnectedOld = true;
        },
      });
    }
    return createFakeAdminClient();
  });

  await manager.connect(profile);
  assert.equal(manager.getState(profile.name).status, 'connected');

  await manager.reconnect(profile);

  assert.equal(createCount, 2);
  assert.ok(disconnectedOld);
  assert.equal(manager.getState(profile.name).status, 'connected');
});

test('reconnect works when the profile was never connected', async () => {
  const manager = new ConnectionManager(async () => createFakeAdminClient());

  await manager.reconnect(profile);

  assert.equal(manager.getState(profile.name).status, 'connected');
});

test('reconnect sets status to error when the new client fails to connect', async () => {
  let createCount = 0;
  const manager = new ConnectionManager(async () => {
    createCount += 1;
    if (createCount === 1) return createFakeAdminClient();
    return createFakeAdminClient({
      connect: async () => {
        throw new Error('ECONNREFUSED');
      },
    });
  });

  await manager.connect(profile);
  await assert.rejects(() => manager.reconnect(profile), /ECONNREFUSED/);

  assert.deepEqual(manager.getState(profile.name), { status: 'error', error: 'ECONNREFUSED' });
});

test('a stale connect() failure does not overwrite a newer reconnect() success', async () => {
  let rejectFirstConnect: (err: Error) => void = () => {};
  const firstClient = createFakeAdminClient({
    connect: () =>
      new Promise<void>((_resolve, reject) => {
        rejectFirstConnect = reject;
      }),
  });
  const secondClient = createFakeAdminClient();
  let createCount = 0;
  const manager = new ConnectionManager(async () => {
    createCount += 1;
    return createCount === 1 ? firstClient : secondClient;
  });

  const connectPromise = manager.connect(profile).catch(() => undefined);

  await manager.reconnect(profile);
  assert.equal(manager.getState(profile.name).status, 'connected');

  rejectFirstConnect(new Error('ECONNREFUSED'));
  await connectPromise;

  assert.equal(manager.getState(profile.name).status, 'connected');
});
