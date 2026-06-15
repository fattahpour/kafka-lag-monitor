import assert from 'node:assert/strict';
import test from 'node:test';
import { ConnectionManager } from '../connection/connectionManager';
import { KafkaAdminClient } from '../kafka/adminClient';
import { KafkaProducerClient } from '../kafka/producerClient';
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

function createFakeProducerClient(overrides: Partial<KafkaProducerClient> = {}): KafkaProducerClient {
  return {
    connect: async () => {},
    disconnect: async () => {},
    send: async () => ({ partition: 0, offset: '0' }),
    ...overrides,
  };
}

const fakeProducerFactory = async () => createFakeProducerClient();

const profile: ConnectionProfile = {
  name: 'local-cluster',
  brokers: ['localhost:9091'],
  sasl: null,
  ssl: false,
  clientId: 'kafka-lag-monitor',
};

test('connect transitions idle -> connected and exposes an AdminService', async () => {
  const client = createFakeAdminClient();
  const manager = new ConnectionManager(async () => client, fakeProducerFactory);

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
  const manager = new ConnectionManager(async () => client, fakeProducerFactory);

  await assert.rejects(() => manager.connect(profile), /ECONNREFUSED/);

  assert.deepEqual(manager.getState(profile.name), { status: 'error', error: 'ECONNREFUSED' });
  assert.equal(manager.getAdminService(profile.name), undefined);
});

test('disconnect resets status to idle and re-creates the client on the next connect', async () => {
  let createCount = 0;
  const manager = new ConnectionManager(async () => {
    createCount += 1;
    return createFakeAdminClient();
  }, fakeProducerFactory);

  await manager.connect(profile);
  await manager.disconnect(profile.name);

  assert.equal(manager.getState(profile.name).status, 'idle');
  assert.equal(manager.getAdminService(profile.name), undefined);

  await manager.connect(profile);

  assert.equal(createCount, 2);
});

test('getState returns idle for a profile that has never been connected', () => {
  const manager = new ConnectionManager(async () => createFakeAdminClient(), fakeProducerFactory);
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
  }, fakeProducerFactory);

  await manager.connect(profile);
  assert.equal(manager.getState(profile.name).status, 'connected');

  await manager.reconnect(profile);

  assert.equal(createCount, 2);
  assert.ok(disconnectedOld);
  assert.equal(manager.getState(profile.name).status, 'connected');
});

test('reconnect works when the profile was never connected', async () => {
  const manager = new ConnectionManager(async () => createFakeAdminClient(), fakeProducerFactory);

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
  }, fakeProducerFactory);

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
  }, fakeProducerFactory);

  const connectPromise = manager.connect(profile).catch(() => undefined);

  await manager.reconnect(profile);
  assert.equal(manager.getState(profile.name).status, 'connected');

  rejectFirstConnect(new Error('ECONNREFUSED'));
  await connectPromise;

  assert.equal(manager.getState(profile.name).status, 'connected');
});

test('disconnect() during an in-flight reconnect() leaves status idle', async () => {
  let resolveReconnectConnect: () => void = () => {};
  let connectCalled: () => void = () => {};
  const connectCalledPromise = new Promise<void>((resolve) => {
    connectCalled = resolve;
  });
  const firstClient = createFakeAdminClient();
  const secondClient = createFakeAdminClient({
    connect: () =>
      new Promise<void>((resolve) => {
        resolveReconnectConnect = resolve;
        connectCalled();
      }),
  });
  let createCount = 0;
  const manager = new ConnectionManager(async () => {
    createCount += 1;
    return createCount === 1 ? firstClient : secondClient;
  }, fakeProducerFactory);

  await manager.connect(profile);
  assert.equal(manager.getState(profile.name).status, 'connected');

  const reconnectPromise = manager.reconnect(profile);

  // Wait until reconnect() has reached its in-flight client.connect() call.
  await connectCalledPromise;

  await manager.disconnect(profile.name);
  assert.equal(manager.getState(profile.name).status, 'idle');

  resolveReconnectConnect();
  await reconnectPromise;

  assert.equal(manager.getState(profile.name).status, 'idle');
});

test('getProducerService returns undefined when not connected', async () => {
  const manager = new ConnectionManager(async () => createFakeAdminClient(), fakeProducerFactory);

  assert.equal(await manager.getProducerService(profile), undefined);
});

test('getProducerService creates and connects a producer client lazily, and reuses it on subsequent calls', async () => {
  let createCount = 0;
  let connected = 0;
  const manager = new ConnectionManager(
    async () => createFakeAdminClient(),
    async () => {
      createCount += 1;
      return createFakeProducerClient({
        connect: async () => {
          connected += 1;
        },
      });
    },
  );

  await manager.connect(profile);

  const first = await manager.getProducerService(profile);
  const second = await manager.getProducerService(profile);

  assert.ok(first);
  assert.ok(second);
  assert.equal(createCount, 1);
  assert.equal(connected, 1);
});

test('disconnect disposes the cached producer client', async () => {
  let createCount = 0;
  let disconnected = 0;
  const manager = new ConnectionManager(
    async () => createFakeAdminClient(),
    async () => {
      createCount += 1;
      return createFakeProducerClient({
        disconnect: async () => {
          disconnected += 1;
        },
      });
    },
  );

  await manager.connect(profile);
  await manager.getProducerService(profile);

  await manager.disconnect(profile.name);

  assert.equal(disconnected, 1);

  await manager.connect(profile);
  await manager.getProducerService(profile);

  assert.equal(createCount, 2);
});

test('reconnect disposes the cached producer client', async () => {
  let createCount = 0;
  let disconnected = 0;
  const manager = new ConnectionManager(
    async () => createFakeAdminClient(),
    async () => {
      createCount += 1;
      return createFakeProducerClient({
        disconnect: async () => {
          disconnected += 1;
        },
      });
    },
  );

  await manager.connect(profile);
  await manager.getProducerService(profile);

  await manager.reconnect(profile);

  assert.equal(disconnected, 1);

  await manager.getProducerService(profile);

  assert.equal(createCount, 2);
});

test('a stale getProducerService() producer-create does not get cached after a concurrent reconnect()', async () => {
  let resolveFirstProducerConnect: () => void = () => {};
  let firstProducerDisconnected = false;
  let createCount = 0;
  const manager = new ConnectionManager(
    async () => createFakeAdminClient(),
    async () => {
      createCount += 1;
      if (createCount === 1) {
        return createFakeProducerClient({
          connect: () =>
            new Promise<void>((resolve) => {
              resolveFirstProducerConnect = resolve;
            }),
          disconnect: async () => {
            firstProducerDisconnected = true;
          },
        });
      }
      return createFakeProducerClient();
    },
  );

  await manager.connect(profile);

  const getProducerPromise = manager.getProducerService(profile);

  await manager.reconnect(profile);

  resolveFirstProducerConnect();
  const result = await getProducerPromise;

  assert.ok(result);
  assert.ok(firstProducerDisconnected);
  assert.equal(createCount, 2);
});
