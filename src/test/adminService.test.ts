import assert from 'node:assert/strict';
import test from 'node:test';
import { AdminService } from '../kafka/adminService';
import { KafkaAdminClient } from '../kafka/adminClient';

function createFakeAdminClient(overrides: Partial<KafkaAdminClient>): KafkaAdminClient {
  const notImplemented = () => {
    throw new Error('not implemented in fake');
  };
  return {
    connect: notImplemented,
    disconnect: notImplemented,
    listTopics: notImplemented,
    fetchTopicMetadata: notImplemented,
    describeConfigs: notImplemented,
    listGroups: notImplemented,
    fetchOffsets: notImplemented,
    fetchTopicOffsets: notImplemented,
    ...overrides,
  } as KafkaAdminClient;
}

test('listTopics filters internal topics and reports partition counts', async () => {
  const admin = createFakeAdminClient({
    listTopics: async () => ['orders.events', '__consumer_offsets', 'payments.dlq'],
    fetchTopicMetadata: async ({ topics }) => ({
      topics: topics.map((name) => ({
        name,
        partitions: name === 'orders.events' ? [{}, {}, {}, {}, {}, {}] : [{}, {}, {}],
      })) as any,
    }),
  });

  const result = await new AdminService(admin).listTopics();

  assert.deepEqual(result, [
    { name: 'orders.events', partitionCount: 6 },
    { name: 'payments.dlq', partitionCount: 3 },
  ]);
});

test('listTopics returns an empty array when there are no user topics', async () => {
  const admin = createFakeAdminClient({
    listTopics: async () => ['__consumer_offsets'],
  });

  const result = await new AdminService(admin).listTopics();

  assert.deepEqual(result, []);
});

test('getTopicMetadata maps partition leader, replicas, and ISR', async () => {
  const admin = createFakeAdminClient({
    fetchTopicMetadata: async () => ({
      topics: [
        {
          name: 'orders.events',
          partitions: [
            { partitionId: 0, leader: 1, replicas: [1, 2, 3], isr: [1, 2, 3] },
            { partitionId: 1, leader: 2, replicas: [2, 3, 1], isr: [2, 3] },
          ],
        },
      ],
    }),
  });

  const result = await new AdminService(admin).getTopicMetadata('orders.events');

  assert.deepEqual(result, {
    name: 'orders.events',
    partitions: [
      { partitionId: 0, leader: 1, replicas: [1, 2, 3], isr: [1, 2, 3] },
      { partitionId: 1, leader: 2, replicas: [2, 3, 1], isr: [2, 3] },
    ],
  });
});

test('getTopicMetadata throws when the topic is missing from the response', async () => {
  const admin = createFakeAdminClient({
    fetchTopicMetadata: async () => ({ topics: [] }),
  });

  await assert.rejects(() => new AdminService(admin).getTopicMetadata('missing-topic'), /not found/);
});

test('getTopicConfig maps config entries', async () => {
  const admin = createFakeAdminClient({
    describeConfigs: async () => ({
      resources: [
        {
          configEntries: [
            { configName: 'retention.ms', configValue: '604800000', isDefault: false },
            { configName: 'cleanup.policy', configValue: 'delete', isDefault: true },
          ],
        },
      ],
    }),
  });

  const result = await new AdminService(admin).getTopicConfig('orders.events');

  assert.deepEqual(result, [
    { name: 'retention.ms', value: '604800000', isDefault: false },
    { name: 'cleanup.policy', value: 'delete', isDefault: true },
  ]);
});

test('getTopicConfig returns an empty array when no resource is returned', async () => {
  const admin = createFakeAdminClient({
    describeConfigs: async () => ({ resources: [] }),
  });

  const result = await new AdminService(admin).getTopicConfig('orders.events');

  assert.deepEqual(result, []);
});
