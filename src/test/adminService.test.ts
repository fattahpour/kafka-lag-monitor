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

test('listConsumerGroups filters out non-consumer protocol groups', async () => {
  const admin = createFakeAdminClient({
    listGroups: async () => ({
      groups: [
        { groupId: 'order-service', protocolType: 'consumer' },
        { groupId: 'kafka-connect-cluster', protocolType: '' },
      ],
    }),
  });

  const result = await new AdminService(admin).listConsumerGroups();

  assert.deepEqual(result, [{ groupId: 'order-service' }]);
});

test('getGroupLag computes per-partition and total lag, including not-started partitions', async () => {
  const admin = createFakeAdminClient({
    fetchOffsets: async ({ groupId }) => {
      assert.equal(groupId, 'order-service');
      return [
        {
          topic: 'orders.events',
          partitions: [
            { partition: 0, offset: '401' },
            { partition: 1, offset: '-1' },
          ],
        },
      ];
    },
    fetchTopicOffsets: async (topic) => {
      assert.equal(topic, 'orders.events');
      return [
        { partition: 0, offset: '0', high: '600', low: '0' },
        { partition: 1, offset: '0', high: '220', low: '0' },
      ];
    },
  });

  const result = await new AdminService(admin).getGroupLag('order-service');

  assert.equal(result.length, 1);
  assert.equal(result[0].topic, 'orders.events');
  assert.equal(result[0].totalLag, 199 + 220);
  assert.deepEqual(result[0].partitions[0], {
    partition: 0,
    currentOffset: 401,
    endOffset: 600,
    lag: 199,
    status: 'lag',
  });
  assert.deepEqual(result[0].partitions[1], {
    partition: 1,
    currentOffset: 0,
    endOffset: 220,
    lag: 220,
    status: 'not-started',
  });
});

test('getGroupLag returns an empty array for a group with no committed offsets', async () => {
  const admin = createFakeAdminClient({
    fetchOffsets: async () => [],
  });

  const result = await new AdminService(admin).getGroupLag('idle-group');

  assert.deepEqual(result, []);
});

test('getTopicOffsets maps partition/low/high to numbers', async () => {
  const admin = createFakeAdminClient({
    fetchTopicOffsets: async (topic) => {
      assert.equal(topic, 'orders.events');
      return [
        { partition: 0, offset: '600', high: '600', low: '0' },
        { partition: 1, offset: '220', high: '220', low: '20' },
      ];
    },
  });

  const result = await new AdminService(admin).getTopicOffsets('orders.events');

  assert.deepEqual(result, [
    { partition: 0, low: 0, high: 600 },
    { partition: 1, low: 20, high: 220 },
  ]);
});
