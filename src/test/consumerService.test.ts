import assert from 'node:assert/strict';
import test from 'node:test';
import { AdminService } from '../kafka/adminService';
import { KafkaAdminClient } from '../kafka/adminClient';
import { KafkaConsumerClient, RawKafkaMessage } from '../kafka/consumerClient';
import { computeWindow, ConsumerService } from '../kafka/consumerService';

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

function createFakeConsumerClient(overrides: Partial<KafkaConsumerClient>): KafkaConsumerClient {
  return {
    fetchMessages: async () => [],
    ...overrides,
  };
}

test('computeWindow latest and earliest for a normal partition', () => {
  assert.deepEqual(computeWindow('latest', 0, 200), { from: 150, to: 200 });
  assert.deepEqual(computeWindow('earliest', 0, 200), { from: 0, to: 50 });
});

test('computeWindow latest and earliest for an empty partition', () => {
  assert.deepEqual(computeWindow('latest', 100, 100), { from: 100, to: 100 });
  assert.deepEqual(computeWindow('earliest', 100, 100), { from: 100, to: 100 });
});

test('computeWindow latest and earliest for a partition with fewer than PAGE_SIZE messages', () => {
  assert.deepEqual(computeWindow('latest', 0, 30), { from: 0, to: 30 });
  assert.deepEqual(computeWindow('earliest', 0, 30), { from: 0, to: 30 });
});

test('computeWindow prev and next from a mid-range window', () => {
  assert.deepEqual(computeWindow('prev', 0, 200, { from: 150, to: 200 }), { from: 100, to: 150 });
  assert.deepEqual(computeWindow('next', 0, 200, { from: 100, to: 150 }), { from: 150, to: 200 });
});

test('computeWindow prev and next at the low/high watermark boundary return an empty window', () => {
  assert.deepEqual(computeWindow('prev', 0, 200, { from: 0, to: 50 }), { from: 0, to: 0 });
  assert.deepEqual(computeWindow('next', 0, 200, { from: 150, to: 200 }), { from: 200, to: 200 });
});

test('computeWindow refresh clamps the current window into the low/high range', () => {
  assert.deepEqual(computeWindow('refresh', 100, 200, { from: 0, to: 50 }), { from: 100, to: 100 });
  assert.deepEqual(computeWindow('refresh', 0, 120, { from: 100, to: 200 }), { from: 100, to: 120 });
});

test('computeWindow prev, next, and refresh fall back to latest when there is no current window', () => {
  assert.deepEqual(computeWindow('prev', 0, 200), { from: 150, to: 200 });
  assert.deepEqual(computeWindow('next', 0, 200), { from: 150, to: 200 });
  assert.deepEqual(computeWindow('refresh', 0, 200), { from: 150, to: 200 });
});

test('fetchPage maps RawKafkaMessage[] to MessageView[] and returns watermarks and window', async () => {
  const admin = createFakeAdminClient({
    fetchTopicOffsets: async () => [{ partition: 0, offset: '200', high: '200', low: '0' }],
  });
  const consumerClient = createFakeConsumerClient({
    fetchMessages: async (): Promise<RawKafkaMessage[]> => [
      { offset: '150', timestamp: '1700000000000', key: 'k1', value: 'v1', headers: { h: '1' } },
      { offset: '151', timestamp: '1700000000001', key: null, value: null, headers: {} },
    ],
  });

  const service = new ConsumerService(consumerClient, new AdminService(admin));
  const page = await service.fetchPage('orders.events', 0, 'latest');

  assert.equal(page.partition, 0);
  assert.equal(page.lowWatermark, 0);
  assert.equal(page.highWatermark, 200);
  assert.deepEqual(page.window, { from: 150, to: 200 });
  assert.deepEqual(page.messages, [
    { offset: 150, timestamp: '1700000000000', key: 'k1', value: 'v1', headers: { h: '1' } },
    { offset: 151, timestamp: '1700000000001', key: null, value: null, headers: {} },
  ]);
});

test('fetchPage throws when the requested partition is not found', async () => {
  const admin = createFakeAdminClient({
    fetchTopicOffsets: async () => [{ partition: 0, offset: '200', high: '200', low: '0' }],
  });
  const consumerClient = createFakeConsumerClient({});

  const service = new ConsumerService(consumerClient, new AdminService(admin));

  await assert.rejects(
    () => service.fetchPage('orders.events', 5, 'latest'),
    /Partition 5 not found for topic "orders\.events"/,
  );
});

test('fetchPage passes the computed window through to fetchMessages as fromOffset/toOffset', async () => {
  const admin = createFakeAdminClient({
    fetchTopicOffsets: async () => [{ partition: 0, offset: '200', high: '200', low: '0' }],
  });
  let receivedArgs: { topic: string; partition: number; fromOffset: number; toOffset: number } | undefined;
  const consumerClient = createFakeConsumerClient({
    fetchMessages: async (args) => {
      receivedArgs = args;
      return [];
    },
  });

  const service = new ConsumerService(consumerClient, new AdminService(admin));
  await service.fetchPage('orders.events', 0, 'earliest');

  assert.deepEqual(receivedArgs, { topic: 'orders.events', partition: 0, fromOffset: 0, toOffset: 50 });
});
