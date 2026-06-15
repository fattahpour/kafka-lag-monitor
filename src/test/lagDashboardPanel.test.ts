import assert from 'node:assert/strict';
import test from 'node:test';
import { toDashboardData } from '../webviews/lagDashboardPanel';
import { TopicLag } from '../kafka/lag';
import { Thresholds } from '../connection/profileStore';

const thresholds: Thresholds = { warning: 100, critical: 1000 };

test('toDashboardData computes percentConsumed and severity for a partially-consumed partition', () => {
  const topicLags: TopicLag[] = [
    {
      topic: 'orders.events',
      totalLag: 199,
      partitions: [{ partition: 0, currentOffset: 401, endOffset: 600, lag: 199, status: 'lag' }],
    },
  ];

  const data = toDashboardData('order-service', topicLags, thresholds);

  assert.equal(data.topics[0].partitions[0].percentConsumed, 67);
  assert.equal(data.topics[0].partitions[0].severity, 'warning');
});

test('toDashboardData computes 100% and severity none for a fully-consumed partition', () => {
  const topicLags: TopicLag[] = [
    {
      topic: 'orders.events',
      totalLag: 0,
      partitions: [{ partition: 1, currentOffset: 600, endOffset: 600, lag: 0, status: 'ok' }],
    },
  ];

  const data = toDashboardData('order-service', topicLags, thresholds);

  assert.equal(data.topics[0].partitions[0].percentConsumed, 100);
  assert.equal(data.topics[0].partitions[0].severity, 'none');
});

test('toDashboardData computes 0% for a partition that has not started consuming', () => {
  const topicLags: TopicLag[] = [
    {
      topic: 'orders.events',
      totalLag: 600,
      partitions: [{ partition: 2, currentOffset: 0, endOffset: 600, lag: 600, status: 'not-started' }],
    },
  ];

  const data = toDashboardData('order-service', topicLags, thresholds);

  assert.equal(data.topics[0].partitions[0].percentConsumed, 0);
  assert.equal(data.topics[0].partitions[0].severity, 'warning');
});

test('toDashboardData treats an empty partition (endOffset 0) as 100% consumed', () => {
  const topicLags: TopicLag[] = [
    {
      topic: 'orders.events',
      totalLag: 0,
      partitions: [{ partition: 3, currentOffset: 0, endOffset: 0, lag: 0, status: 'ok' }],
    },
  ];

  const data = toDashboardData('order-service', topicLags, thresholds);

  assert.equal(data.topics[0].partitions[0].percentConsumed, 100);
  assert.equal(data.topics[0].partitions[0].severity, 'none');
});

test('toDashboardData applies severity boundaries per partition', () => {
  const topicLags: TopicLag[] = [
    {
      topic: 'orders.events',
      totalLag: 99 + 100 + 1000,
      partitions: [
        { partition: 0, currentOffset: 901, endOffset: 1000, lag: 99, status: 'lag' },
        { partition: 1, currentOffset: 900, endOffset: 1000, lag: 100, status: 'lag' },
        { partition: 2, currentOffset: 0, endOffset: 1000, lag: 1000, status: 'not-started' },
      ],
    },
  ];

  const data = toDashboardData('order-service', topicLags, thresholds);

  assert.equal(data.topics[0].partitions[0].severity, 'none');
  assert.equal(data.topics[0].partitions[1].severity, 'warning');
  assert.equal(data.topics[0].partitions[2].severity, 'critical');
});

test('toDashboardData sums totalLag across topics, counts over-threshold partitions, and derives overall severity', () => {
  const topicLags: TopicLag[] = [
    {
      topic: 'orders.events',
      totalLag: 199,
      partitions: [
        { partition: 0, currentOffset: 401, endOffset: 600, lag: 199, status: 'lag' },
        { partition: 1, currentOffset: 600, endOffset: 600, lag: 0, status: 'ok' },
      ],
    },
    {
      topic: 'payments.events',
      totalLag: 1000,
      partitions: [{ partition: 0, currentOffset: 0, endOffset: 1000, lag: 1000, status: 'not-started' }],
    },
  ];

  const data = toDashboardData('order-service', topicLags, thresholds);

  assert.equal(data.groupId, 'order-service');
  assert.equal(data.totalLag, 1199);
  assert.equal(data.severity, 'critical');
  assert.equal(data.overThresholdCount, 2);
  assert.equal(data.topics.length, 2);
  assert.equal(data.topics[0].topic, 'orders.events');
  assert.equal(data.topics[0].totalLag, 199);
  assert.equal(data.topics[1].topic, 'payments.events');
});

test('toDashboardData handles an empty topic list', () => {
  const data = toDashboardData('order-service', [], thresholds);

  assert.equal(data.totalLag, 0);
  assert.equal(data.severity, 'none');
  assert.equal(data.overThresholdCount, 0);
  assert.deepEqual(data.topics, []);
});
