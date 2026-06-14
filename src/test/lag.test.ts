import assert from 'node:assert/strict';
import test from 'node:test';
import { aggregateTopicLag, computePartitionLag, lagSeverity } from '../kafka/lag';

test('computePartitionLag reports lag when the group is behind', () => {
  const result = computePartitionLag(0, 401, 600);
  assert.deepEqual(result, { partition: 0, currentOffset: 401, endOffset: 600, lag: 199, status: 'lag' });
});

test('computePartitionLag reports ok when fully caught up', () => {
  const result = computePartitionLag(1, 600, 600);
  assert.deepEqual(result, { partition: 1, currentOffset: 600, endOffset: 600, lag: 0, status: 'ok' });
});

test('computePartitionLag reports not-started when there is no committed offset', () => {
  const result = computePartitionLag(2, null, 600);
  assert.deepEqual(result, { partition: 2, currentOffset: 0, endOffset: 600, lag: 600, status: 'not-started' });
});

test('computePartitionLag reports ok for an empty partition with no committed offset', () => {
  const result = computePartitionLag(3, null, 0);
  assert.deepEqual(result, { partition: 3, currentOffset: 0, endOffset: 0, lag: 0, status: 'ok' });
});

test('aggregateTopicLag sums lag across partitions', () => {
  const partitions = [
    computePartitionLag(0, 401, 600),
    computePartitionLag(1, 600, 600),
    computePartitionLag(2, null, 220),
  ];

  const result = aggregateTopicLag('orders.events', partitions);

  assert.equal(result.topic, 'orders.events');
  assert.equal(result.partitions, partitions);
  assert.equal(result.totalLag, 199 + 0 + 220);
});

test('lagSeverity boundaries', () => {
  assert.equal(lagSeverity(99, 100, 1000), 'none');
  assert.equal(lagSeverity(100, 100, 1000), 'warning');
  assert.equal(lagSeverity(999, 100, 1000), 'warning');
  assert.equal(lagSeverity(1000, 100, 1000), 'critical');
  assert.equal(lagSeverity(0, 100, 1000), 'none');
});
