import assert from 'node:assert/strict';
import test from 'node:test';
import { buildConnectionNode, buildGroupNode, buildPartitionNode, buildTopicNode } from '../treeView/treeItems';

test('buildConnectionNode shows a checkmark when connected and no description', () => {
  assert.deepEqual(buildConnectionNode('local-cluster', 'connected'), {
    label: 'local-cluster ✓',
    description: '',
  });
});

test('buildConnectionNode shows the error message as the description when errored', () => {
  assert.deepEqual(buildConnectionNode('local-cluster', 'error', 'ECONNREFUSED'), {
    label: 'local-cluster ⚠',
    description: 'ECONNREFUSED',
  });
});

test('buildTopicNode pluralizes the partition count', () => {
  assert.deepEqual(buildTopicNode('orders.events', 6), { label: 'orders.events', description: '6 partitions' });
  assert.deepEqual(buildTopicNode('single-partition-topic', 1), {
    label: 'single-partition-topic',
    description: '1 partition',
  });
});

test('buildGroupNode includes the total lag and severity', () => {
  assert.deepEqual(buildGroupNode('order-service', 1420, 'critical'), {
    label: 'order-service',
    description: '●1420',
    severity: 'critical',
  });
});

test('buildPartitionNode formats current/end (lag)', () => {
  assert.deepEqual(buildPartitionNode(0, 401, 600, 199), { label: 'p0: 401/600 (199)' });
});
