import assert from 'node:assert/strict';
import test from 'node:test';
import { toMessageBrowserData } from '../webviews/messageBrowserPanel';
import { MessagePage } from '../kafka/consumerService';

function buildPage(overrides: Partial<MessagePage> = {}): MessagePage {
  return {
    partition: 0,
    lowWatermark: 0,
    highWatermark: 200,
    window: { from: 150, to: 200 },
    messages: [],
    ...overrides,
  };
}

test('toMessageBrowserData pretty-prints JSON values, passes through non-JSON values, and keeps nulls', () => {
  const page = buildPage({
    messages: [
      { offset: 150, timestamp: '1700000000000', key: 'order-1', value: '{"id":1,"status":"ok"}', headers: {} },
      { offset: 151, timestamp: '1700000000001', key: 'order-2', value: 'not json', headers: {} },
      { offset: 152, timestamp: '1700000000002', key: null, value: null, headers: {} },
    ],
  });

  const data = toMessageBrowserData('orders.events', 3, page);

  assert.equal(data.messages[0].value, JSON.stringify({ id: 1, status: 'ok' }, null, 2));
  assert.equal(data.messages[1].value, 'not json');
  assert.equal(data.messages[2].key, null);
  assert.equal(data.messages[2].value, null);
});

test('toMessageBrowserData converts headers to an ordered array and passes through partition/window/watermarks', () => {
  const page = buildPage({
    partition: 2,
    lowWatermark: 10,
    highWatermark: 60,
    window: { from: 10, to: 60 },
    messages: [{ offset: 10, timestamp: '1700000000000', key: 'k', value: 'v', headers: { a: '1', b: '2' } }],
  });

  const data = toMessageBrowserData('orders.events', 3, page);

  assert.equal(data.topic, 'orders.events');
  assert.equal(data.partition, 2);
  assert.equal(data.partitionCount, 3);
  assert.equal(data.lowWatermark, 10);
  assert.equal(data.highWatermark, 60);
  assert.deepEqual(data.window, { from: 10, to: 60 });
  assert.deepEqual(data.messages[0].headers, [
    { key: 'a', value: '1' },
    { key: 'b', value: '2' },
  ]);
});
