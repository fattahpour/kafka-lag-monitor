import assert from 'node:assert/strict';
import test from 'node:test';
import { renderMessageBrowserHtml, toMessageBrowserData } from '../webviews/messageBrowserPanel';
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

test('renderMessageBrowserHtml includes the topic name and control element ids', () => {
  const data = toMessageBrowserData('orders.events', 3, buildPage());
  const html = renderMessageBrowserHtml('orders.events', data);

  assert.match(html, /<title>Messages: orders\.events<\/title>/);
  assert.match(html, /id="title"/);
  assert.match(html, /id="partition"/);
  assert.match(html, /id="earliest"/);
  assert.match(html, /id="prev"/);
  assert.match(html, /id="next"/);
  assert.match(html, /id="latest"/);
  assert.match(html, /id="refresh"/);
  assert.match(html, /id="banner"/);
  assert.match(html, /id="windowInfo"/);
  assert.match(html, /id="rows"/);
});

test('renderMessageBrowserHtml embeds the serialized initial data and VALUE_TRUNCATE_LENGTH', () => {
  const data = toMessageBrowserData('orders.events', 3, buildPage());
  const html = renderMessageBrowserHtml('orders.events', data);

  assert.match(html, /<script>[\s\S]*const initialData = \{[\s\S]*"topic":"orders\.events"[\s\S]*\}[\s\S]*<\/script>/);
  assert.match(html, /const VALUE_TRUNCATE_LENGTH = 300;/);
});

test('renderMessageBrowserHtml escapes "</script>" sequences inside the serialized initial data', () => {
  const page = buildPage({
    messages: [
      { offset: 150, timestamp: '1700000000000', key: '</script><script>alert(1)</script>', value: null, headers: {} },
    ],
  });
  const data = toMessageBrowserData('orders.events', 3, page);
  const html = renderMessageBrowserHtml('orders.events', data);

  const closingTagCount = (html.match(/<\/script>/g) || []).length;
  assert.equal(closingTagCount, 1);
});

test('renderMessageBrowserHtml wires the partition select and nav buttons to postMessage', () => {
  const data = toMessageBrowserData('orders.events', 3, buildPage());
  const html = renderMessageBrowserHtml('orders.events', data);

  assert.match(html, /postMessage\(\{ type: 'setPartition', partition: Number\(event\.target\.value\) \}\)/);
  assert.match(html, /postMessage\(\{ type: 'nav', action: 'earliest' \}\)/);
  assert.match(html, /postMessage\(\{ type: 'nav', action: 'prev' \}\)/);
  assert.match(html, /postMessage\(\{ type: 'nav', action: 'next' \}\)/);
  assert.match(html, /postMessage\(\{ type: 'nav', action: 'latest' \}\)/);
  assert.match(html, /postMessage\(\{ type: 'nav', action: 'refresh' \}\)/);
});
