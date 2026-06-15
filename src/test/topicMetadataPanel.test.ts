import assert from 'node:assert/strict';
import test from 'node:test';
import { renderErrorHtml, renderTopicMetadataHtml } from '../webviews/topicMetadataPanel';
import { ConfigEntry, TopicMetadata } from '../kafka/adminService';

const metadata: TopicMetadata = {
  name: 'orders.events',
  partitions: [
    { partitionId: 0, leader: 1, replicas: [1, 2, 3], isr: [1, 2, 3] },
    { partitionId: 1, leader: 2, replicas: [2, 3, 1], isr: [2, 3] },
  ],
};

const configEntries: ConfigEntry[] = [
  { name: 'retention.ms', value: '604800000', isDefault: false },
  { name: 'cleanup.policy', value: 'delete', isDefault: true },
];

test('renderTopicMetadataHtml includes the topic name and a refresh button wired to postMessage', () => {
  const html = renderTopicMetadataHtml('orders.events', metadata, configEntries);
  assert.match(html, /<h2>orders\.events<\/h2>/);
  assert.match(html, /id="refresh"/);
  assert.match(html, /postMessage\(\{ type: 'refresh' \}\)/);
});

test('renderTopicMetadataHtml renders a partition row per partition with leader/replicas/isr', () => {
  const html = renderTopicMetadataHtml('orders.events', metadata, configEntries);
  assert.match(html, /<td>0<\/td><td>1<\/td><td>1, 2, 3<\/td><td>1, 2, 3<\/td>/);
  assert.match(html, /<td>1<\/td><td>2<\/td><td>2, 3, 1<\/td><td>2, 3<\/td>/);
});

test('renderTopicMetadataHtml renders a config row per entry with its default flag', () => {
  const html = renderTopicMetadataHtml('orders.events', metadata, configEntries);
  assert.match(html, /<td>retention\.ms<\/td><td>604800000<\/td><td>No<\/td>/);
  assert.match(html, /<td>cleanup\.policy<\/td><td>delete<\/td><td>Yes<\/td>/);
});

test('renderTopicMetadataHtml escapes HTML in the topic name and config entries', () => {
  const html = renderTopicMetadataHtml('<script>', metadata, [{ name: '<x>', value: '<y>', isDefault: false }]);
  assert.match(html, /<h2>&lt;script&gt;<\/h2>/);
  assert.match(html, /<td>&lt;x&gt;<\/td><td>&lt;y&gt;<\/td><td>No<\/td>/);
});

test('renderErrorHtml escapes and includes the error message', () => {
  const html = renderErrorHtml('Not connected — <b>retry</b>');
  assert.match(html, /<p>Not connected — &lt;b&gt;retry&lt;\/b&gt;<\/p>/);
});
