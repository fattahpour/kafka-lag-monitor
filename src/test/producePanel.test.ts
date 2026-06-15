import assert from 'node:assert/strict';
import test from 'node:test';
import { renderProduceHtml } from '../webviews/producePanel';

test('renderProduceHtml escapes the topic name in the title and heading', () => {
  const html = renderProduceHtml('orders<events>', 3);

  assert.match(html, /<title>Produce: orders&lt;events&gt;<\/title>/);
  assert.match(html, /<h2>Produce: orders&lt;events&gt;<\/h2>/);
});

test('renderProduceHtml includes the form element ids', () => {
  const html = renderProduceHtml('orders.events', 3);

  assert.match(html, /id="partition"/);
  assert.match(html, /id="key"/);
  assert.match(html, /id="value"/);
  assert.match(html, /id="headers"/);
  assert.match(html, /id="addHeader"/);
  assert.match(html, /id="send"/);
  assert.match(html, /id="result"/);
});

test('renderProduceHtml embeds PARTITION_COUNT and builds the "Auto (by key)" option', () => {
  const html = renderProduceHtml('orders.events', 3);

  assert.match(html, /const PARTITION_COUNT = 3;/);
  assert.match(html, /autoOption\.value = '';/);
  assert.match(html, /autoOption\.textContent = 'Auto \(by key\)';/);
  assert.match(html, /for \(let i = 0; i < PARTITION_COUNT; i\+\+\)/);
});

test('renderProduceHtml wires the Send button to post a send message with the form values', () => {
  const html = renderProduceHtml('orders.events', 3);

  assert.match(html, /sendButton\.addEventListener\('click', \(\) => \{/);
  assert.match(html, /type: 'send',/);
  assert.match(html, /partition: partitionValue === '' \? null : Number\(partitionValue\),/);
  assert.match(html, /key: document\.getElementById\('key'\)\.value,/);
  assert.match(html, /value: document\.getElementById\('value'\)\.value,/);
});

test('renderProduceHtml renders success and error results from the result message', () => {
  const html = renderProduceHtml('orders.events', 3);

  assert.match(html, /if \(message\.type !== 'result'\) return;/);
  assert.match(html, /resultDiv\.className = 'success';/);
  assert.match(html, /resultDiv\.className = 'error';/);
  assert.match(html, /'Sent to partition ' \+ message\.partition \+ ', offset ' \+ message\.offset/);
});
