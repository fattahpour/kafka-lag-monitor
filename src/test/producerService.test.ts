import assert from 'node:assert/strict';
import test from 'node:test';
import { ProducerService } from '../kafka/producerService';
import { KafkaProducerClient } from '../kafka/producerClient';

function createFakeProducerClient(): { client: KafkaProducerClient; calls: Parameters<KafkaProducerClient['send']>[0][] } {
  const calls: Parameters<KafkaProducerClient['send']>[0][] = [];
  const client: KafkaProducerClient = {
    connect: async () => {},
    disconnect: async () => {},
    send: async (args) => {
      calls.push(args);
      return { partition: 0, offset: '42' };
    },
  };
  return { client, calls };
}

test('send drops header rows with an empty key and keeps rows with a non-empty key', async () => {
  const { client, calls } = createFakeProducerClient();
  const service = new ProducerService(client);

  await service.send({
    topic: 'orders.events',
    partition: null,
    key: '',
    value: 'payload',
    headers: [
      { key: '', value: 'dropped' },
      { key: 'trace-id', value: 'abc-123' },
    ],
  });

  assert.deepEqual(calls[0].headers, { 'trace-id': 'abc-123' });
});

test('send converts an empty key to null, and passes a non-empty key through unchanged', async () => {
  const { client, calls } = createFakeProducerClient();
  const service = new ProducerService(client);

  await service.send({ topic: 'orders.events', partition: null, key: '', value: 'payload', headers: [] });
  await service.send({ topic: 'orders.events', partition: null, key: 'order-1', value: 'payload', headers: [] });

  assert.equal(calls[0].key, null);
  assert.equal(calls[1].key, 'order-1');
});

test('send converts partition null to undefined, and passes a numeric partition through unchanged', async () => {
  const { client, calls } = createFakeProducerClient();
  const service = new ProducerService(client);

  await service.send({ topic: 'orders.events', partition: null, key: 'order-1', value: 'payload', headers: [] });
  await service.send({ topic: 'orders.events', partition: 2, key: 'order-1', value: 'payload', headers: [] });

  assert.equal(calls[0].partition, undefined);
  assert.equal(calls[1].partition, 2);
});

test('send passes topic and value through unchanged, and returns the client result unchanged', async () => {
  const { client, calls } = createFakeProducerClient();
  const service = new ProducerService(client);

  const result = await service.send({ topic: 'orders.events', partition: null, key: 'order-1', value: 'payload', headers: [] });

  assert.equal(calls[0].topic, 'orders.events');
  assert.equal(calls[0].value, 'payload');
  assert.deepEqual(result, { partition: 0, offset: '42' });
});
