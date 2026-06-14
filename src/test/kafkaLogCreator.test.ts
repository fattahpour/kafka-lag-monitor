import assert from 'node:assert/strict';
import test from 'node:test';
import { createKafkaLogCreator } from '../logging/kafkaLogCreator';

test('formats a basic log entry as "[LABEL] namespace: message"', () => {
  const lines: string[] = [];
  const log = createKafkaLogCreator((line) => lines.push(line))(1);

  log({
    namespace: 'CONNECTION',
    level: 1,
    label: 'ERROR',
    log: { message: 'Connection error' },
  });

  assert.deepEqual(lines, ['[ERROR] CONNECTION: Connection error']);
});

test('appends extra log fields as key=value pairs', () => {
  const lines: string[] = [];
  const log = createKafkaLogCreator((line) => lines.push(line))(2);

  log({
    namespace: 'CONNECTION',
    level: 2,
    label: 'WARN',
    log: { message: 'Retrying', broker: 'localhost:9092', retryCount: 2 },
  });

  assert.deepEqual(lines, ['[WARN] CONNECTION: Retrying (broker=localhost:9092, retryCount=2)']);
});
