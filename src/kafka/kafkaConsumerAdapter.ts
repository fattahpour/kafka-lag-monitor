import { Kafka } from 'kafkajs';
import { randomUUID } from 'node:crypto';
import { KafkaConsumerClient, RawKafkaMessage } from './consumerClient';

const FETCH_TIMEOUT_MS = 15000;

function mapHeaders(
  headers?: Record<string, Buffer | string | (Buffer | string)[] | undefined>,
): Record<string, string> {
  const result: Record<string, string> = {};
  if (!headers) return result;
  for (const [key, value] of Object.entries(headers)) {
    if (value === undefined) continue;
    const values = Array.isArray(value) ? value : [value];
    result[key] = values.map((v) => v.toString()).join(', ');
  }
  return result;
}

export function createKafkaConsumerClient(kafka: Kafka): KafkaConsumerClient {
  return {
    fetchMessages: async ({ topic, partition, fromOffset, toOffset }) => {
      if (fromOffset >= toOffset) return [];

      const consumer = kafka.consumer({ groupId: `kafka-lag-monitor-browse-${randomUUID()}` });
      const messages: RawKafkaMessage[] = [];

      try {
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: false });

        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(() => {
            reject(new Error(`Timed out waiting for messages from "${topic}" partition ${partition}`));
          }, FETCH_TIMEOUT_MS);

          const finish = () => {
            clearTimeout(timer);
            resolve();
          };

          consumer.on(consumer.events.GROUP_JOIN, () => {
            consumer.seek({ topic, partition, offset: String(fromOffset) });
          });

          consumer.on(consumer.events.CRASH, ({ payload }) => {
            clearTimeout(timer);
            reject(payload.error);
          });

          consumer
            .run({
              autoCommit: false,
              eachBatch: async ({ batch, heartbeat }) => {
                if (batch.partition === partition) {
                  for (const message of batch.messages) {
                    const offset = Number(message.offset);
                    if (offset >= fromOffset && offset < toOffset) {
                      messages.push({
                        offset: message.offset,
                        timestamp: message.timestamp,
                        key: message.key ? message.key.toString('utf8') : null,
                        value: message.value ? message.value.toString('utf8') : null,
                        headers: mapHeaders(message.headers),
                      });
                    }
                  }
                  if (!batch.isEmpty() && Number(batch.lastOffset()) >= toOffset - 1) {
                    finish();
                  }
                }
                await heartbeat();
              },
            })
            .catch(reject);
        });
      } finally {
        await consumer.stop().catch(() => undefined);
        await consumer.disconnect().catch(() => undefined);
      }

      return messages.sort((a, b) => Number(a.offset) - Number(b.offset));
    },
  };
}
