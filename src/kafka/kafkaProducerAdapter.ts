import { Producer } from 'kafkajs';
import { KafkaProducerClient } from './producerClient';

export function createKafkaProducerClient(producer: Producer): KafkaProducerClient {
  return {
    connect: () => producer.connect(),
    disconnect: () => producer.disconnect(),
    send: async ({ topic, partition, key, value, headers }) => {
      const [metadata] = await producer.send({
        topic,
        messages: [{ partition, key, value, headers }],
      });
      return { partition: metadata.partition, offset: metadata.baseOffset ?? '0' };
    },
  };
}
