import { KafkaProducerClient, ProducerSendResult } from './producerClient';

export interface HeaderEntry {
  key: string;
  value: string;
}

export interface ProduceRequest {
  topic: string;
  partition: number | null;
  key: string;
  value: string;
  headers: HeaderEntry[];
}

export class ProducerService {
  constructor(private readonly client: KafkaProducerClient) {}

  async send(request: ProduceRequest): Promise<ProducerSendResult> {
    const headers: Record<string, string> = {};
    for (const header of request.headers) {
      if (header.key === '') continue;
      headers[header.key] = header.value;
    }
    return this.client.send({
      topic: request.topic,
      partition: request.partition ?? undefined,
      key: request.key === '' ? null : request.key,
      value: request.value,
      headers,
    });
  }
}
