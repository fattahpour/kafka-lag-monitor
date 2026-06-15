export interface ProducerSendResult {
  partition: number;
  offset: string;
}

export interface KafkaProducerClient {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  send(args: {
    topic: string;
    partition?: number;
    key: string | null;
    value: string;
    headers: Record<string, string>;
  }): Promise<ProducerSendResult>;
}
