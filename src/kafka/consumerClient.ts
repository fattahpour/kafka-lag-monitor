export interface RawKafkaMessage {
  offset: string;
  timestamp: string;
  key: string | null;
  value: string | null;
  headers: Record<string, string>;
}

export interface KafkaConsumerClient {
  fetchMessages(args: {
    topic: string;
    partition: number;
    fromOffset: number;
    toOffset: number;
  }): Promise<RawKafkaMessage[]>;
}
