export interface KafkaTopicPartitionMetadata {
  partitionId: number;
  leader: number;
  replicas: number[];
  isr: number[];
}

export interface KafkaTopicMetadata {
  topics: Array<{ name: string; partitions: KafkaTopicPartitionMetadata[] }>;
}

export interface KafkaConfigEntry {
  configName: string;
  configValue: string | null;
  isDefault: boolean;
}

export interface KafkaDescribeConfigsResult {
  resources: Array<{ configEntries: KafkaConfigEntry[] }>;
}

export interface KafkaGroupOverview {
  groupId: string;
  protocolType: string;
}

export interface KafkaFetchOffsetsTopic {
  topic: string;
  partitions: Array<{ partition: number; offset: string }>;
}

export interface KafkaTopicOffset {
  partition: number;
  offset: string;
  high: string;
  low: string;
}

export interface KafkaAdminClient {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  listTopics(): Promise<string[]>;
  fetchTopicMetadata(args: { topics: string[] }): Promise<KafkaTopicMetadata>;
  describeConfigs(args: {
    resources: Array<{ type: number; name: string }>;
    includeSynonyms: boolean;
  }): Promise<KafkaDescribeConfigsResult>;
  listGroups(): Promise<{ groups: KafkaGroupOverview[] }>;
  fetchOffsets(args: { groupId: string }): Promise<KafkaFetchOffsetsTopic[]>;
  fetchTopicOffsets(topic: string): Promise<KafkaTopicOffset[]>;
}
