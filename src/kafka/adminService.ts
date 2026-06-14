import { KafkaAdminClient } from './adminClient';

export interface TopicSummary {
  name: string;
  partitionCount: number;
}

export interface PartitionMetadata {
  partitionId: number;
  leader: number;
  replicas: number[];
  isr: number[];
}

export interface TopicMetadata {
  name: string;
  partitions: PartitionMetadata[];
}

export interface ConfigEntry {
  name: string;
  value: string | null;
  isDefault: boolean;
}

const TOPIC_RESOURCE_TYPE = 2; // kafkajs ResourceTypes.TOPIC

export class AdminService {
  constructor(private readonly admin: KafkaAdminClient) {}

  async listTopics(): Promise<TopicSummary[]> {
    const names = (await this.admin.listTopics()).filter((n) => !n.startsWith('__'));
    if (names.length === 0) return [];
    const metadata = await this.admin.fetchTopicMetadata({ topics: names });
    return metadata.topics.map((t) => ({ name: t.name, partitionCount: t.partitions.length }));
  }

  async getTopicMetadata(topic: string): Promise<TopicMetadata> {
    const metadata = await this.admin.fetchTopicMetadata({ topics: [topic] });
    const found = metadata.topics.find((t) => t.name === topic);
    if (!found) {
      throw new Error(`Topic "${topic}" not found`);
    }
    return {
      name: found.name,
      partitions: found.partitions.map((p) => ({
        partitionId: p.partitionId,
        leader: p.leader,
        replicas: p.replicas,
        isr: p.isr,
      })),
    };
  }

  async getTopicConfig(topic: string): Promise<ConfigEntry[]> {
    const result = await this.admin.describeConfigs({
      resources: [{ type: TOPIC_RESOURCE_TYPE, name: topic }],
      includeSynonyms: false,
    });
    const resource = result.resources[0];
    if (!resource) return [];
    return resource.configEntries.map((e) => ({ name: e.configName, value: e.configValue, isDefault: e.isDefault }));
  }
}
