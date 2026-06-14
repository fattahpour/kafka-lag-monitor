import { Admin, ConfigResourceTypes } from 'kafkajs';
import { KafkaAdminClient } from './adminClient';

export function createKafkaAdminClient(admin: Admin): KafkaAdminClient {
  return {
    connect: () => admin.connect(),
    disconnect: () => admin.disconnect(),
    listTopics: () => admin.listTopics(),
    fetchTopicMetadata: (args) => admin.fetchTopicMetadata(args),
    describeConfigs: (args) =>
      admin.describeConfigs({
        resources: args.resources.map((r) => ({ type: r.type as ConfigResourceTypes, name: r.name })),
        includeSynonyms: args.includeSynonyms,
      }),
    listGroups: () => admin.listGroups(),
    fetchOffsets: (args) => admin.fetchOffsets({ groupId: args.groupId }),
    fetchTopicOffsets: (topic) => admin.fetchTopicOffsets(topic),
  };
}
