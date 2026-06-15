import { LagSeverity, TopicLag, lagSeverity } from '../kafka/lag';
import { Thresholds } from '../connection/profileStore';

export interface PartitionLagView {
  partition: number;
  currentOffset: number;
  endOffset: number;
  lag: number;
  percentConsumed: number;
  severity: LagSeverity;
}

export interface TopicLagView {
  topic: string;
  totalLag: number;
  partitions: PartitionLagView[];
}

export interface LagDashboardData {
  groupId: string;
  totalLag: number;
  severity: LagSeverity;
  overThresholdCount: number;
  topics: TopicLagView[];
}

export function toDashboardData(groupId: string, topicLags: TopicLag[], thresholds: Thresholds): LagDashboardData {
  let totalLag = 0;
  let overThresholdCount = 0;

  const topics: TopicLagView[] = topicLags.map((topicLag) => {
    const partitions: PartitionLagView[] = topicLag.partitions.map((p) => {
      const percentConsumed = p.endOffset === 0 ? 100 : Math.round((p.currentOffset / p.endOffset) * 100);
      const severity = lagSeverity(p.lag, thresholds.warning, thresholds.critical);
      if (severity !== 'none') {
        overThresholdCount++;
      }
      totalLag += p.lag;
      return {
        partition: p.partition,
        currentOffset: p.currentOffset,
        endOffset: p.endOffset,
        lag: p.lag,
        percentConsumed,
        severity,
      };
    });
    return { topic: topicLag.topic, totalLag: topicLag.totalLag, partitions };
  });

  return {
    groupId,
    totalLag,
    severity: lagSeverity(totalLag, thresholds.warning, thresholds.critical),
    overThresholdCount,
    topics,
  };
}
