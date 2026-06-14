export type LagStatus = 'ok' | 'lag' | 'not-started';

export interface PartitionLag {
  partition: number;
  currentOffset: number;
  endOffset: number;
  lag: number;
  status: LagStatus;
}

export function computePartitionLag(
  partition: number,
  committedOffset: number | null,
  highWatermark: number,
): PartitionLag {
  if (committedOffset === null) {
    return {
      partition,
      currentOffset: 0,
      endOffset: highWatermark,
      lag: highWatermark,
      status: highWatermark > 0 ? 'not-started' : 'ok',
    };
  }
  const lag = Math.max(highWatermark - committedOffset, 0);
  return {
    partition,
    currentOffset: committedOffset,
    endOffset: highWatermark,
    lag,
    status: lag > 0 ? 'lag' : 'ok',
  };
}

export interface TopicLag {
  topic: string;
  partitions: PartitionLag[];
  totalLag: number;
}

export function aggregateTopicLag(topic: string, partitions: PartitionLag[]): TopicLag {
  const totalLag = partitions.reduce((sum, p) => sum + p.lag, 0);
  return { topic, partitions, totalLag };
}

export type LagSeverity = 'none' | 'warning' | 'critical';

export function lagSeverity(totalLag: number, warningThreshold: number, criticalThreshold: number): LagSeverity {
  if (totalLag >= criticalThreshold) return 'critical';
  if (totalLag >= warningThreshold) return 'warning';
  return 'none';
}
