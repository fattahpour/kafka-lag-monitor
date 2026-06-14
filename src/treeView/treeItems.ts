import { ConnectionStatus } from '../connection/types';
import { LagSeverity } from '../kafka/lag';

export interface ConnectionNodeView {
  label: string;
  description: string;
}

const STATUS_ICONS: Record<ConnectionStatus, string> = {
  idle: '⚪',
  connecting: '…',
  connected: '✓',
  error: '⚠',
};

export function buildConnectionNode(name: string, status: ConnectionStatus, errorMessage?: string): ConnectionNodeView {
  return {
    label: `${name} ${STATUS_ICONS[status]}`,
    description: status === 'error' && errorMessage ? errorMessage : '',
  };
}

export interface TopicNodeView {
  label: string;
  description: string;
}

export function buildTopicNode(name: string, partitionCount: number): TopicNodeView {
  return { label: name, description: `${partitionCount} partition${partitionCount === 1 ? '' : 's'}` };
}

export interface GroupNodeView {
  label: string;
  description: string;
  severity: LagSeverity;
}

export function buildGroupNode(groupId: string, totalLag: number, severity: LagSeverity): GroupNodeView {
  return { label: groupId, description: `●${totalLag}`, severity };
}

export interface PartitionNodeView {
  label: string;
}

export function buildPartitionNode(
  partition: number,
  currentOffset: number,
  endOffset: number,
  lag: number,
): PartitionNodeView {
  return { label: `p${partition}: ${currentOffset}/${endOffset} (${lag})` };
}
