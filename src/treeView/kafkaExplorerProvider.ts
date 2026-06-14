import * as vscode from 'vscode';
import { ConnectionManager } from '../connection/connectionManager';
import { Thresholds } from '../connection/profileStore';
import { ConnectionProfile } from '../connection/types';
import { TopicSummary } from '../kafka/adminService';
import { lagSeverity, PartitionLag, TopicLag } from '../kafka/lag';
import { buildConnectionNode, buildGroupNode, buildPartitionNode, buildTopicNode } from './treeItems';

export type KafkaTreeNode =
  | { kind: 'connection'; profile: ConnectionProfile }
  | { kind: 'topicsFolder'; profile: ConnectionProfile }
  | { kind: 'groupsFolder'; profile: ConnectionProfile }
  | { kind: 'topic'; topic: TopicSummary }
  | { kind: 'group'; groupId: string; totalLag: number; topicLags: TopicLag[] }
  | { kind: 'groupTopic'; topicLag: TopicLag }
  | { kind: 'partition'; partitionLag: PartitionLag }
  | { kind: 'message'; text: string };

export class KafkaExplorerProvider implements vscode.TreeDataProvider<KafkaTreeNode> {
  private readonly _onDidChangeTreeData = new vscode.EventEmitter<KafkaTreeNode | undefined>();
  readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

  constructor(
    private readonly profiles: ConnectionProfile[],
    private readonly connectionManager: ConnectionManager,
    private readonly thresholds: Thresholds,
  ) {}

  refresh(): void {
    this._onDidChangeTreeData.fire(undefined);
  }

  getTreeItem(element: KafkaTreeNode): vscode.TreeItem {
    switch (element.kind) {
      case 'connection': {
        const state = this.connectionManager.getState(element.profile.name);
        const view = buildConnectionNode(element.profile.name, state.status, state.error);
        const item = new vscode.TreeItem(view.label, vscode.TreeItemCollapsibleState.Collapsed);
        item.description = view.description;
        return item;
      }
      case 'topicsFolder':
        return new vscode.TreeItem('Topics', vscode.TreeItemCollapsibleState.Collapsed);
      case 'groupsFolder':
        return new vscode.TreeItem('Consumer Groups', vscode.TreeItemCollapsibleState.Collapsed);
      case 'topic': {
        const view = buildTopicNode(element.topic.name, element.topic.partitionCount);
        const item = new vscode.TreeItem(view.label, vscode.TreeItemCollapsibleState.None);
        item.description = view.description;
        return item;
      }
      case 'group': {
        const severity = lagSeverity(element.totalLag, this.thresholds.warning, this.thresholds.critical);
        const view = buildGroupNode(element.groupId, element.totalLag, severity);
        const item = new vscode.TreeItem(
          view.label,
          element.topicLags.length > 0
            ? vscode.TreeItemCollapsibleState.Collapsed
            : vscode.TreeItemCollapsibleState.None,
        );
        item.description = view.description;
        return item;
      }
      case 'groupTopic': {
        const item = new vscode.TreeItem(element.topicLag.topic, vscode.TreeItemCollapsibleState.Collapsed);
        item.description = `●${element.topicLag.totalLag}`;
        return item;
      }
      case 'partition': {
        const view = buildPartitionNode(
          element.partitionLag.partition,
          element.partitionLag.currentOffset,
          element.partitionLag.endOffset,
          element.partitionLag.lag,
        );
        return new vscode.TreeItem(view.label, vscode.TreeItemCollapsibleState.None);
      }
      case 'message':
        return new vscode.TreeItem(element.text, vscode.TreeItemCollapsibleState.None);
    }
  }

  async getChildren(element?: KafkaTreeNode): Promise<KafkaTreeNode[]> {
    if (!element) {
      return this.profiles.map((profile) => ({ kind: 'connection', profile }));
    }

    switch (element.kind) {
      case 'connection': {
        if (this.connectionManager.getState(element.profile.name).status === 'idle') {
          await this.connectionManager.connect(element.profile).catch(() => undefined);
        }
        return [
          { kind: 'topicsFolder', profile: element.profile },
          { kind: 'groupsFolder', profile: element.profile },
        ];
      }
      case 'topicsFolder': {
        const adminService = this.connectionManager.getAdminService(element.profile.name);
        if (!adminService) {
          return [
            { kind: 'message', text: this.connectionManager.getState(element.profile.name).error ?? 'Not connected' },
          ];
        }
        try {
          const topics = await adminService.listTopics();
          return topics.map((topic) => ({ kind: 'topic', topic }));
        } catch (err) {
          return [{ kind: 'message', text: (err as Error).message }];
        }
      }
      case 'groupsFolder': {
        const adminService = this.connectionManager.getAdminService(element.profile.name);
        if (!adminService) {
          return [
            { kind: 'message', text: this.connectionManager.getState(element.profile.name).error ?? 'Not connected' },
          ];
        }
        try {
          const groups = await adminService.listConsumerGroups();
          const nodes: KafkaTreeNode[] = [];
          for (const group of groups) {
            const topicLags = await adminService.getGroupLag(group.groupId);
            const totalLag = topicLags.reduce((sum, t) => sum + t.totalLag, 0);
            nodes.push({ kind: 'group', groupId: group.groupId, totalLag, topicLags });
          }
          return nodes;
        } catch (err) {
          return [{ kind: 'message', text: (err as Error).message }];
        }
      }
      case 'group':
        return element.topicLags.map((topicLag) => ({ kind: 'groupTopic', topicLag }));
      case 'groupTopic':
        return element.topicLag.partitions.map((partitionLag) => ({ kind: 'partition', partitionLag }));
      case 'topic':
      case 'partition':
      case 'message':
        return [];
    }
  }
}
