import * as vscode from 'vscode';
import { ConnectionManager } from '../connection/connectionManager';
import { ConnectionProfile } from '../connection/types';
import { KafkaConsumerClient } from '../kafka/consumerClient';
import { ConsumerService, MessageWindow, NavAction } from '../kafka/consumerService';
import { renderMessageBrowserHtml, toMessageBrowserData } from './messageBrowserPanel';
import { renderErrorHtml } from './topicMetadataPanel';

export type ConsumerClientFactory = (profile: ConnectionProfile) => Promise<KafkaConsumerClient>;

export class MessageBrowserPanel {
  private static currentPanel: MessageBrowserPanel | undefined;

  private profile: ConnectionProfile | undefined;
  private topicName = '';
  private partition = 0;
  private partitionCount = 0;
  private currentWindow: MessageWindow | undefined;
  private generation = 0;

  private constructor(
    private readonly panel: vscode.WebviewPanel,
    private readonly connectionManager: ConnectionManager,
    private readonly createConsumerClient: ConsumerClientFactory,
  ) {
    this.panel.webview.onDidReceiveMessage((message: { type: string; action?: NavAction; partition?: number }) => {
      if (message.type === 'nav' && message.action) void this.navigate(message.action);
      else if (message.type === 'setPartition' && message.partition !== undefined) void this.changePartition(message.partition);
    });
    this.panel.onDidDispose(() => {
      MessageBrowserPanel.currentPanel = undefined;
    });
  }

  static async show(
    connectionManager: ConnectionManager,
    createConsumerClient: ConsumerClientFactory,
    profile: ConnectionProfile,
    topicName: string,
  ): Promise<void> {
    let instance = MessageBrowserPanel.currentPanel;
    if (instance) {
      instance.panel.reveal();
    } else {
      const panel = vscode.window.createWebviewPanel('kafkaMessageBrowser', 'Messages', vscode.ViewColumn.Active, {
        enableScripts: true,
        retainContextWhenHidden: true,
      });
      instance = new MessageBrowserPanel(panel, connectionManager, createConsumerClient);
      MessageBrowserPanel.currentPanel = instance;
    }
    instance.panel.title = `Messages: ${topicName}`;
    instance.profile = profile;
    instance.topicName = topicName;
    instance.partition = 0;
    instance.currentWindow = undefined;
    await instance.renderFull();
  }

  private async renderFull(): Promise<void> {
    const gen = ++this.generation;
    const profile = this.profile!;
    const adminService = this.connectionManager.getAdminService(profile.name);
    if (!adminService) {
      if (gen !== this.generation) return;
      this.panel.webview.html = renderErrorHtml('Not connected — expand the connection in the sidebar first.');
      return;
    }
    try {
      const metadata = await adminService.getTopicMetadata(this.topicName);
      const consumerClient = await this.createConsumerClient(profile);
      const consumerService = new ConsumerService(consumerClient, adminService);
      const page = await consumerService.fetchPage(this.topicName, this.partition, 'latest');
      if (gen !== this.generation) return;
      this.partitionCount = metadata.partitions.length;
      this.currentWindow = page.window;
      const data = toMessageBrowserData(this.topicName, this.partitionCount, page);
      this.panel.webview.html = renderMessageBrowserHtml(this.topicName, data);
    } catch (err) {
      if (gen !== this.generation) return;
      this.panel.webview.html = renderErrorHtml((err as Error).message);
    }
  }

  private async navigate(action: NavAction): Promise<void> {
    const gen = this.generation;
    const profile = this.profile!;
    const adminService = this.connectionManager.getAdminService(profile.name);
    if (!adminService) return;
    try {
      const consumerClient = await this.createConsumerClient(profile);
      const consumerService = new ConsumerService(consumerClient, adminService);
      const page = await consumerService.fetchPage(this.topicName, this.partition, action, this.currentWindow);
      if (gen !== this.generation) return;
      this.currentWindow = page.window;
      const data = toMessageBrowserData(this.topicName, this.partitionCount, page);
      void this.panel.webview.postMessage({ type: 'update', data });
    } catch (err) {
      if (gen !== this.generation) return;
      void this.panel.webview.postMessage({ type: 'error', message: (err as Error).message });
    }
  }

  private async changePartition(partition: number): Promise<void> {
    this.partition = partition;
    this.currentWindow = undefined;
    await this.navigate('latest');
  }
}
