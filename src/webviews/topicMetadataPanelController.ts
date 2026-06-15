import * as vscode from 'vscode';
import { ConnectionManager } from '../connection/connectionManager';
import { renderErrorHtml, renderTopicMetadataHtml } from './topicMetadataPanel';

export class TopicMetadataPanel {
  private static currentPanel: TopicMetadataPanel | undefined;

  private profileName = '';
  private topicName = '';
  private generation = 0;

  private constructor(
    private readonly panel: vscode.WebviewPanel,
    private readonly connectionManager: ConnectionManager,
  ) {
    this.panel.webview.onDidReceiveMessage((message: { type: string }) => {
      if (message.type === 'refresh') {
        void this.render();
      }
    });
    this.panel.onDidDispose(() => {
      TopicMetadataPanel.currentPanel = undefined;
    });
  }

  static async show(connectionManager: ConnectionManager, profileName: string, topicName: string): Promise<void> {
    let instance = TopicMetadataPanel.currentPanel;
    if (instance) {
      instance.panel.reveal();
    } else {
      const panel = vscode.window.createWebviewPanel('kafkaTopicMetadata', 'Topic Metadata', vscode.ViewColumn.Active, {
        enableScripts: true,
      });
      instance = new TopicMetadataPanel(panel, connectionManager);
      TopicMetadataPanel.currentPanel = instance;
    }
    instance.panel.title = `Topic: ${topicName}`;
    instance.profileName = profileName;
    instance.topicName = topicName;
    await instance.render();
  }

  private async render(): Promise<void> {
    const gen = ++this.generation;
    const adminService = this.connectionManager.getAdminService(this.profileName);
    if (!adminService) {
      if (gen !== this.generation) return;
      this.panel.webview.html = renderErrorHtml('Not connected — expand the connection in the sidebar first.');
      return;
    }
    try {
      const [metadata, configEntries] = await Promise.all([
        adminService.getTopicMetadata(this.topicName),
        adminService.getTopicConfig(this.topicName),
      ]);
      if (gen !== this.generation) return;
      this.panel.webview.html = renderTopicMetadataHtml(this.topicName, metadata, configEntries);
    } catch (err) {
      if (gen !== this.generation) return;
      this.panel.webview.html = renderErrorHtml((err as Error).message);
    }
  }
}
