import * as vscode from 'vscode';
import { ConnectionManager } from '../connection/connectionManager';
import { ConnectionProfile } from '../connection/types';
import { renderProduceHtml, ProduceSendMessage } from './producePanel';
import { renderErrorHtml } from './topicMetadataPanel';

export class ProducePanel {
  private static currentPanel: ProducePanel | undefined;

  private profile: ConnectionProfile | undefined;
  private topicName = '';
  private generation = 0;

  private constructor(
    private readonly panel: vscode.WebviewPanel,
    private readonly connectionManager: ConnectionManager,
  ) {
    this.panel.webview.onDidReceiveMessage((message: ProduceSendMessage) => {
      if (message.type === 'send') void this.send(message);
    });
    this.panel.onDidDispose(() => {
      ProducePanel.currentPanel = undefined;
    });
  }

  static async show(connectionManager: ConnectionManager, profile: ConnectionProfile, topicName: string): Promise<void> {
    let instance = ProducePanel.currentPanel;
    if (instance) {
      instance.panel.reveal();
    } else {
      const panel = vscode.window.createWebviewPanel('kafkaProduce', 'Produce', vscode.ViewColumn.Active, {
        enableScripts: true,
        retainContextWhenHidden: true,
      });
      instance = new ProducePanel(panel, connectionManager);
      ProducePanel.currentPanel = instance;
    }
    instance.panel.title = `Produce: ${topicName}`;
    instance.profile = profile;
    instance.topicName = topicName;
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
      if (gen !== this.generation) return;
      this.panel.webview.html = renderProduceHtml(this.topicName, metadata.partitions.length);
    } catch (err) {
      if (gen !== this.generation) return;
      this.panel.webview.html = renderErrorHtml((err as Error).message);
    }
  }

  private async send(message: ProduceSendMessage): Promise<void> {
    const gen = this.generation;
    const profile = this.profile!;
    const topic = this.topicName;
    try {
      const producerService = await this.connectionManager.getProducerService(profile);
      if (gen !== this.generation) return;
      if (!producerService) {
        void this.panel.webview.postMessage({
          type: 'result',
          success: false,
          message: 'Not connected — expand the connection in the sidebar first.',
        });
        return;
      }
      const result = await producerService.send({
        topic,
        partition: message.partition,
        key: message.key,
        value: message.value,
        headers: message.headers,
      });
      if (gen !== this.generation) return;
      void this.panel.webview.postMessage({ type: 'result', success: true, partition: result.partition, offset: result.offset });
    } catch (err) {
      if (gen !== this.generation) return;
      void this.panel.webview.postMessage({ type: 'result', success: false, message: (err as Error).message });
    }
  }
}
