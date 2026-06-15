import * as vscode from 'vscode';
import { ConnectionManager } from '../connection/connectionManager';
import { Thresholds } from '../connection/profileStore';
import { ConnectionProfile } from '../connection/types';
import { PollingManager } from '../polling/pollingManager';
import { renderLagDashboardHtml, toDashboardData } from './lagDashboardPanel';
import { renderErrorHtml } from './topicMetadataPanel';

export class LagDashboardPanel {
  private static currentPanel: LagDashboardPanel | undefined;

  private profileName = '';
  private groupId = '';
  private generation = 0;
  private autoPollEnabled = false;
  private consecutiveFailures = 0;
  private readonly polling = new PollingManager();

  private constructor(
    private readonly panel: vscode.WebviewPanel,
    private readonly connectionManager: ConnectionManager,
    private readonly thresholds: Thresholds,
    private readonly pollIntervalSeconds: number,
  ) {
    this.panel.webview.onDidReceiveMessage((message: { type: string; enabled?: boolean }) => {
      if (message.type === 'refresh') void this.refresh();
      else if (message.type === 'setAutoPoll') this.setAutoPoll(message.enabled === true);
    });
    this.panel.onDidChangeViewState((e) => {
      if (!e.webviewPanel.visible) this.polling.stop();
      else if (this.autoPollEnabled) this.polling.start(this.pollIntervalSeconds * 1000, () => this.pollTick());
    });
    this.panel.onDidDispose(() => {
      this.polling.stop();
      LagDashboardPanel.currentPanel = undefined;
    });
  }

  static async show(
    connectionManager: ConnectionManager,
    profile: ConnectionProfile,
    groupId: string,
    thresholds: Thresholds,
    pollIntervalSeconds: number,
  ): Promise<void> {
    let instance = LagDashboardPanel.currentPanel;
    if (instance) {
      instance.panel.reveal();
    } else {
      const panel = vscode.window.createWebviewPanel('kafkaLagDashboard', 'Lag Dashboard', vscode.ViewColumn.Active, {
        enableScripts: true,
        retainContextWhenHidden: true,
      });
      instance = new LagDashboardPanel(panel, connectionManager, thresholds, pollIntervalSeconds);
      LagDashboardPanel.currentPanel = instance;
    }
    instance.panel.title = `Lag: ${groupId}`;
    instance.profileName = profile.name;
    instance.groupId = groupId;
    instance.polling.stop();
    instance.autoPollEnabled = false;
    instance.consecutiveFailures = 0;
    await instance.renderFull();
  }

  private async renderFull(): Promise<void> {
    const gen = ++this.generation;
    const adminService = this.connectionManager.getAdminService(this.profileName);
    if (!adminService) {
      if (gen !== this.generation) return;
      this.panel.webview.html = renderErrorHtml('Not connected — expand the connection in the sidebar first.');
      return;
    }
    try {
      const topicLags = await adminService.getGroupLag(this.groupId);
      if (gen !== this.generation) return;
      const data = toDashboardData(this.groupId, topicLags, this.thresholds);
      this.panel.webview.html = renderLagDashboardHtml(this.groupId, data, this.pollIntervalSeconds);
    } catch (err) {
      if (gen !== this.generation) return;
      this.panel.webview.html = renderErrorHtml((err as Error).message);
    }
  }

  private async refresh(): Promise<void> {
    const gen = this.generation;
    const adminService = this.connectionManager.getAdminService(this.profileName);
    if (!adminService) return;
    try {
      const topicLags = await adminService.getGroupLag(this.groupId);
      if (gen !== this.generation) return;
      this.consecutiveFailures = 0;
      const data = toDashboardData(this.groupId, topicLags, this.thresholds);
      void this.panel.webview.postMessage({ type: 'update', data });
    } catch (err) {
      if (gen !== this.generation) return;
      this.consecutiveFailures++;
      void this.panel.webview.postMessage({
        type: 'pollError',
        message: (err as Error).message,
        showReconnectHint: this.consecutiveFailures >= 3,
      });
    }
  }

  private pollTick(): void {
    void this.refresh();
  }

  private setAutoPoll(enabled: boolean): void {
    this.autoPollEnabled = enabled;
    if (enabled) {
      this.consecutiveFailures = 0;
      this.polling.start(this.pollIntervalSeconds * 1000, () => this.pollTick());
    } else {
      this.polling.stop();
    }
  }
}
