import { Kafka } from 'kafkajs';
import * as vscode from 'vscode';
import { ConnectionManager } from './connection/connectionManager';
import { getConnectionProfiles, getLagThresholds } from './connection/profileStore';
import { createKafkaAdminClient } from './kafka/kafkaAdminAdapter';
import { createKafkaLogCreator } from './logging/kafkaLogCreator';
import { KafkaExplorerProvider } from './treeView/kafkaExplorerProvider';

export function activate(context: vscode.ExtensionContext): void {
  const output = vscode.window.createOutputChannel('Kafka Lag Monitor');
  output.appendLine('Kafka Lag Monitor activated');
  context.subscriptions.push(output);

  const connectionManager = new ConnectionManager(async (profile) => {
    if (profile.sasl) {
      throw new Error('SASL authentication is not supported yet. Use a PLAINTEXT or SSL-only connection.');
    }
    const kafka = new Kafka({
      clientId: profile.clientId,
      brokers: profile.brokers,
      ssl: profile.ssl,
      logCreator: createKafkaLogCreator((line) => output.appendLine(line)),
    });
    return createKafkaAdminClient(kafka.admin());
  });

  const profiles = getConnectionProfiles((message) => output.appendLine(`[CONFIG] ${message}`));
  const thresholds = getLagThresholds();

  const explorer = new KafkaExplorerProvider(profiles, connectionManager, thresholds);
  const treeView = vscode.window.createTreeView('kafkaLagMonitor.explorer', { treeDataProvider: explorer });
  context.subscriptions.push(treeView);

  context.subscriptions.push(vscode.commands.registerCommand('kafkaLagMonitor.refresh', () => explorer.refresh()));
}

export function deactivate(): void {}
