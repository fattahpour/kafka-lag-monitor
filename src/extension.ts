import { Kafka, SASLOptions } from 'kafkajs';
import * as vscode from 'vscode';
import { ConnectionManager } from './connection/connectionManager';
import { getConnectionProfiles, getLagThresholds } from './connection/profileStore';
import { getCredential } from './connection/secretStore';
import { SaslMechanism } from './connection/types';
import { createKafkaAdminClient } from './kafka/kafkaAdminAdapter';
import { createKafkaLogCreator } from './logging/kafkaLogCreator';
import { KafkaExplorerProvider } from './treeView/kafkaExplorerProvider';

function buildSasl(mechanism: SaslMechanism, username: string, password: string): SASLOptions {
  switch (mechanism) {
    case 'plain':
      return { mechanism: 'plain', username, password };
    case 'scram-sha-256':
      return { mechanism: 'scram-sha-256', username, password };
    case 'scram-sha-512':
      return { mechanism: 'scram-sha-512', username, password };
  }
}

export function activate(context: vscode.ExtensionContext): void {
  const output = vscode.window.createOutputChannel('Kafka Lag Monitor');
  output.appendLine('Kafka Lag Monitor activated');
  context.subscriptions.push(output);

  const connectionManager = new ConnectionManager(async (profile) => {
    let sasl: SASLOptions | undefined;
    if (profile.sasl) {
      const username = await getCredential(context.secrets, profile.name, 'username');
      const password = await getCredential(context.secrets, profile.name, 'password');
      if (username === undefined || password === undefined) {
        throw new Error(
          `Missing SASL credentials for connection "${profile.name}". Use the 'Kafka: Add Connection' command to set them.`,
        );
      }
      sasl = buildSasl(profile.sasl.mechanism, username, password);
    }
    const kafka = new Kafka({
      clientId: profile.clientId,
      brokers: profile.brokers,
      ssl: profile.ssl,
      sasl,
      logCreator: createKafkaLogCreator((line) => output.appendLine(line)),
    });
    return createKafkaAdminClient(kafka.admin());
  });

  const onConfigError = (message: string) => output.appendLine(`[CONFIG] ${message}`);
  const profiles = getConnectionProfiles(onConfigError);
  const thresholds = getLagThresholds();

  const explorer = new KafkaExplorerProvider(profiles, connectionManager, thresholds);
  const treeView = vscode.window.createTreeView('kafkaLagMonitor.explorer', { treeDataProvider: explorer });
  context.subscriptions.push(treeView);

  context.subscriptions.push(vscode.commands.registerCommand('kafkaLagMonitor.refresh', () => explorer.refresh()));
}

export function deactivate(): void {}
