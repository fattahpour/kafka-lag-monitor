import * as fs from 'fs';
import { Kafka, SASLOptions } from 'kafkajs';
import * as vscode from 'vscode';
import { registerConnectionCommands } from './connection/connectionCommands';
import { ConnectionManager } from './connection/connectionManager';
import { getConnectionProfiles, getLagThresholds } from './connection/profileStore';
import { getCredential } from './connection/secretStore';
import { buildSslOptions, TlsConnectionOptions } from './connection/sslConfig';
import { ConnectionProfile, SaslMechanism } from './connection/types';
import { createKafkaAdminClient } from './kafka/kafkaAdminAdapter';
import { createKafkaConsumerClient } from './kafka/kafkaConsumerAdapter';
import { createKafkaProducerClient } from './kafka/kafkaProducerAdapter';
import { createKafkaLogCreator } from './logging/kafkaLogCreator';
import { KafkaExplorerProvider } from './treeView/kafkaExplorerProvider';
import { LagDashboardPanel } from './webviews/lagDashboardPanelController';
import { MessageBrowserPanel } from './webviews/messageBrowserPanelController';
import { ProducePanel } from './webviews/producePanelController';
import { TopicMetadataPanel } from './webviews/topicMetadataPanelController';

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

  async function buildKafka(profile: ConnectionProfile): Promise<Kafka> {
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

    let ssl: boolean | TlsConnectionOptions;
    try {
      let passphrase: string | undefined;
      if (typeof profile.ssl === 'object') {
        passphrase = await getCredential(context.secrets, profile.name, 'tlsKeyPassphrase');
      }
      ssl = buildSslOptions(profile.ssl, (path) => fs.readFileSync(path, 'utf-8'), passphrase);
    } catch (err) {
      throw new Error(`${(err as Error).message} (connection "${profile.name}")`);
    }

    return new Kafka({
      clientId: profile.clientId,
      brokers: profile.brokers,
      ssl,
      sasl,
      logCreator: createKafkaLogCreator((line) => output.appendLine(line)),
    });
  }

  const connectionManager = new ConnectionManager(
    async (profile) => createKafkaAdminClient((await buildKafka(profile)).admin()),
    async (profile) => createKafkaProducerClient((await buildKafka(profile)).producer()),
  );

  const createConsumerClient = async (profile: ConnectionProfile) => createKafkaConsumerClient(await buildKafka(profile));

  const onConfigError = (message: string) => output.appendLine(`[CONFIG] ${message}`);
  const profiles = getConnectionProfiles(onConfigError);
  const thresholds = getLagThresholds();

  const explorer = new KafkaExplorerProvider(profiles, connectionManager, thresholds);
  const treeView = vscode.window.createTreeView('kafkaLagMonitor.explorer', { treeDataProvider: explorer });
  context.subscriptions.push(treeView);

  context.subscriptions.push(vscode.commands.registerCommand('kafkaLagMonitor.refresh', () => explorer.refresh()));

  registerConnectionCommands(context, connectionManager, explorer, onConfigError);

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'kafkaLagMonitor.showTopicMetadata',
      async (profile: ConnectionProfile, topicName: string) => {
        await TopicMetadataPanel.show(connectionManager, profile.name, topicName);
      },
    ),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'kafkaLagMonitor.showLagDashboard',
      async (profile: ConnectionProfile, groupId: string) => {
        const pollIntervalSeconds = vscode.workspace
          .getConfiguration('kafkaLagMonitor')
          .get('pollIntervalSeconds', 10);
        await LagDashboardPanel.show(connectionManager, profile, groupId, thresholds, pollIntervalSeconds);
      },
    ),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'kafkaLagMonitor.browseMessages',
      async (profile: ConnectionProfile, topicName: string) => {
        await MessageBrowserPanel.show(connectionManager, createConsumerClient, profile, topicName);
      },
    ),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand(
      'kafkaLagMonitor.produce',
      async (profile: ConnectionProfile, topicName: string) => {
        await ProducePanel.show(connectionManager, profile, topicName);
      },
    ),
  );
}

export function deactivate(): void {}
