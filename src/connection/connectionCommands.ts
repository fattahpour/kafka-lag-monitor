import * as vscode from 'vscode';
import { ConnectionManager } from './connectionManager';
import { saveConnectionProfiles } from './connectionStore';
import { parseBrokerList, validateProfileName } from './connectionWizard';
import { getConnectionProfiles } from './profileStore';
import { validateProfile } from './profileValidation';
import { deleteCredentials, setCredential } from './secretStore';
import { ConnectionProfile, SaslMechanism } from './types';
import { KafkaExplorerProvider } from '../treeView/kafkaExplorerProvider';
import { STATUS_ICONS } from '../treeView/treeItems';

const AUTH_TYPES: Array<{ label: string; mechanism: SaslMechanism | null }> = [
  { label: 'None', mechanism: null },
  { label: 'PLAIN', mechanism: 'plain' },
  { label: 'SCRAM-SHA-256', mechanism: 'scram-sha-256' },
  { label: 'SCRAM-SHA-512', mechanism: 'scram-sha-512' },
];

interface WizardResult {
  profile: ConnectionProfile;
  username?: string;
  password?: string;
}

async function runConnectionWizard(
  existingNames: string[],
  initial?: ConnectionProfile,
): Promise<WizardResult | undefined> {
  const nameTargets = initial ? existingNames.filter((n) => n !== initial.name) : existingNames;

  const name = await vscode.window.showInputBox({
    title: 'Connection name',
    value: initial?.name ?? '',
    validateInput: (value) => validateProfileName(value, nameTargets),
  });
  if (name === undefined) return undefined;

  const brokersInput = await vscode.window.showInputBox({
    title: 'Brokers (comma-separated host:port)',
    value: initial?.brokers.join(', ') ?? '',
    validateInput: (value) => {
      const { errors } = parseBrokerList(value);
      return errors.length > 0 ? errors.join('; ') : null;
    },
  });
  if (brokersInput === undefined) return undefined;
  const { brokers } = parseBrokerList(brokersInput);

  const sslChoice = await vscode.window.showQuickPick(['No', 'Yes'], {
    title: 'Use SSL?',
    placeHolder: initial?.ssl ? 'Yes' : 'No',
  });
  if (sslChoice === undefined) return undefined;

  const authChoice = await vscode.window.showQuickPick(
    AUTH_TYPES.map((a) => a.label),
    { title: 'Authentication', placeHolder: initial?.sasl?.mechanism ?? 'None' },
  );
  if (authChoice === undefined) return undefined;
  const mechanism = AUTH_TYPES.find((a) => a.label === authChoice)?.mechanism ?? null;

  let username: string | undefined;
  let password: string | undefined;
  if (mechanism) {
    username = await vscode.window.showInputBox({ title: 'Username (leave blank to keep existing)' });
    if (username === undefined) return undefined;

    password = await vscode.window.showInputBox({
      title: 'Password (leave blank to keep existing)',
      password: true,
    });
    if (password === undefined) return undefined;
  }

  const clientId = await vscode.window.showInputBox({
    title: 'Client ID',
    value: initial?.clientId ?? 'kafka-lag-monitor',
  });
  if (clientId === undefined) return undefined;

  const { profile, errors } = validateProfile({
    name,
    brokers,
    sasl: mechanism ? { mechanism } : null,
    ssl: sslChoice === 'Yes',
    clientId,
  });
  if (!profile) {
    vscode.window.showErrorMessage(`Invalid connection: ${errors.join('; ')}`);
    return undefined;
  }

  return { profile, username, password };
}

export function registerConnectionCommands(
  context: vscode.ExtensionContext,
  connectionManager: ConnectionManager,
  explorer: KafkaExplorerProvider,
  onConfigError: (message: string) => void,
): void {
  const refresh = (): void => {
    explorer.setProfiles(getConnectionProfiles(onConfigError));
    explorer.refresh();
  };

  context.subscriptions.push(
    vscode.commands.registerCommand('kafkaLagMonitor.addConnection', async () => {
      const existing = getConnectionProfiles(onConfigError);
      const result = await runConnectionWizard(existing.map((p) => p.name));
      if (!result) return;

      try {
        await saveConnectionProfiles([...existing, result.profile]);
        if (result.profile.sasl) {
          if (result.username) await setCredential(context.secrets, result.profile.name, 'username', result.username);
          if (result.password) await setCredential(context.secrets, result.profile.name, 'password', result.password);
        }
      } catch (err) {
        vscode.window.showErrorMessage((err as Error).message);
        return;
      }
      refresh();
    }),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('kafkaLagMonitor.editConnection', async () => {
      const existing = getConnectionProfiles(onConfigError);
      const target = await vscode.window.showQuickPick(existing.map((p) => p.name), {
        title: 'Edit which connection?',
      });
      if (!target) return;
      const current = existing.find((p) => p.name === target);
      if (!current) return;

      const result = await runConnectionWizard(existing.map((p) => p.name), current);
      if (!result) return;

      try {
        await saveConnectionProfiles(existing.map((p) => (p.name === current.name ? result.profile : p)));
        if (result.profile.sasl) {
          if (result.username) await setCredential(context.secrets, result.profile.name, 'username', result.username);
          if (result.password) await setCredential(context.secrets, result.profile.name, 'password', result.password);
        }
      } catch (err) {
        vscode.window.showErrorMessage((err as Error).message);
        return;
      }
      refresh();
    }),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('kafkaLagMonitor.removeConnection', async () => {
      const existing = getConnectionProfiles(onConfigError);
      const target = await vscode.window.showQuickPick(existing.map((p) => p.name), {
        title: 'Remove which connection?',
      });
      if (!target) return;

      const confirm = await vscode.window.showWarningMessage(
        `Remove connection "${target}" and its stored credentials?`,
        { modal: true },
        'Remove',
      );
      if (confirm !== 'Remove') return;

      try {
        await connectionManager.disconnect(target);
        await deleteCredentials(context.secrets, target, ['username', 'password']);
        await saveConnectionProfiles(existing.filter((p) => p.name !== target));
      } catch (err) {
        vscode.window.showErrorMessage((err as Error).message);
        return;
      }
      refresh();
    }),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand('kafkaLagMonitor.reconnect', async () => {
      const existing = getConnectionProfiles(onConfigError);
      const target = await vscode.window.showQuickPick(
        existing.map((p) => ({
          label: `${p.name} ${STATUS_ICONS[connectionManager.getState(p.name).status]}`,
          profile: p,
        })),
        { title: 'Reconnect which connection?' },
      );
      if (!target) return;

      try {
        await connectionManager.reconnect(target.profile);
      } catch (err) {
        vscode.window.showErrorMessage((err as Error).message);
      }
      explorer.refresh();
    }),
  );
}
