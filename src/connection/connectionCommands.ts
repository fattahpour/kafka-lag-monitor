import * as vscode from 'vscode';
import { ConnectionManager } from './connectionManager';
import { saveConnectionProfiles } from './connectionStore';
import { parseBrokerList, validateProfileName } from './connectionWizard';
import { getConnectionProfiles } from './profileStore';
import { validateProfile } from './profileValidation';
import { deleteCredentials, setCredential } from './secretStore';
import { ConnectionProfile, MtlsConfig, SaslMechanism } from './types';
import { KafkaExplorerProvider } from '../treeView/kafkaExplorerProvider';
import { STATUS_ICONS } from '../treeView/treeItems';

const AUTH_TYPES: Array<{ label: string; mechanism: SaslMechanism | null }> = [
  { label: 'None', mechanism: null },
  { label: 'PLAIN', mechanism: 'plain' },
  { label: 'SCRAM-SHA-256', mechanism: 'scram-sha-256' },
  { label: 'SCRAM-SHA-512', mechanism: 'scram-sha-512' },
];

const SSL_CHOICES = ['No', 'Yes', 'Yes (with client certificate)'];

interface WizardResult {
  profile: ConnectionProfile;
  username?: string;
  password?: string;
  tlsPassphrase?: string;
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

  const initialSsl = initial?.ssl;
  const sslChoice = await vscode.window.showQuickPick(SSL_CHOICES, {
    title: 'Use SSL?',
    placeHolder:
      typeof initialSsl === 'object' ? SSL_CHOICES[2] : initialSsl ? SSL_CHOICES[1] : SSL_CHOICES[0],
  });
  if (sslChoice === undefined) return undefined;

  let mtls: MtlsConfig | undefined;
  let tlsPassphrase: string | undefined;

  if (sslChoice === SSL_CHOICES[2]) {
    const initialMtls = typeof initialSsl === 'object' ? initialSsl : undefined;

    const ca = await vscode.window.showInputBox({
      title: 'CA certificate path (optional, leave blank for default trust store)',
      value: initialMtls?.ca ?? '',
    });
    if (ca === undefined) return undefined;

    const cert = await vscode.window.showInputBox({
      title: 'Client certificate path (PEM)',
      value: initialMtls?.cert ?? '',
      validateInput: (value) => (value.trim() === '' ? '"Client certificate path" must not be empty' : null),
    });
    if (cert === undefined) return undefined;

    const key = await vscode.window.showInputBox({
      title: 'Client private key path (PEM)',
      value: initialMtls?.key ?? '',
      validateInput: (value) => (value.trim() === '' ? '"Client private key path" must not be empty' : null),
    });
    if (key === undefined) return undefined;

    mtls = { cert, key, ...(ca.trim() !== '' ? { ca } : {}) };

    const hasPassphrase = await vscode.window.showQuickPick(['No', 'Yes'], {
      title: 'Does the private key have a passphrase?',
    });
    if (hasPassphrase === undefined) return undefined;

    if (hasPassphrase === 'Yes') {
      tlsPassphrase = await vscode.window.showInputBox({
        title: 'Private key passphrase (leave blank to keep existing)',
        password: true,
      });
      if (tlsPassphrase === undefined) return undefined;
    }
  }

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
    ssl: mtls ?? sslChoice === SSL_CHOICES[1],
    clientId,
  });
  if (!profile) {
    vscode.window.showErrorMessage(`Invalid connection: ${errors.join('; ')}`);
    return undefined;
  }

  return { profile, username, password, tlsPassphrase };
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
        if (result.tlsPassphrase) {
          await setCredential(context.secrets, result.profile.name, 'tlsKeyPassphrase', result.tlsPassphrase);
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
        if (result.tlsPassphrase) {
          await setCredential(context.secrets, result.profile.name, 'tlsKeyPassphrase', result.tlsPassphrase);
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
        await deleteCredentials(context.secrets, target, ['username', 'password', 'tlsKeyPassphrase']);
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
