import * as vscode from 'vscode';
import { ConnectionProfile } from './types';

export async function saveConnectionProfiles(profiles: ConnectionProfile[]): Promise<void> {
  await vscode.workspace
    .getConfiguration('kafkaLagMonitor')
    .update('connections', profiles, vscode.ConfigurationTarget.Global);
}
