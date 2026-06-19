import * as vscode from 'vscode';
import * as fs from 'node:fs/promises';
import * as path from 'node:path';
import { CONNECTIONS_FILE_NAME } from './profileStore';
import { ConnectionProfile } from './types';

export async function saveConnectionProfiles(profiles: ConnectionProfile[]): Promise<void> {
  const folder = vscode.workspace.workspaceFolders?.[0];
  if (!folder) {
    throw new Error('Open a workspace folder to manage Kafka connections');
  }

  const vscodeDir = path.join(folder.uri.fsPath, '.vscode');
  await fs.mkdir(vscodeDir, { recursive: true });
  await fs.writeFile(
    path.join(vscodeDir, CONNECTIONS_FILE_NAME),
    JSON.stringify({ connections: profiles }, null, 2),
    'utf8'
  );
}
