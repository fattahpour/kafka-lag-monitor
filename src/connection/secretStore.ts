import * as vscode from 'vscode';
import { secretKey } from './secretKey';

export async function getCredential(
  secrets: vscode.SecretStorage,
  profileName: string,
  field: string,
): Promise<string | undefined> {
  return secrets.get(secretKey(profileName, field));
}

export async function setCredential(
  secrets: vscode.SecretStorage,
  profileName: string,
  field: string,
  value: string,
): Promise<void> {
  await secrets.store(secretKey(profileName, field), value);
}

export async function deleteCredentials(
  secrets: vscode.SecretStorage,
  profileName: string,
  fields: string[],
): Promise<void> {
  for (const field of fields) {
    await secrets.delete(secretKey(profileName, field));
  }
}
