import * as vscode from 'vscode';
import * as fs from 'node:fs';
import * as path from 'node:path';
import { parseConnectionProfiles } from './profileValidation';
import { ConnectionProfile } from './types';

export interface Thresholds {
  warning: number;
  critical: number;
}

export const CONNECTIONS_FILE_NAME = 'kafka-lag-monitor.connections.json';
export const DEFAULT_PROFILE: ConnectionProfile = {
  name: 'local-cluster',
  brokers: ['localhost:9092'],
  sasl: null,
  ssl: false,
  clientId: 'kafka-lag-monitor',
};

export function getConnectionsFilePath(): string | null {
  const folder = vscode.workspace.workspaceFolders?.[0];
  if (!folder) {
    return null;
  }
  return path.join(folder.uri.fsPath, '.vscode', CONNECTIONS_FILE_NAME);
}

export function getConnectionProfiles(onError: (message: string) => void): ConnectionProfile[] {
  const configPath = getConnectionsFilePath();
  if (!configPath) {
    onError('Open a workspace folder to manage Kafka connections');
    return [DEFAULT_PROFILE];
  }

  if (!fs.existsSync(configPath)) {
    return [DEFAULT_PROFILE];
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(fs.readFileSync(configPath, 'utf8'));
  } catch (err) {
    onError(`Failed to parse ${configPath}: ${(err as Error).message}`);
    return [];
  }

  if (typeof parsed !== 'object' || parsed === null || !Array.isArray((parsed as { connections?: unknown }).connections)) {
    onError(`Failed to parse ${configPath}: expected top-level "connections" array`);
    return [];
  }

  const raw = (parsed as { connections: unknown[] }).connections;
  const { profiles, errors } = parseConnectionProfiles(raw);
  for (const { index, errors: entryErrors } of errors) {
    onError(`kafkaLagMonitor.connections[${index}]: ${entryErrors.join('; ')}`);
  }
  return profiles;
}

export function getLagThresholds(): Thresholds {
  const config = vscode.workspace.getConfiguration('kafkaLagMonitor');
  return {
    warning: config.get('lagWarningThreshold', 100),
    critical: config.get('lagCriticalThreshold', 1000),
  };
}
