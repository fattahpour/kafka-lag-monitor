import * as vscode from 'vscode';
import { parseConnectionProfiles } from './profileValidation';
import { ConnectionProfile } from './types';

export interface Thresholds {
  warning: number;
  critical: number;
}

export function getConnectionProfiles(onError: (message: string) => void): ConnectionProfile[] {
  const raw = vscode.workspace.getConfiguration('kafkaLagMonitor').get('connections', []);
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
