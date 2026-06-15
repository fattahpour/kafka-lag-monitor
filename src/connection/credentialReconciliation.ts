import { ConnectionProfile } from './types';

export interface WizardResult {
  profile: ConnectionProfile;
  username?: string;
  password?: string;
  tlsPassphrase?: string;
}

export interface CredentialChanges {
  set: Record<string, string>;
  delete: string[];
}

export function planCredentialChanges(result: WizardResult): CredentialChanges {
  const set: Record<string, string> = {};
  const del: string[] = [];

  if (result.profile.sasl) {
    if (result.username) set.username = result.username;
    if (result.password) set.password = result.password;
  } else {
    del.push('username', 'password');
  }

  if (typeof result.profile.ssl === 'object') {
    if (result.tlsPassphrase) {
      set.tlsKeyPassphrase = result.tlsPassphrase;
    } else if (result.tlsPassphrase === undefined) {
      del.push('tlsKeyPassphrase');
    }
  } else {
    del.push('tlsKeyPassphrase');
  }

  return { set, delete: del };
}
