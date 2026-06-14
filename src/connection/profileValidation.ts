import { ConnectionProfile, SaslMechanism } from './types';

const SASL_MECHANISMS: SaslMechanism[] = ['plain', 'scram-sha-256', 'scram-sha-512'];
export const BROKER_PATTERN = /^[\w.-]+:\d+$/;

export function validateProfile(raw: unknown): { profile: ConnectionProfile | null; errors: string[] } {
  const errors: string[] = [];

  if (typeof raw !== 'object' || raw === null) {
    return { profile: null, errors: ['Connection must be an object'] };
  }
  const obj = raw as Record<string, unknown>;

  if (typeof obj.name !== 'string' || obj.name.trim() === '') {
    errors.push('"name" must be a non-empty string');
  }

  let brokers: string[] = [];
  if (!Array.isArray(obj.brokers) || obj.brokers.length === 0) {
    errors.push('"brokers" must be a non-empty array of "host:port" strings');
  } else {
    for (const b of obj.brokers) {
      if (typeof b !== 'string' || !BROKER_PATTERN.test(b)) {
        errors.push(`"brokers" entry "${String(b)}" must look like "host:port"`);
      }
    }
    brokers = obj.brokers as string[];
  }

  let sasl: ConnectionProfile['sasl'] = null;
  if (obj.sasl !== null && obj.sasl !== undefined) {
    if (typeof obj.sasl !== 'object') {
      errors.push('"sasl" must be an object or null');
    } else {
      const mechanism = (obj.sasl as Record<string, unknown>).mechanism;
      if (typeof mechanism !== 'string' || !SASL_MECHANISMS.includes(mechanism as SaslMechanism)) {
        errors.push(`"sasl.mechanism" must be one of ${SASL_MECHANISMS.join(', ')}`);
      } else {
        sasl = { mechanism: mechanism as SaslMechanism };
      }
    }
  }

  const ssl = obj.ssl === true;
  const clientId =
    typeof obj.clientId === 'string' && obj.clientId.trim() !== '' ? obj.clientId : 'kafka-lag-monitor';

  if (errors.length > 0) {
    return { profile: null, errors };
  }

  return {
    profile: { name: obj.name as string, brokers, sasl, ssl, clientId },
    errors: [],
  };
}

export function parseConnectionProfiles(raw: unknown): {
  profiles: ConnectionProfile[];
  errors: { index: number; errors: string[] }[];
} {
  if (!Array.isArray(raw)) {
    return { profiles: [], errors: [] };
  }
  const profiles: ConnectionProfile[] = [];
  const errors: { index: number; errors: string[] }[] = [];
  raw.forEach((item, index) => {
    const result = validateProfile(item);
    if (result.profile) {
      profiles.push(result.profile);
    } else {
      errors.push({ index, errors: result.errors });
    }
  });
  return { profiles, errors };
}
