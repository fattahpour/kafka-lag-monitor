import { BROKER_PATTERN } from './profileValidation';

export function validateProfileName(name: string, existingNames: string[]): string | null {
  const trimmed = name.trim();
  if (trimmed === '') {
    return '"name" must not be empty';
  }
  if (trimmed.includes('.')) {
    return '"name" must not contain "." (used as a separator in stored credential keys)';
  }
  if (existingNames.includes(trimmed)) {
    return `A connection named "${trimmed}" already exists`;
  }
  return null;
}

export interface ParsedBrokerList {
  brokers: string[];
  errors: string[];
}

export function parseBrokerList(input: string): ParsedBrokerList {
  const brokers: string[] = [];
  const errors: string[] = [];
  for (const raw of input.split(',')) {
    const broker = raw.trim();
    if (broker === '') continue;
    if (!BROKER_PATTERN.test(broker)) {
      errors.push(`"${broker}" must look like "host:port"`);
    } else {
      brokers.push(broker);
    }
  }
  if (brokers.length === 0 && errors.length === 0) {
    errors.push('At least one broker is required');
  }
  return { brokers, errors };
}
