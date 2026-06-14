export type SaslMechanism = 'plain' | 'scram-sha-256' | 'scram-sha-512';

export interface SaslConfig {
  mechanism: SaslMechanism;
}

export interface ConnectionProfile {
  name: string;
  brokers: string[];
  sasl: SaslConfig | null;
  ssl: boolean;
  clientId: string;
}

export type ConnectionStatus = 'idle' | 'connecting' | 'connected' | 'error';
