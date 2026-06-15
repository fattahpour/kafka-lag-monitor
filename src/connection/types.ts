export type SaslMechanism = 'plain' | 'scram-sha-256' | 'scram-sha-512';

export interface SaslConfig {
  mechanism: SaslMechanism;
}

export interface MtlsConfig {
  ca?: string;
  cert: string;
  key: string;
}

export interface ConnectionProfile {
  name: string;
  brokers: string[];
  sasl: SaslConfig | null;
  ssl: boolean | MtlsConfig;
  clientId: string;
}

export type ConnectionStatus = 'idle' | 'connecting' | 'connected' | 'error';
