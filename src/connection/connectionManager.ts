import { ConnectionProfile, ConnectionStatus } from './types';
import { KafkaAdminClient } from '../kafka/adminClient';
import { AdminService } from '../kafka/adminService';

export interface ConnectionState {
  status: ConnectionStatus;
  error?: string;
}

export type AdminClientFactory = (profile: ConnectionProfile) => KafkaAdminClient;

export class ConnectionManager {
  private readonly clients = new Map<string, KafkaAdminClient>();
  private readonly states = new Map<string, ConnectionState>();

  constructor(private readonly createAdminClient: AdminClientFactory) {}

  getState(profileName: string): ConnectionState {
    return this.states.get(profileName) ?? { status: 'idle' };
  }

  async connect(profile: ConnectionProfile): Promise<void> {
    this.states.set(profile.name, { status: 'connecting' });
    try {
      let client = this.clients.get(profile.name);
      if (!client) {
        client = this.createAdminClient(profile);
        this.clients.set(profile.name, client);
      }
      await client.connect();
      this.states.set(profile.name, { status: 'connected' });
    } catch (err) {
      this.states.set(profile.name, { status: 'error', error: (err as Error).message });
      throw err;
    }
  }

  async disconnect(profileName: string): Promise<void> {
    const client = this.clients.get(profileName);
    if (client) {
      await client.disconnect();
      this.clients.delete(profileName);
    }
    this.states.set(profileName, { status: 'idle' });
  }

  getAdminService(profileName: string): AdminService | undefined {
    const state = this.getState(profileName);
    const client = this.clients.get(profileName);
    if (state.status !== 'connected' || !client) return undefined;
    return new AdminService(client);
  }
}
