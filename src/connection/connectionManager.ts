import { ConnectionProfile, ConnectionStatus } from './types';
import { KafkaAdminClient } from '../kafka/adminClient';
import { AdminService } from '../kafka/adminService';
import { KafkaProducerClient } from '../kafka/producerClient';
import { ProducerService } from '../kafka/producerService';

export interface ConnectionState {
  status: ConnectionStatus;
  error?: string;
}

export type AdminClientFactory = (profile: ConnectionProfile) => Promise<KafkaAdminClient>;
export type ProducerClientFactory = (profile: ConnectionProfile) => Promise<KafkaProducerClient>;

export class ConnectionManager {
  private readonly clients = new Map<string, KafkaAdminClient>();
  private readonly producers = new Map<string, KafkaProducerClient>();
  private readonly states = new Map<string, ConnectionState>();
  private readonly generations = new Map<string, number>();

  constructor(
    private readonly createAdminClient: AdminClientFactory,
    private readonly createProducerClient: ProducerClientFactory,
  ) {}

  getState(profileName: string): ConnectionState {
    return this.states.get(profileName) ?? { status: 'idle' };
  }

  private nextGeneration(profileName: string): number {
    const gen = (this.generations.get(profileName) ?? 0) + 1;
    this.generations.set(profileName, gen);
    return gen;
  }

  private isCurrentGeneration(profileName: string, gen: number): boolean {
    return this.generations.get(profileName) === gen;
  }

  async connect(profile: ConnectionProfile): Promise<void> {
    const gen = this.nextGeneration(profile.name);
    this.states.set(profile.name, { status: 'connecting' });
    try {
      let client = this.clients.get(profile.name);
      if (!client) {
        client = await this.createAdminClient(profile);
        this.clients.set(profile.name, client);
      }
      await client.connect();
      if (this.isCurrentGeneration(profile.name, gen)) {
        this.states.set(profile.name, { status: 'connected' });
      }
    } catch (err) {
      if (this.isCurrentGeneration(profile.name, gen)) {
        this.states.set(profile.name, { status: 'error', error: (err as Error).message });
      }
      throw err;
    }
  }

  async reconnect(profile: ConnectionProfile): Promise<void> {
    const gen = this.nextGeneration(profile.name);
    this.states.set(profile.name, { status: 'connecting' });

    const existing = this.clients.get(profile.name);
    if (existing) {
      this.clients.delete(profile.name);
      await existing.disconnect().catch(() => undefined);
    }
    const existingProducer = this.producers.get(profile.name);
    if (existingProducer) {
      this.producers.delete(profile.name);
      await existingProducer.disconnect().catch(() => undefined);
    }

    try {
      const client = await this.createAdminClient(profile);
      this.clients.set(profile.name, client);
      await client.connect();
      if (this.isCurrentGeneration(profile.name, gen)) {
        this.states.set(profile.name, { status: 'connected' });
      }
    } catch (err) {
      if (this.isCurrentGeneration(profile.name, gen)) {
        this.states.set(profile.name, { status: 'error', error: (err as Error).message });
      }
      throw err;
    }
  }

  async disconnect(profileName: string): Promise<void> {
    this.nextGeneration(profileName);
    const client = this.clients.get(profileName);
    if (client) {
      await client.disconnect();
      this.clients.delete(profileName);
    }
    const producer = this.producers.get(profileName);
    if (producer) {
      this.producers.delete(profileName);
      await producer.disconnect().catch(() => undefined);
    }
    this.states.set(profileName, { status: 'idle' });
  }

  getAdminService(profileName: string): AdminService | undefined {
    const state = this.getState(profileName);
    const client = this.clients.get(profileName);
    if (state.status !== 'connected' || !client) return undefined;
    return new AdminService(client);
  }

  async getProducerService(profile: ConnectionProfile): Promise<ProducerService | undefined> {
    const gen = this.generations.get(profile.name) ?? 0;
    if (this.getState(profile.name).status !== 'connected') return undefined;

    let client = this.producers.get(profile.name);
    if (!client) {
      client = await this.createProducerClient(profile);
      await client.connect();
      if (!this.isCurrentGeneration(profile.name, gen)) {
        await client.disconnect().catch(() => undefined);
        return this.getProducerService(profile);
      }
      this.producers.set(profile.name, client);
    }
    return new ProducerService(client);
  }
}
