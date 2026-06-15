import { AdminService } from './adminService';
import { KafkaConsumerClient, RawKafkaMessage } from './consumerClient';

export const PAGE_SIZE = 50;

export interface MessageWindow {
  from: number;
  to: number;
}

export type NavAction = 'latest' | 'earliest' | 'prev' | 'next' | 'refresh';

export interface MessageView {
  offset: number;
  timestamp: string;
  key: string | null;
  value: string | null;
  headers: Record<string, string>;
}

export interface MessagePage {
  partition: number;
  lowWatermark: number;
  highWatermark: number;
  window: MessageWindow;
  messages: MessageView[];
}

export function computeWindow(
  action: NavAction,
  low: number,
  high: number,
  current?: MessageWindow,
): MessageWindow {
  switch (action) {
    case 'latest':
      return { from: Math.max(high - PAGE_SIZE, low), to: high };
    case 'earliest':
      return { from: low, to: Math.min(low + PAGE_SIZE, high) };
    case 'prev': {
      if (!current) return computeWindow('latest', low, high);
      const to = Math.max(current.from, low);
      const from = Math.max(to - PAGE_SIZE, low);
      return { from, to };
    }
    case 'next': {
      if (!current) return computeWindow('latest', low, high);
      const from = Math.min(current.to, high);
      const to = Math.min(from + PAGE_SIZE, high);
      return { from, to };
    }
    case 'refresh': {
      if (!current) return computeWindow('latest', low, high);
      const from = Math.min(Math.max(current.from, low), high);
      const to = Math.min(Math.max(current.to, from), high);
      return { from, to };
    }
  }
}

function toMessageView(raw: RawKafkaMessage): MessageView {
  return {
    offset: Number(raw.offset),
    timestamp: raw.timestamp,
    key: raw.key,
    value: raw.value,
    headers: raw.headers,
  };
}

export class ConsumerService {
  constructor(
    private readonly consumerClient: KafkaConsumerClient,
    private readonly adminService: AdminService,
  ) {}

  async fetchPage(
    topic: string,
    partition: number,
    action: NavAction,
    currentWindow?: MessageWindow,
  ): Promise<MessagePage> {
    const offsets = await this.adminService.getTopicOffsets(topic);
    const partitionOffsets = offsets.find((o) => o.partition === partition);
    if (!partitionOffsets) {
      throw new Error(`Partition ${partition} not found for topic "${topic}"`);
    }
    const { low, high } = partitionOffsets;
    const window = computeWindow(action, low, high, currentWindow);
    const raw = await this.consumerClient.fetchMessages({
      topic,
      partition,
      fromOffset: window.from,
      toOffset: window.to,
    });
    return {
      partition,
      lowWatermark: low,
      highWatermark: high,
      window,
      messages: raw.map(toMessageView),
    };
  }
}
