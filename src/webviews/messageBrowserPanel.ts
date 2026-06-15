import { MessagePage, MessageWindow } from '../kafka/consumerService';

// Used by renderMessageBrowserHtml (Task 6) to truncate long values in the table view.
export const VALUE_TRUNCATE_LENGTH = 300;

export interface MessageHeaderView {
  key: string;
  value: string;
}

export interface MessageRowView {
  offset: number;
  timestamp: string;
  key: string | null;
  value: string | null;
  headers: MessageHeaderView[];
}

export interface MessageBrowserData {
  topic: string;
  partition: number;
  partitionCount: number;
  lowWatermark: number;
  highWatermark: number;
  window: MessageWindow;
  messages: MessageRowView[];
}

function formatValue(value: string | null): string | null {
  if (value === null) return null;
  try {
    return JSON.stringify(JSON.parse(value), null, 2);
  } catch {
    return value;
  }
}

export function toMessageBrowserData(topic: string, partitionCount: number, page: MessagePage): MessageBrowserData {
  return {
    topic,
    partition: page.partition,
    partitionCount,
    lowWatermark: page.lowWatermark,
    highWatermark: page.highWatermark,
    window: page.window,
    messages: page.messages.map((m) => ({
      offset: m.offset,
      timestamp: m.timestamp,
      key: m.key,
      value: formatValue(m.value),
      headers: Object.entries(m.headers).map(([key, value]) => ({ key, value })),
    })),
  };
}
