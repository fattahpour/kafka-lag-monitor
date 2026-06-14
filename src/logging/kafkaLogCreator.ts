export interface KafkaLogEntry {
  namespace: string;
  level: number;
  label: string;
  log: { message: string; [key: string]: unknown };
}

export type KafkaLogCreator = (logLevel: number) => (entry: KafkaLogEntry) => void;

export function createKafkaLogCreator(sink: (line: string) => void): KafkaLogCreator {
  return () => (entry: KafkaLogEntry) => {
    const { namespace, label, log } = entry;
    const { message, ...extra } = log;
    const extraKeys = Object.keys(extra);
    const suffix =
      extraKeys.length > 0
        ? ' (' + extraKeys.map((k) => `${k}=${String(extra[k])}`).join(', ') + ')'
        : '';
    sink(`[${label}] ${namespace}: ${message}${suffix}`);
  };
}
