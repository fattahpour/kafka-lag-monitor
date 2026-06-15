import { ConfigEntry, TopicMetadata } from '../kafka/adminService';

export function escapeHtml(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function renderPartitionRows(metadata: TopicMetadata): string {
  return metadata.partitions
    .map(
      (p) =>
        `<tr><td>${p.partitionId}</td><td>${p.leader}</td><td>${p.replicas.join(', ')}</td><td>${p.isr.join(', ')}</td></tr>`,
    )
    .join('');
}

function renderConfigRows(configEntries: ConfigEntry[]): string {
  return configEntries
    .map(
      (c) =>
        `<tr><td>${escapeHtml(c.name)}</td><td>${escapeHtml(c.value ?? '')}</td><td>${c.isDefault ? 'Yes' : 'No'}</td></tr>`,
    )
    .join('');
}

export function renderTopicMetadataHtml(topicName: string, metadata: TopicMetadata, configEntries: ConfigEntry[]): string {
  const safeName = escapeHtml(topicName);
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Topic: ${safeName}</title>
<style>
  body { font-family: var(--vscode-font-family, sans-serif); padding: 0 16px; }
  table { border-collapse: collapse; width: 100%; margin-bottom: 24px; }
  th, td { border: 1px solid var(--vscode-panel-border, #ccc); padding: 4px 8px; text-align: left; }
  th { background: var(--vscode-editor-lineHighlightBackground, #eee); }
</style>
</head>
<body>
<h2>${safeName}</h2>
<button id="refresh">Refresh</button>
<h3>Partitions</h3>
<table>
<thead><tr><th>Partition</th><th>Leader</th><th>Replicas</th><th>ISR</th></tr></thead>
<tbody>${renderPartitionRows(metadata)}</tbody>
</table>
<h3>Config</h3>
<table>
<thead><tr><th>Name</th><th>Value</th><th>Default?</th></tr></thead>
<tbody>${renderConfigRows(configEntries)}</tbody>
</table>
<script>
  const vscode = acquireVsCodeApi();
  document.getElementById('refresh').addEventListener('click', () => {
    vscode.postMessage({ type: 'refresh' });
  });
</script>
</body>
</html>`;
}

export function renderErrorHtml(message: string): string {
  return `<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><title>Topic Metadata</title></head>
<body>
<p>${escapeHtml(message)}</p>
</body>
</html>`;
}
