import { MessagePage, MessageWindow } from '../kafka/consumerService';
import { escapeHtml } from './topicMetadataPanel';

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

export function renderMessageBrowserHtml(topic: string, data: MessageBrowserData): string {
  const safeTopic = escapeHtml(topic);
  const initialData = JSON.stringify(data).replace(/</g, '\\u003c');
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Messages: ${safeTopic}</title>
<style>
  body { font-family: var(--vscode-font-family, sans-serif); padding: 0 16px; }
  table { border-collapse: collapse; width: 100%; margin: 12px 0; }
  th, td { border: 1px solid var(--vscode-panel-border, #ccc); padding: 4px 8px; text-align: left; vertical-align: top; }
  th { background: var(--vscode-editor-lineHighlightBackground, #eee); }
  td.value, td.headers { font-family: var(--vscode-editor-font-family, monospace); white-space: pre-wrap; word-break: break-all; }
  .controls { display: flex; align-items: center; gap: 8px; margin: 12px 0; flex-wrap: wrap; }
  .show-more { background: none; border: none; color: var(--vscode-textLink-foreground, #3794ff); cursor: pointer; padding: 0; text-decoration: underline; }
  #banner { background: var(--vscode-inputValidation-errorBackground, #5a1d1d); border: 1px solid var(--vscode-inputValidation-errorBorder, #be1100); padding: 8px; margin: 12px 0; }
</style>
</head>
<body>
<h2 id="title"></h2>
<div class="controls">
  <label>Partition: <select id="partition"></select></label>
  <button id="earliest">Earliest</button>
  <button id="prev">Prev</button>
  <button id="next">Next</button>
  <button id="latest">Latest</button>
  <button id="refresh">Refresh</button>
</div>
<div id="banner" style="display:none"></div>
<p id="windowInfo"></p>
<table>
<thead><tr><th>Offset</th><th>Timestamp</th><th>Key</th><th>Value</th><th>Headers</th></tr></thead>
<tbody id="rows"></tbody>
</table>
<script>
  const vscode = acquireVsCodeApi();
  const initialData = ${initialData};
  const VALUE_TRUNCATE_LENGTH = ${VALUE_TRUNCATE_LENGTH};

  function appendTruncatable(cell, text) {
    if (text.length <= VALUE_TRUNCATE_LENGTH) {
      cell.textContent = text;
      return;
    }

    function renderTruncated() {
      cell.textContent = '';
      cell.appendChild(document.createTextNode(text.slice(0, VALUE_TRUNCATE_LENGTH) + '... '));
      const button = document.createElement('button');
      button.className = 'show-more';
      button.textContent = 'Show more';
      button.addEventListener('click', renderExpanded);
      cell.appendChild(button);
    }

    function renderExpanded() {
      cell.textContent = '';
      cell.appendChild(document.createTextNode(text + ' '));
      const button = document.createElement('button');
      button.className = 'show-more';
      button.textContent = 'Show less';
      button.addEventListener('click', renderTruncated);
      cell.appendChild(button);
    }

    renderTruncated();
  }

  function render(data) {
    document.getElementById('title').textContent = 'Messages: ' + data.topic + ' (partition ' + data.partition + ')';

    const partitionSelect = document.getElementById('partition');
    partitionSelect.textContent = '';
    for (let i = 0; i < data.partitionCount; i++) {
      const option = document.createElement('option');
      option.value = String(i);
      option.textContent = String(i);
      if (i === data.partition) option.selected = true;
      partitionSelect.appendChild(option);
    }

    document.getElementById('windowInfo').textContent =
      'Showing offsets ' + data.window.from + '-' + data.window.to + ' of ' + data.lowWatermark + '-' + data.highWatermark;

    document.getElementById('earliest').disabled = data.window.from <= data.lowWatermark;
    document.getElementById('prev').disabled = data.window.from <= data.lowWatermark;
    document.getElementById('next').disabled = data.window.to >= data.highWatermark;
    document.getElementById('latest').disabled = data.window.to >= data.highWatermark;

    const rows = document.getElementById('rows');
    rows.textContent = '';

    if (data.messages.length === 0) {
      const tr = document.createElement('tr');
      const td = document.createElement('td');
      td.colSpan = 5;
      td.textContent = 'No messages in this range.';
      tr.appendChild(td);
      rows.appendChild(tr);
      return;
    }

    for (const message of data.messages) {
      const tr = document.createElement('tr');

      const offsetCell = document.createElement('td');
      offsetCell.textContent = String(message.offset);
      tr.appendChild(offsetCell);

      const timestampCell = document.createElement('td');
      timestampCell.textContent = new Date(Number(message.timestamp)).toLocaleString();
      tr.appendChild(timestampCell);

      const keyCell = document.createElement('td');
      keyCell.textContent = message.key === null ? '(null)' : message.key;
      tr.appendChild(keyCell);

      const valueCell = document.createElement('td');
      valueCell.className = 'value';
      appendTruncatable(valueCell, message.value === null ? '(null)' : message.value);
      tr.appendChild(valueCell);

      const headersCell = document.createElement('td');
      headersCell.className = 'headers';
      headersCell.textContent = message.headers.map((h) => h.key + '=' + h.value).join(', ');
      tr.appendChild(headersCell);

      rows.appendChild(tr);
    }
  }

  render(initialData);

  window.addEventListener('message', (event) => {
    const message = event.data;
    if (message.type === 'update') {
      document.getElementById('banner').style.display = 'none';
      render(message.data);
    } else if (message.type === 'error') {
      const banner = document.getElementById('banner');
      banner.textContent = message.message;
      banner.style.display = 'block';
    }
  });

  document.getElementById('partition').addEventListener('change', (event) => {
    vscode.postMessage({ type: 'setPartition', partition: Number(event.target.value) });
  });

  document.getElementById('earliest').addEventListener('click', () => {
    vscode.postMessage({ type: 'nav', action: 'earliest' });
  });
  document.getElementById('prev').addEventListener('click', () => {
    vscode.postMessage({ type: 'nav', action: 'prev' });
  });
  document.getElementById('next').addEventListener('click', () => {
    vscode.postMessage({ type: 'nav', action: 'next' });
  });
  document.getElementById('latest').addEventListener('click', () => {
    vscode.postMessage({ type: 'nav', action: 'latest' });
  });
  document.getElementById('refresh').addEventListener('click', () => {
    vscode.postMessage({ type: 'nav', action: 'refresh' });
  });
</script>
</body>
</html>`;
}
