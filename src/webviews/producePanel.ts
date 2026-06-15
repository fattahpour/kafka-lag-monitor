import { escapeHtml } from './topicMetadataPanel';
import { HeaderEntry } from '../kafka/producerService';

export interface ProduceSendMessage {
  type: 'send';
  partition: number | null;
  key: string;
  value: string;
  headers: HeaderEntry[];
}

export function renderProduceHtml(topic: string, partitionCount: number): string {
  const safeTopic = escapeHtml(topic);
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Produce: ${safeTopic}</title>
<style>
  body { font-family: var(--vscode-font-family, sans-serif); padding: 0 16px; }
  label { display: block; margin: 12px 0 4px; font-weight: bold; }
  select, input[type="text"], textarea {
    width: 100%; box-sizing: border-box; font-family: var(--vscode-font-family, sans-serif);
  }
  textarea { font-family: var(--vscode-editor-font-family, monospace); min-height: 120px; }
  .header-row { display: flex; gap: 8px; margin-bottom: 4px; }
  .header-row input { flex: 1; }
  #result { margin: 12px 0; padding: 8px; display: none; }
  #result.success {
    background: var(--vscode-inputValidation-infoBackground, #1d3a5a);
    border: 1px solid var(--vscode-inputValidation-infoBorder, #3794ff);
  }
  #result.error {
    background: var(--vscode-inputValidation-errorBackground, #5a1d1d);
    border: 1px solid var(--vscode-inputValidation-errorBorder, #be1100);
  }
  .actions { margin: 16px 0; }
</style>
</head>
<body>
<h2>Produce: ${safeTopic}</h2>

<label for="partition">Partition</label>
<select id="partition"></select>

<label for="key">Key</label>
<input type="text" id="key">

<label for="value">Value</label>
<textarea id="value"></textarea>

<label>Headers</label>
<div id="headers"></div>
<button id="addHeader" type="button">+ Add header</button>

<div class="actions">
  <button id="send" type="button">Send</button>
</div>
<div id="result"></div>

<script>
  const vscode = acquireVsCodeApi();
  const PARTITION_COUNT = ${partitionCount};

  const partitionSelect = document.getElementById('partition');
  const autoOption = document.createElement('option');
  autoOption.value = '';
  autoOption.textContent = 'Auto (by key)';
  partitionSelect.appendChild(autoOption);
  for (let i = 0; i < PARTITION_COUNT; i++) {
    const option = document.createElement('option');
    option.value = String(i);
    option.textContent = String(i);
    partitionSelect.appendChild(option);
  }

  const headersContainer = document.getElementById('headers');
  function addHeaderRow() {
    const row = document.createElement('div');
    row.className = 'header-row';

    const keyInput = document.createElement('input');
    keyInput.type = 'text';
    keyInput.placeholder = 'Header key';
    row.appendChild(keyInput);

    const valueInput = document.createElement('input');
    valueInput.type = 'text';
    valueInput.placeholder = 'Header value';
    row.appendChild(valueInput);

    const removeButton = document.createElement('button');
    removeButton.type = 'button';
    removeButton.textContent = 'Remove';
    removeButton.addEventListener('click', () => row.remove());
    row.appendChild(removeButton);

    headersContainer.appendChild(row);
  }
  document.getElementById('addHeader').addEventListener('click', addHeaderRow);

  const sendButton = document.getElementById('send');
  const resultDiv = document.getElementById('result');

  sendButton.addEventListener('click', () => {
    const headers = [];
    for (const row of headersContainer.children) {
      const inputs = row.querySelectorAll('input');
      headers.push({ key: inputs[0].value, value: inputs[1].value });
    }
    const partitionValue = partitionSelect.value;
    sendButton.disabled = true;
    resultDiv.style.display = 'none';
    vscode.postMessage({
      type: 'send',
      partition: partitionValue === '' ? null : Number(partitionValue),
      key: document.getElementById('key').value,
      value: document.getElementById('value').value,
      headers,
    });
  });

  window.addEventListener('message', (event) => {
    const message = event.data;
    if (message.type !== 'result') return;
    sendButton.disabled = false;
    resultDiv.style.display = 'block';
    if (message.success) {
      resultDiv.className = 'success';
      resultDiv.textContent = 'Sent to partition ' + message.partition + ', offset ' + message.offset;
    } else {
      resultDiv.className = 'error';
      resultDiv.textContent = message.message;
    }
  });
</script>
</body>
</html>`;
}
