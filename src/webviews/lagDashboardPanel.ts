import { LagSeverity, TopicLag, lagSeverity } from '../kafka/lag';
import { Thresholds } from '../connection/profileStore';
import { escapeHtml } from './topicMetadataPanel';

export interface PartitionLagView {
  partition: number;
  currentOffset: number;
  endOffset: number;
  lag: number;
  percentConsumed: number;
  severity: LagSeverity;
}

export interface TopicLagView {
  topic: string;
  totalLag: number;
  partitions: PartitionLagView[];
}

export interface LagDashboardData {
  groupId: string;
  totalLag: number;
  severity: LagSeverity;
  overThresholdCount: number;
  topics: TopicLagView[];
}

export function toDashboardData(groupId: string, topicLags: TopicLag[], thresholds: Thresholds): LagDashboardData {
  let totalLag = 0;
  let overThresholdCount = 0;

  const topics: TopicLagView[] = topicLags.map((topicLag) => {
    const partitions: PartitionLagView[] = topicLag.partitions.map((p) => {
      const percentConsumed = p.endOffset === 0 ? 100 : Math.round((p.currentOffset / p.endOffset) * 100);
      const severity = lagSeverity(p.lag, thresholds.warning, thresholds.critical);
      if (severity !== 'none') {
        overThresholdCount++;
      }
      totalLag += p.lag;
      return {
        partition: p.partition,
        currentOffset: p.currentOffset,
        endOffset: p.endOffset,
        lag: p.lag,
        percentConsumed,
        severity,
      };
    });
    return { topic: topicLag.topic, totalLag: topicLag.totalLag, partitions };
  });

  return {
    groupId,
    totalLag,
    severity: lagSeverity(totalLag, thresholds.warning, thresholds.critical),
    overThresholdCount,
    topics,
  };
}

export function renderLagDashboardHtml(groupId: string, data: LagDashboardData, pollIntervalSeconds: number): string {
  const safeGroupId = escapeHtml(groupId);
  const initialData = JSON.stringify(data);
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Lag: ${safeGroupId}</title>
<style>
  body { font-family: var(--vscode-font-family, sans-serif); padding: 0 16px; }
  .summary { display: flex; gap: 16px; margin: 12px 0; }
  .card { border: 1px solid var(--vscode-panel-border, #ccc); padding: 8px 12px; border-radius: 4px; }
  #banner { background: var(--vscode-inputValidation-warningBackground, #5a3d00); border: 1px solid var(--vscode-inputValidation-warningBorder, #b89500); padding: 8px; margin: 12px 0; }
  section { margin-bottom: 16px; }
  .partition-row { display: flex; align-items: center; gap: 8px; margin: 4px 0; }
  .partition-row span { min-width: 220px; }
  .bar { flex: 1; height: 10px; border: 1px solid var(--vscode-panel-border, #ccc); position: relative; }
  .bar[data-severity="warning"] { border-color: var(--vscode-editorWarning-foreground, #cca700); }
  .bar[data-severity="critical"] { border-color: var(--vscode-editorError-foreground, #f14c4c); }
  .bar-fill { height: 100%; background: var(--vscode-progressBar-background, #0e70c0); }
  .status-none { color: var(--vscode-foreground); }
  .status-warning { color: var(--vscode-editorWarning-foreground, #cca700); }
  .status-critical { color: var(--vscode-editorError-foreground, #f14c4c); }
</style>
</head>
<body>
<h2 id="groupTitle"></h2>
<button id="refresh">Refresh</button>
<label><input type="checkbox" id="autopoll"> Auto-refresh every ${pollIntervalSeconds}s</label>
<div id="banner" style="display:none"></div>
<div class="summary">
  <div class="card">Total Lag: <span id="totalLag"></span></div>
  <div class="card">Status: <span id="status"></span></div>
  <div class="card">Partitions over threshold: <span id="overThreshold"></span></div>
</div>
<div id="topics"></div>
<script>
  const vscode = acquireVsCodeApi();
  const initialData = ${initialData};

  function render(data) {
    document.getElementById('groupTitle').textContent = 'Lag: ' + data.groupId;
    document.getElementById('totalLag').textContent = String(data.totalLag);
    const status = document.getElementById('status');
    status.textContent = data.severity;
    status.className = 'status-' + data.severity;
    document.getElementById('overThreshold').textContent = String(data.overThresholdCount);

    const topicsEl = document.getElementById('topics');
    topicsEl.textContent = '';

    if (data.topics.length === 0) {
      const p = document.createElement('p');
      p.textContent = 'No topic lag data.';
      topicsEl.appendChild(p);
      return;
    }

    for (const topic of data.topics) {
      const section = document.createElement('section');
      const h3 = document.createElement('h3');
      h3.textContent = topic.topic + ' — total lag: ' + topic.totalLag;
      section.appendChild(h3);

      for (const partition of topic.partitions) {
        const row = document.createElement('div');
        row.className = 'partition-row';

        const label = document.createElement('span');
        label.textContent = 'p' + partition.partition + ': ' + partition.currentOffset + '/' + partition.endOffset + ' (lag ' + partition.lag + ')';
        row.appendChild(label);

        const bar = document.createElement('div');
        bar.className = 'bar';
        bar.dataset.severity = partition.severity;

        const fill = document.createElement('div');
        fill.className = 'bar-fill';
        fill.style.width = partition.percentConsumed + '%';
        bar.appendChild(fill);

        row.appendChild(bar);
        section.appendChild(row);
      }

      topicsEl.appendChild(section);
    }
  }

  render(initialData);

  window.addEventListener('message', (event) => {
    const message = event.data;
    if (message.type === 'update') {
      document.getElementById('banner').style.display = 'none';
      render(message.data);
    } else if (message.type === 'pollError') {
      const banner = document.getElementById('banner');
      banner.textContent = message.showReconnectHint
        ? message.message + ' Use Kafka: Reconnect on this connection in the sidebar.'
        : message.message;
      banner.style.display = 'block';
    }
  });

  document.getElementById('refresh').addEventListener('click', () => {
    vscode.postMessage({ type: 'refresh' });
  });

  document.getElementById('autopoll').addEventListener('change', (event) => {
    vscode.postMessage({ type: 'setAutoPoll', enabled: event.target.checked });
  });
</script>
</body>
</html>`;
}
