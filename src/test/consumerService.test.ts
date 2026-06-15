import assert from 'node:assert/strict';
import test from 'node:test';
import { computeWindow } from '../kafka/consumerService';

test('computeWindow latest and earliest for a normal partition', () => {
  assert.deepEqual(computeWindow('latest', 0, 200), { from: 150, to: 200 });
  assert.deepEqual(computeWindow('earliest', 0, 200), { from: 0, to: 50 });
});

test('computeWindow latest and earliest for an empty partition', () => {
  assert.deepEqual(computeWindow('latest', 100, 100), { from: 100, to: 100 });
  assert.deepEqual(computeWindow('earliest', 100, 100), { from: 100, to: 100 });
});

test('computeWindow latest and earliest for a partition with fewer than PAGE_SIZE messages', () => {
  assert.deepEqual(computeWindow('latest', 0, 30), { from: 0, to: 30 });
  assert.deepEqual(computeWindow('earliest', 0, 30), { from: 0, to: 30 });
});

test('computeWindow prev and next from a mid-range window', () => {
  assert.deepEqual(computeWindow('prev', 0, 200, { from: 150, to: 200 }), { from: 100, to: 150 });
  assert.deepEqual(computeWindow('next', 0, 200, { from: 100, to: 150 }), { from: 150, to: 200 });
});

test('computeWindow prev and next at the low/high watermark boundary return an empty window', () => {
  assert.deepEqual(computeWindow('prev', 0, 200, { from: 0, to: 50 }), { from: 0, to: 0 });
  assert.deepEqual(computeWindow('next', 0, 200, { from: 150, to: 200 }), { from: 200, to: 200 });
});

test('computeWindow refresh clamps the current window into the low/high range', () => {
  assert.deepEqual(computeWindow('refresh', 100, 200, { from: 0, to: 50 }), { from: 100, to: 100 });
  assert.deepEqual(computeWindow('refresh', 0, 120, { from: 100, to: 200 }), { from: 100, to: 120 });
});

test('computeWindow prev, next, and refresh fall back to latest when there is no current window', () => {
  assert.deepEqual(computeWindow('prev', 0, 200), { from: 150, to: 200 });
  assert.deepEqual(computeWindow('next', 0, 200), { from: 150, to: 200 });
  assert.deepEqual(computeWindow('refresh', 0, 200), { from: 150, to: 200 });
});
