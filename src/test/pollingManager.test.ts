import assert from 'node:assert/strict';
import test, { mock } from 'node:test';
import { PollingManager } from '../polling/pollingManager';

test('start() invokes tick repeatedly at the given interval', () => {
  mock.timers.enable({ apis: ['setInterval'] });
  try {
    const manager = new PollingManager();
    let calls = 0;
    manager.start(1000, () => {
      calls++;
    });

    mock.timers.tick(1000);
    assert.equal(calls, 1);

    mock.timers.tick(2000);
    assert.equal(calls, 3);
  } finally {
    mock.timers.reset();
  }
});

test('stop() halts further ticks', () => {
  mock.timers.enable({ apis: ['setInterval'] });
  try {
    const manager = new PollingManager();
    let calls = 0;
    manager.start(1000, () => {
      calls++;
    });

    mock.timers.tick(1000);
    assert.equal(calls, 1);

    manager.stop();
    mock.timers.tick(5000);
    assert.equal(calls, 1);
  } finally {
    mock.timers.reset();
  }
});

test('isRunning() reflects whether a timer is active', () => {
  mock.timers.enable({ apis: ['setInterval'] });
  try {
    const manager = new PollingManager();
    assert.equal(manager.isRunning(), false);

    manager.start(1000, () => {});
    assert.equal(manager.isRunning(), true);

    manager.stop();
    assert.equal(manager.isRunning(), false);
  } finally {
    mock.timers.reset();
  }
});

test('start() while already running restarts cleanly without double-ticking', () => {
  mock.timers.enable({ apis: ['setInterval'] });
  try {
    const manager = new PollingManager();
    let calls = 0;
    manager.start(1000, () => {
      calls++;
    });

    mock.timers.tick(500);
    manager.start(1000, () => {
      calls++;
    });

    mock.timers.tick(1000);
    assert.equal(calls, 1);
  } finally {
    mock.timers.reset();
  }
});
