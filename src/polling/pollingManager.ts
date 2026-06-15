export class PollingManager {
  private timer: NodeJS.Timeout | undefined;

  start(intervalMs: number, tick: () => void): void {
    this.stop();
    this.timer = setInterval(tick, intervalMs);
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
  }

  isRunning(): boolean {
    return this.timer !== undefined;
  }
}
