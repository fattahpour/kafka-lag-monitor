export const PAGE_SIZE = 50;

export interface MessageWindow {
  from: number;
  to: number;
}

export type NavAction = 'latest' | 'earliest' | 'prev' | 'next' | 'refresh';

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
