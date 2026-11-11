/**
 * Reconnecting WebSocket with exponential backoff (capped at 10 s) and an HTTP polling
 * fallback that runs only while the socket is down. All side-effecting dependencies are
 * injectable so the behaviour can be tested deterministically.
 */
import type { Frame } from "../types";

export type ConnectionStatus = "connecting" | "open" | "polling" | "offline";

export interface SocketLike {
  onopen: ((ev: unknown) => void) | null;
  onclose: ((ev: unknown) => void) | null;
  onerror: ((ev: unknown) => void) | null;
  onmessage: ((ev: { data: string }) => void) | null;
  close(): void;
}

export interface SocketDeps {
  createSocket: (url: string) => SocketLike;
  fetchJson: (url: string) => Promise<unknown>;
  setTimeout: (fn: () => void, ms: number) => unknown;
  clearTimeout: (id: unknown) => void;
  setInterval: (fn: () => void, ms: number) => unknown;
  clearInterval: (id: unknown) => void;
  random: () => number;
  now: () => number;
}

export interface SocketOptions {
  baseDelayMs?: number;
  maxDelayMs?: number;
  pollMs?: number;
  jitter?: number; // 0..1 fraction of the delay added at random
}

export interface SocketHandlers<T> {
  onFrame: (frame: Frame<T>) => void;
  onStatus?: (status: ConnectionStatus) => void;
}

export function backoffDelay(attempt: number, baseMs = 500, maxMs = 10_000): number {
  return Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt));
}

export const browserDeps = (): SocketDeps => ({
  createSocket: (url) => new WebSocket(url) as unknown as SocketLike,
  fetchJson: async (url) => {
    const r = await fetch(url);
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return r.json();
  },
  setTimeout: (fn, ms) => window.setTimeout(fn, ms),
  clearTimeout: (id) => window.clearTimeout(id as number),
  setInterval: (fn, ms) => window.setInterval(fn, ms),
  clearInterval: (id) => window.clearInterval(id as number),
  random: Math.random,
  now: () => Date.now(),
});

export class ReconnectingSocket<T = unknown> {
  private ws: SocketLike | null = null;
  private attempt = 0;
  private reconnectTimer: unknown = null;
  private pollTimer: unknown = null;
  private stopped = true;
  private status: ConnectionStatus = "offline";
  private readonly opts: Required<SocketOptions>;

  constructor(
    private readonly url: string,
    private readonly pollUrl: string | null,
    private readonly handlers: SocketHandlers<T>,
    opts: SocketOptions = {},
    private readonly deps: SocketDeps = browserDeps(),
  ) {
    this.opts = { baseDelayMs: 500, maxDelayMs: 10_000, pollMs: 2_000, jitter: 0.2, ...opts };
  }

  get currentStatus(): ConnectionStatus {
    return this.status;
  }

  get attempts(): number {
    return this.attempt;
  }

  start(): void {
    if (!this.stopped) return;
    this.stopped = false;
    this.connect();
  }

  stop(): void {
    this.stopped = true;
    if (this.reconnectTimer !== null) this.deps.clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.stopPolling();
    const ws = this.ws;
    this.ws = null;
    if (ws) {
      ws.onclose = ws.onerror = ws.onmessage = ws.onopen = null;
      ws.close();
    }
    this.setStatus("offline");
  }

  /** Delay before the next reconnect attempt (exposed for tests). */
  nextDelay(): number {
    const d = backoffDelay(this.attempt, this.opts.baseDelayMs, this.opts.maxDelayMs);
    return Math.min(this.opts.maxDelayMs, Math.round(d * (1 + this.opts.jitter * this.deps.random())));
  }

  private setStatus(s: ConnectionStatus): void {
    if (s === this.status) return;
    this.status = s;
    this.handlers.onStatus?.(s);
  }

  private connect(): void {
    if (this.stopped) return;
    if (this.status !== "polling") this.setStatus("connecting");
    let ws: SocketLike;
    try {
      ws = this.deps.createSocket(this.url);
    } catch {
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;
    ws.onopen = () => {
      this.attempt = 0;
      this.stopPolling();
      this.setStatus("open");
    };
    ws.onmessage = (ev) => {
      let frame: Frame<T>;
      try {
        frame = JSON.parse(ev.data) as Frame<T>;
      } catch {
        return;
      }
      if (frame.type === "ping") return;
      this.handlers.onFrame(frame);
    };
    ws.onerror = () => ws.close();
    ws.onclose = () => {
      if (this.ws !== ws) return;
      this.ws = null;
      if (this.stopped) return;
      this.startPolling();
      this.scheduleReconnect();
    };
  }

  private scheduleReconnect(): void {
    if (this.stopped || this.reconnectTimer !== null) return;
    const delay = this.nextDelay();
    this.attempt += 1;
    if (this.status !== "polling") this.setStatus(this.pollUrl ? "connecting" : "offline");
    this.reconnectTimer = this.deps.setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }

  private startPolling(): void {
    if (!this.pollUrl || this.pollTimer !== null) return;
    const poll = () => {
      this.deps
        .fetchJson(this.pollUrl as string)
        .then((data) => {
          if (this.stopped || this.status === "open") return;
          this.setStatus("polling");
          this.handlers.onFrame({ type: "poll", server_sent_at: this.deps.now() / 1000, data: data as T });
        })
        .catch(() => {
          if (!this.stopped && this.status !== "open") this.setStatus("offline");
        });
    };
    poll();
    this.pollTimer = this.deps.setInterval(poll, this.opts.pollMs);
  }

  private stopPolling(): void {
    if (this.pollTimer !== null) this.deps.clearInterval(this.pollTimer);
    this.pollTimer = null;
  }
}
