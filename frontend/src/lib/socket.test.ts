import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ReconnectingSocket, backoffDelay, type ConnectionStatus, type SocketDeps, type SocketLike } from "./socket";

class FakeSocket implements SocketLike {
  onopen: ((ev: unknown) => void) | null = null;
  onclose: ((ev: unknown) => void) | null = null;
  onerror: ((ev: unknown) => void) | null = null;
  onmessage: ((ev: { data: string }) => void) | null = null;
  closed = false;
  constructor(public url: string) {}
  open() {
    this.onopen?.({});
  }
  send(obj: unknown) {
    this.onmessage?.({ data: JSON.stringify(obj) });
  }
  drop() {
    this.closed = true;
    this.onclose?.({});
  }
  close() {
    if (this.closed) return;
    this.closed = true;
    this.onclose?.({});
  }
}

function setup(fetchImpl?: SocketDeps["fetchJson"], pollUrl: string | null = "http://api/trends") {
  const sockets: FakeSocket[] = [];
  const frames: unknown[] = [];
  const statuses: ConnectionStatus[] = [];
  const deps: SocketDeps = {
    createSocket: (url) => {
      const s = new FakeSocket(url);
      sockets.push(s);
      return s;
    },
    fetchJson: fetchImpl ?? (async () => ({ generated_at: 1, trends: [] })),
    setTimeout: (fn, ms) => setTimeout(fn, ms),
    clearTimeout: (id) => clearTimeout(id as ReturnType<typeof setTimeout>),
    setInterval: (fn, ms) => setInterval(fn, ms),
    clearInterval: (id) => clearInterval(id as ReturnType<typeof setInterval>),
    random: () => 0,
    now: () => Date.now(),
  };
  const sock = new ReconnectingSocket(
    "ws://x/ws/trends",
    pollUrl,
    { onFrame: (f) => frames.push(f), onStatus: (s) => statuses.push(s) },
    { baseDelayMs: 500, maxDelayMs: 10_000, pollMs: 2000, jitter: 0 },
    deps,
  );
  return { sock, sockets, frames, statuses };
}

describe("backoffDelay", () => {
  it("doubles and caps at 10s", () => {
    expect([0, 1, 2, 3, 4, 5, 6, 10].map((a) => backoffDelay(a))).toEqual([
      500, 1000, 2000, 4000, 8000, 10000, 10000, 10000,
    ]);
  });
});

describe("ReconnectingSocket", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("connects, forwards frames and ignores pings", () => {
    const { sock, sockets, frames, statuses } = setup();
    sock.start();
    expect(statuses).toEqual(["connecting"]);
    sockets[0].open();
    expect(sock.currentStatus).toBe("open");
    sockets[0].send({ type: "ping", server_sent_at: 1 });
    sockets[0].send({ type: "update", server_sent_at: 2, data: { a: 1 } });
    expect(frames).toEqual([{ type: "update", server_sent_at: 2, data: { a: 1 } }]);
  });

  it("reconnects with exponential backoff capped at 10s", async () => {
    const { sock, sockets } = setup(async () => {
      throw new Error("down");
    });
    sock.start();
    const expected = [500, 1000, 2000, 4000, 8000, 10000, 10000];
    for (let i = 0; i < expected.length; i++) {
      sockets[i].drop();
      await vi.advanceTimersByTimeAsync(expected[i] - 1);
      expect(sockets.length).toBe(i + 1); // not yet
      await vi.advanceTimersByTimeAsync(1);
      expect(sockets.length).toBe(i + 2); // reconnected exactly on schedule
    }
  });

  it("resets backoff after a successful open", async () => {
    const { sock, sockets } = setup();
    sock.start();
    sockets[0].drop();
    await vi.advanceTimersByTimeAsync(500);
    sockets[1].drop();
    await vi.advanceTimersByTimeAsync(1000);
    sockets[2].open();
    expect(sock.attempts).toBe(0);
    sockets[2].drop();
    await vi.advanceTimersByTimeAsync(500);
    expect(sockets.length).toBe(4);
  });

  it("polls the REST snapshot only while disconnected", async () => {
    const fetchJson = vi.fn(async () => ({ generated_at: 5, trends: [] }));
    const { sock, sockets, frames } = setup(fetchJson);
    sock.start();
    sockets[0].open();
    await vi.advanceTimersByTimeAsync(5000);
    expect(fetchJson).not.toHaveBeenCalled();

    sockets[0].drop();
    await vi.advanceTimersByTimeAsync(0);
    expect(fetchJson).toHaveBeenCalledTimes(1);
    expect(sock.currentStatus).toBe("polling");
    expect(frames.at(-1)).toMatchObject({ type: "poll", data: { generated_at: 5 } });

    // socket comes back: polling stops
    await vi.advanceTimersByTimeAsync(500);
    sockets[1].open();
    const calls = fetchJson.mock.calls.length;
    await vi.advanceTimersByTimeAsync(10_000);
    expect(fetchJson.mock.calls.length).toBe(calls);
    expect(sock.currentStatus).toBe("open");
  });

  it("goes offline when both socket and polling fail", async () => {
    const { sock, sockets } = setup(async () => {
      throw new Error("down");
    });
    sock.start();
    sockets[0].drop();
    await vi.advanceTimersByTimeAsync(0);
    expect(sock.currentStatus).toBe("offline");
  });

  it("stop() closes the socket and cancels timers", async () => {
    const { sock, sockets } = setup();
    sock.start();
    sockets[0].drop();
    sock.stop();
    await vi.advanceTimersByTimeAsync(60_000);
    expect(sockets.length).toBe(1);
    expect(sock.currentStatus).toBe("offline");
  });

  it("works without a poll URL", async () => {
    const { sock, sockets, frames } = setup(undefined, null);
    sock.start();
    sockets[0].drop();
    await vi.advanceTimersByTimeAsync(500);
    expect(sockets.length).toBe(2);
    expect(frames).toEqual([]);
  });
});
