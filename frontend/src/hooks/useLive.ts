import { useEffect, useReducer, useRef, useState } from "react";
import { ReconnectingSocket, type ConnectionStatus } from "../lib/socket";
import { applyUpdate, initialTrendState, type TrendState } from "../lib/trends";
import type { Frame, LivePost, MetricsSummary, TrendUpdate } from "../types";

type Action = { type: "update"; update: TrendUpdate; receivedAt: number };

function reducer(state: TrendState, action: Action): TrendState {
  return applyUpdate(state, action.update, action.receivedAt);
}

/** Live trend state from /ws/trends, with auto-reconnect and polling fallback. */
export function useTrendSocket(wsUrl: string, pollUrl: string) {
  const [state, dispatch] = useReducer(reducer, initialTrendState);
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
  const [frameRate, setFrameRate] = useState(0);
  const frames = useRef<number[]>([]);

  useEffect(() => {
    const sock = new ReconnectingSocket<TrendUpdate>(wsUrl, pollUrl, {
      onStatus: setStatus,
      onFrame: (frame: Frame<TrendUpdate>) => {
        if (!frame.data) return;
        const now = Date.now() / 1000;
        frames.current.push(now);
        dispatch({ type: "update", update: frame.data, receivedAt: now });
      },
    });
    sock.start();
    const rateTimer = window.setInterval(() => {
      const cutoff = Date.now() / 1000 - 10;
      frames.current = frames.current.filter((t) => t >= cutoff);
      setFrameRate(frames.current.length / 10);
    }, 1000);
    return () => {
      sock.stop();
      window.clearInterval(rateTimer);
    };
  }, [wsUrl, pollUrl]);

  return { state, status, frameRate };
}

/** Newest enriched posts from /ws/posts (no polling fallback; the ticker is best-effort). */
export function usePostStream(wsUrl: string, max = 40) {
  const [posts, setPosts] = useState<LivePost[]>([]);
  const buffer = useRef<LivePost[]>([]);

  useEffect(() => {
    const sock = new ReconnectingSocket<LivePost>(wsUrl, null, {
      onFrame: (frame) => {
        if (frame.type === "post" && frame.data) buffer.current.push(frame.data);
      },
    });
    sock.start();
    // Batch renders: at 200 posts/s re-rendering per message would be wasteful.
    const flush = window.setInterval(() => {
      if (!buffer.current.length) return;
      const incoming = buffer.current.splice(0).reverse();
      setPosts((prev) => [...incoming, ...prev].slice(0, max));
    }, 400);
    return () => {
      sock.stop();
      window.clearInterval(flush);
    };
  }, [wsUrl, max]);

  return posts;
}

/** Poll /api/metrics for pipeline throughput and latency. */
export function useMetrics(apiUrl: string, intervalMs = 3000) {
  const [summary, setSummary] = useState<MetricsSummary | null>(null);
  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      try {
        const r = await fetch(`${apiUrl}/api/metrics`);
        if (!r.ok) return;
        const body = await r.json();
        if (!cancelled) setSummary(body.summary as MetricsSummary);
      } catch {
        /* the header simply keeps the last value */
      }
    };
    load();
    const id = window.setInterval(load, intervalMs);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [apiUrl, intervalMs]);
  return summary;
}

/** Re-render periodically so relative times ("12s ago") stay current. */
export function useNow(intervalMs = 1000): number {
  const [now, setNow] = useState(() => Date.now() / 1000);
  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now() / 1000), intervalMs);
    return () => window.clearInterval(id);
  }, [intervalMs]);
  return now;
}
