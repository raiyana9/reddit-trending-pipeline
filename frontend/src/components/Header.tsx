import type { ConnectionStatus } from "../lib/socket";
import { formatNumber, formatSeconds } from "../lib/format";
import type { MetricsSummary } from "../types";

const STATUS: Record<ConnectionStatus, { dot: string; label: string }> = {
  open: { dot: "bg-emerald-500", label: "Live" },
  connecting: { dot: "bg-amber-400", label: "Connecting" },
  polling: { dot: "bg-amber-400", label: "Polling" },
  offline: { dot: "bg-red-500", label: "Offline" },
};

interface Props {
  status: ConnectionStatus;
  freshnessS: number | null;
  metrics: MetricsSummary | null;
  postsInWindow: number;
  dark: boolean;
  onToggleTheme: () => void;
}

function Stat({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="min-w-0" title={hint}>
      <div className="text-[11px] uppercase tracking-wider text-zinc-500 dark:text-zinc-400">{label}</div>
      <div className="font-mono text-sm font-semibold tabular-nums sm:text-base">{value}</div>
    </div>
  );
}

export function Header({ status, freshnessS, metrics, postsInWindow, dark, onToggleTheme }: Props) {
  const s = STATUS[status];
  return (
    <header className="sticky top-0 z-30 border-b border-zinc-200 bg-white/85 backdrop-blur dark:border-zinc-800 dark:bg-[#0b0d12]/85">
      <div className="mx-auto flex max-w-7xl flex-wrap items-center gap-x-6 gap-y-2 px-4 py-3">
        <div className="flex items-center gap-3">
          <span className="relative flex h-3 w-3" aria-label={`connection ${s.label}`}>
            {status === "open" && (
              <span className={`absolute inline-flex h-full w-full animate-ping rounded-full opacity-60 ${s.dot}`} />
            )}
            <span className={`relative inline-flex h-3 w-3 rounded-full ${s.dot}`} />
          </span>
          <div>
            <h1 className="text-base font-bold leading-tight sm:text-lg">Reddit Trending</h1>
            <div className="text-xs text-zinc-500 dark:text-zinc-400" data-testid="status-label">
              {s.label}
            </div>
          </div>
        </div>

        <div className="order-3 grid w-full grid-cols-4 gap-4 sm:order-none sm:ml-auto sm:w-auto sm:gap-6">
          <Stat
            label="Posts/s"
            value={metrics ? metrics.enriched_per_s.toFixed(1) : "—"}
            hint="Enriched posts per second across all NLP workers"
          />
          <Stat
            label="Freshness"
            value={formatSeconds(freshnessS)}
            hint="Now minus the ingest time of the newest post in the last update"
          />
          <Stat
            label="Ingest p95"
            value={metrics ? `${Math.round(metrics.ingest_latency_ms.p95)} ms` : "—"}
            hint="Kafka produce -> enriched record (p95)"
          />
          <Stat label="In window" value={formatNumber(postsInWindow)} hint="Posts in the current 5-minute window" />
        </div>

        <button
          type="button"
          onClick={onToggleTheme}
          className="ml-auto rounded-md border border-zinc-200 px-2.5 py-1.5 text-sm hover:bg-zinc-100 sm:ml-0 dark:border-zinc-700 dark:hover:bg-zinc-800"
          aria-label="Toggle dark mode"
        >
          {dark ? "☀︎" : "☾"}
        </button>
      </div>
    </header>
  );
}
