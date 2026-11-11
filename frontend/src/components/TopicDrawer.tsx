import { useEffect } from "react";
import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { timeAgo } from "../lib/format";
import type { HistoryPoint, RankedTrend } from "../lib/trends";
import { CategoryTag, SentimentChip } from "./Bits";

interface Props {
  trend: RankedTrend | null;
  history: HistoryPoint[];
  onClose: () => void;
  now: number;
}

const tick = (t: number) => new Date(t * 1000).toLocaleTimeString([], { minute: "2-digit", second: "2-digit" });

export function TopicDrawer({ trend, history, onClose, now }: Props) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);

  const open = trend !== null;
  return (
    <>
      <div
        className={`fixed inset-0 z-40 bg-black/30 transition-opacity ${open ? "opacity-100" : "pointer-events-none opacity-0"}`}
        onClick={onClose}
      />
      <aside
        className={`fixed inset-y-0 right-0 z-50 w-full max-w-md transform overflow-y-auto border-l border-zinc-200 bg-white p-5 shadow-2xl transition-transform duration-300 dark:border-zinc-800 dark:bg-zinc-950 ${
          open ? "translate-x-0" : "translate-x-full"
        }`}
        aria-hidden={!open}
        aria-label="Topic details"
      >
        {trend && (
          <div className="space-y-5">
            <div className="flex items-start justify-between gap-3">
              <div>
                <div className="text-xs text-zinc-500">#{trend.rank} trending</div>
                <h2 className="text-xl font-bold">{trend.topic}</h2>
                <div className="mt-1 flex flex-wrap items-center gap-2">
                  <CategoryTag category={trend.category} />
                  <SentimentChip value={trend.avg_sentiment} />
                  <span className="text-xs text-zinc-500">first seen {timeAgo(trend.first_seen, now)}</span>
                </div>
              </div>
              <button onClick={onClose} className="rounded p-1 text-zinc-500 hover:bg-zinc-100 dark:hover:bg-zinc-800" aria-label="Close">
                ✕
              </button>
            </div>

            <dl className="grid grid-cols-4 gap-2 text-center">
              {[
                ["Score", trend.score.toFixed(1)],
                ["Window", String(trend.count_window)],
                ["Expected", trend.count_baseline.toFixed(1)],
                ["Subs", String(trend.subreddit_count)],
              ].map(([k, v]) => (
                <div key={k} className="rounded-lg bg-zinc-100 p-2 dark:bg-zinc-900">
                  <dt className="text-[10px] uppercase tracking-wider text-zinc-500">{k}</dt>
                  <dd className="font-mono font-semibold tabular-nums">{v}</dd>
                </div>
              ))}
            </dl>

            <section>
              <h3 className="mb-1 text-sm font-semibold">Mentions in window</h3>
              <div className="h-36">
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={history} margin={{ left: -20, right: 4, top: 4 }}>
                    <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />
                    <XAxis dataKey="t" tickFormatter={tick} fontSize={10} minTickGap={30} />
                    <YAxis fontSize={10} allowDecimals={false} />
                    <Tooltip labelFormatter={(t) => tick(Number(t))} />
                    <Area type="monotone" dataKey="count" stroke="#0ea5e9" fill="#0ea5e9" fillOpacity={0.2} isAnimationActive={false} />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </section>

            <section>
              <h3 className="mb-1 text-sm font-semibold">Sentiment over time</h3>
              <div className="h-28">
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={history} margin={{ left: -20, right: 4, top: 4 }}>
                    <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.2} />
                    <XAxis dataKey="t" tickFormatter={tick} fontSize={10} minTickGap={30} />
                    <YAxis domain={[-1, 1]} fontSize={10} />
                    <ReferenceLine y={0} stroke="#71717a" strokeDasharray="4 4" />
                    <Tooltip labelFormatter={(t) => tick(Number(t))} formatter={(v: number) => v.toFixed(2)} />
                    <Line type="monotone" dataKey="sentiment" stroke="#a855f7" dot={false} isAnimationActive={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            </section>

            <section>
              <h3 className="mb-2 text-sm font-semibold">Recent posts</h3>
              <ul className="space-y-2">
                {trend.sample_titles.map((title, i) => (
                  <li key={i}>
                    <a
                      href={trend.sample_urls[i] || "#"}
                      target="_blank"
                      rel="noreferrer noopener"
                      className="block rounded-lg border border-zinc-200 p-2.5 text-sm hover:border-sky-400 hover:text-sky-600 dark:border-zinc-800 dark:hover:text-sky-400"
                    >
                      {title}
                    </a>
                  </li>
                ))}
              </ul>
            </section>
          </div>
        )}
      </aside>
    </>
  );
}
