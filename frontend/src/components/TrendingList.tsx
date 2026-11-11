import type { HistoryPoint, RankedTrend } from "../lib/trends";
import { CategoryTag, RankChange, SentimentChip, Sparkline } from "./Bits";

const ROW_H = 60;

interface Props {
  rows: RankedTrend[];
  history: Record<string, HistoryPoint[]>;
  limit?: number;
  selected: string | null;
  onSelect: (topic: string) => void;
}

/**
 * Rows are absolutely positioned at ``rank * ROW_H`` and keyed by topic, so when ranks change
 * React keeps the same DOM node and the CSS ``top`` transition animates the move.
 */
export function TrendingList({ rows, history, limit = 20, selected, onSelect }: Props) {
  const visible = rows.slice(0, limit);
  if (!visible.length) {
    return (
      <div className="flex h-64 items-center justify-center rounded-xl border border-dashed border-zinc-300 text-sm text-zinc-500 dark:border-zinc-700">
        Waiting for trends… start a producer (e.g. <code className="mx-1 font-mono">make demo-produce</code>)
      </div>
    );
  }
  return (
    <ol className="relative" style={{ height: visible.length * ROW_H }} aria-label="Trending topics">
      {visible.map((r) => (
        <li
          key={r.topic}
          className="absolute inset-x-0 px-0.5 transition-[top] duration-700 ease-out"
          style={{ top: (r.rank - 1) * ROW_H, height: ROW_H }}
          data-testid="trend-row"
        >
          <button
            type="button"
            onClick={() => onSelect(r.topic)}
            className={`flex h-[54px] w-full items-center gap-3 rounded-lg border px-3 text-left transition-colors ${
              selected === r.topic
                ? "border-sky-400 bg-sky-50 dark:border-sky-500/60 dark:bg-sky-500/10"
                : "border-zinc-200 bg-white hover:bg-zinc-50 dark:border-zinc-800 dark:bg-zinc-900/60 dark:hover:bg-zinc-800/70"
            } ${r.isNew ? "animate-[flash_1.2s_ease-out]" : ""}`}
          >
            <span className="w-6 shrink-0 text-right font-mono text-sm font-bold tabular-nums text-zinc-400">
              {r.rank}
            </span>
            <span className="w-8 shrink-0 text-center">
              <RankChange change={r.change} isNew={r.isNew} />
            </span>
            <span className="min-w-0 flex-1">
              <span className="block truncate font-semibold">{r.topic}</span>
              <span className="mt-0.5 flex items-center gap-2 text-xs text-zinc-500 dark:text-zinc-400">
                <CategoryTag category={r.category} />
                <span className="font-mono tabular-nums">{r.count_window} posts</span>
                {r.velocity > 0 && <span className="hidden font-mono text-emerald-500 sm:inline">+{r.velocity}/30s</span>}
              </span>
            </span>
            <span className="hidden shrink-0 sm:block">
              <Sparkline points={history[r.topic] ?? []} />
            </span>
            <span className="hidden shrink-0 md:block">
              <SentimentChip value={r.avg_sentiment} />
            </span>
            <span className="w-12 shrink-0 text-right font-mono text-sm font-semibold tabular-nums">
              {r.score.toFixed(1)}
            </span>
          </button>
        </li>
      ))}
    </ol>
  );
}
