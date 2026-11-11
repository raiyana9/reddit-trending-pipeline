import { categoryHue, formatNumber, sentimentColor } from "../lib/format";
import type { CategoryStat } from "../types";

interface Props {
  stats: CategoryStat[];
  selected: string | null;
  onSelect: (category: string | null) => void;
}

export function CategoryGrid({ stats, selected, onSelect }: Props) {
  const max = Math.max(1, ...stats.map((s) => s.count_window));
  return (
    <div className="grid grid-cols-3 gap-2 sm:grid-cols-5" role="list" aria-label="Categories">
      {stats.map((s) => {
        const active = selected === s.category;
        const h = categoryHue(s.category);
        return (
          <button
            key={s.category}
            type="button"
            role="listitem"
            onClick={() => onSelect(active ? null : s.category)}
            aria-pressed={active}
            className={`group relative overflow-hidden rounded-lg border p-2 text-left transition ${
              active
                ? "border-sky-400 ring-2 ring-sky-400/40 dark:border-sky-500"
                : "border-zinc-200 hover:border-zinc-300 dark:border-zinc-800 dark:hover:border-zinc-700"
            } ${selected && !active ? "opacity-50" : ""}`}
            style={{ backgroundColor: sentimentColor(s.avg_sentiment, 0.12) }}
            title={`${s.category}: ${s.count_window} posts, avg sentiment ${s.avg_sentiment.toFixed(2)}`}
          >
            <div
              className="absolute inset-x-0 bottom-0 h-1"
              style={{ width: `${(s.count_window / max) * 100}%`, backgroundColor: `hsl(${h}, 65%, 52%)` }}
            />
            <div className="truncate text-[11px] font-medium text-zinc-600 dark:text-zinc-300">{s.category}</div>
            <div className="flex items-baseline justify-between gap-1">
              <span className="font-mono text-base font-bold tabular-nums">{formatNumber(s.count_window)}</span>
              <span className="font-mono text-[11px] tabular-nums" style={{ color: sentimentColor(s.avg_sentiment) }}>
                {s.avg_sentiment >= 0 ? "+" : ""}
                {s.avg_sentiment.toFixed(2)}
              </span>
            </div>
          </button>
        );
      })}
    </div>
  );
}
