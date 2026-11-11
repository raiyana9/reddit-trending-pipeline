import { Line, LineChart, YAxis } from "recharts";
import { categoryHue, sentimentColor, sentimentLabel } from "../lib/format";
import type { HistoryPoint } from "../lib/trends";

export function CategoryTag({ category }: { category: string }) {
  const h = categoryHue(category);
  return (
    <span
      className="inline-block whitespace-nowrap rounded px-1.5 py-0.5 text-[11px] font-medium text-[hsl(var(--h)_60%_40%)] dark:text-[hsl(var(--h)_80%_72%)]"
      style={{ backgroundColor: `hsla(${h}, 70%, 50%, 0.16)`, ["--h" as string]: h }}
    >
      {category}
    </span>
  );
}

export function SentimentChip({ value }: { value: number }) {
  const label = sentimentLabel(value);
  return (
    <span
      className="inline-flex items-center gap-1 rounded-full px-2 py-0.5 font-mono text-[11px] tabular-nums"
      style={{ backgroundColor: sentimentColor(value, 0.15), color: sentimentColor(value) }}
      title={`Average sentiment ${value.toFixed(2)}`}
    >
      {label === "pos" ? "▲" : label === "neg" ? "▼" : "●"} {value >= 0 ? "+" : ""}
      {value.toFixed(2)}
    </span>
  );
}

export function RankChange({ change, isNew }: { change: number; isNew: boolean }) {
  if (isNew) return <span className="text-[11px] font-semibold text-sky-500">NEW</span>;
  if (change > 0) return <span className="font-mono text-xs text-emerald-500">▲{change}</span>;
  if (change < 0) return <span className="font-mono text-xs text-red-500">▼{-change}</span>;
  return <span className="font-mono text-xs text-zinc-400">–</span>;
}

export function Sparkline({ points, width = 84, height = 26 }: { points: HistoryPoint[]; width?: number; height?: number }) {
  if (points.length < 2) return <div style={{ width, height }} />;
  const rising = points[points.length - 1].score >= points[0].score;
  return (
    <LineChart width={width} height={height} data={points} margin={{ top: 2, bottom: 2, left: 0, right: 0 }}>
      <YAxis hide domain={["dataMin", "dataMax"]} />
      <Line
        type="monotone"
        dataKey="score"
        dot={false}
        isAnimationActive={false}
        strokeWidth={1.75}
        stroke={rising ? "#10b981" : "#f43f5e"}
      />
    </LineChart>
  );
}
