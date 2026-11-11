/** Sentiment in [-1, 1] -> red (neg) -> gray (neutral) -> green (pos). */
export function sentimentColor(s: number, alpha = 1): string {
  const v = Math.max(-1, Math.min(1, s));
  const gray: [number, number, number] = [113, 113, 122];
  const end: [number, number, number] = v >= 0 ? [22, 163, 74] : [220, 38, 38];
  const k = Math.min(1, Math.abs(v) / 0.6);
  const [r, g, b] = gray.map((c, i) => Math.round(c + (end[i] - c) * k));
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

export function sentimentLabel(s: number): "pos" | "neu" | "neg" {
  if (s >= 0.05) return "pos";
  if (s <= -0.05) return "neg";
  return "neu";
}

export function formatNumber(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 10_000) return `${Math.round(n / 1000)}k`;
  if (n >= 1_000) return `${(n / 1000).toFixed(1)}k`;
  return `${Math.round(n)}`;
}

export function formatSeconds(s: number | null | undefined): string {
  if (s == null || !Number.isFinite(s)) return "—";
  if (s < 1) return `${Math.round(s * 1000)} ms`;
  if (s < 60) return `${s.toFixed(1)} s`;
  return `${Math.round(s / 60)} min`;
}

export function timeAgo(epochS: number, nowS = Date.now() / 1000): string {
  const d = Math.max(0, nowS - epochS);
  if (d < 60) return `${Math.round(d)}s ago`;
  if (d < 3600) return `${Math.round(d / 60)}m ago`;
  return `${Math.round(d / 3600)}h ago`;
}

const CATEGORY_HUES: Record<string, number> = {
  Technology: 210,
  "AI/ML": 265,
  Gaming: 290,
  Sports: 25,
  Politics: 350,
  "World News": 190,
  "Finance/Crypto": 140,
  Science: 175,
  Health: 330,
  Entertainment: 45,
  Music: 310,
  "Business/Startups": 230,
  "Education/Careers": 60,
  Lifestyle: 95,
  "Memes/Humor": 15,
};

export function categoryHue(category: string): number {
  return CATEGORY_HUES[category] ?? 220;
}
