import { useCallback, useMemo, useState } from "react";
import { CategoryGrid } from "./components/CategoryGrid";
import { Header } from "./components/Header";
import { LiveTicker } from "./components/LiveTicker";
import { TopicDrawer } from "./components/TopicDrawer";
import { TrendingList } from "./components/TrendingList";
import { API_URL, POSTS_WS_URL, WS_URL } from "./config";
import { useMetrics, useNow, usePostStream, useTrendSocket } from "./hooks/useLive";

function useTheme() {
  const [dark, setDark] = useState(() => document.documentElement.classList.contains("dark"));
  const toggle = useCallback(() => {
    setDark((d) => {
      const next = !d;
      document.documentElement.classList.toggle("dark", next);
      try {
        localStorage.setItem("theme", next ? "dark" : "light");
      } catch {
        /* storage unavailable */
      }
      return next;
    });
  }, []);
  return { dark, toggle };
}

export default function App() {
  const { state, status } = useTrendSocket(WS_URL, `${API_URL}/api/trends`);
  const posts = usePostStream(POSTS_WS_URL);
  const metrics = useMetrics(API_URL);
  const now = useNow();
  const { dark, toggle } = useTheme();
  const [category, setCategory] = useState<string | null>(null);
  const [selected, setSelected] = useState<string | null>(null);

  const rows = useMemo(
    () => (category ? state.rows.filter((r) => r.category === category) : state.rows),
    [state.rows, category],
  );
  // Re-rank within the filtered view so positions are contiguous.
  const viewRows = useMemo(
    () => rows.map((r, i) => (category ? { ...r, rank: i + 1 } : r)),
    [rows, category],
  );
  const selectedTrend = state.rows.find((r) => r.topic === selected) ?? null;
  const freshness = state.update?.max_source_ingested_at != null ? now - state.update.max_source_ingested_at : null;
  const closeDrawer = useCallback(() => setSelected(null), []);

  return (
    <div className="min-h-screen">
      <Header
        status={status}
        freshnessS={freshness ?? state.freshnessS}
        metrics={metrics}
        postsInWindow={state.update?.total_window ?? 0}
        dark={dark}
        onToggleTheme={toggle}
      />
      <main className="mx-auto grid max-w-7xl gap-6 px-4 py-5 lg:grid-cols-[1fr_340px]">
        <div className="min-w-0 space-y-5">
          <section>
            <div className="mb-2 flex items-baseline justify-between">
              <h2 className="text-sm font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
                Categories <span className="normal-case tracking-normal">· last {Math.round((state.update?.window_seconds ?? 300) / 60)} min</span>
              </h2>
              {category && (
                <button onClick={() => setCategory(null)} className="text-xs text-sky-600 hover:underline dark:text-sky-400">
                  Clear filter
                </button>
              )}
            </div>
            <CategoryGrid stats={state.update?.category_stats ?? []} selected={category} onSelect={setCategory} />
          </section>
          <section>
            <h2 className="mb-2 text-sm font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400">
              Trending {category ? `in ${category}` : "now"}
            </h2>
            <TrendingList rows={viewRows} history={state.history} selected={selected} onSelect={setSelected} />
          </section>
        </div>
        <aside className="min-w-0">
          <LiveTicker posts={posts} now={now} />
        </aside>
      </main>
      <TopicDrawer trend={selectedTrend} history={selected ? state.history[selected] ?? [] : []} onClose={closeDrawer} now={now} />
    </div>
  );
}
