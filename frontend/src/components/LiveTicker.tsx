import { sentimentColor, timeAgo } from "../lib/format";
import type { LivePost } from "../types";
import { CategoryTag } from "./Bits";

export function LiveTicker({ posts, now }: { posts: LivePost[]; now: number }) {
  return (
    <div className="rounded-xl border border-zinc-200 bg-white dark:border-zinc-800 dark:bg-zinc-900/60">
      <div className="flex items-center justify-between border-b border-zinc-200 px-3 py-2 dark:border-zinc-800">
        <h2 className="text-sm font-semibold">Live posts</h2>
        <span className="text-xs text-zinc-500">{posts.length ? "streaming" : "waiting…"}</span>
      </div>
      <ul className="max-h-[420px] divide-y divide-zinc-100 overflow-y-auto dark:divide-zinc-800">
        {posts.map((p) => (
          <li key={p.id} className="animate-[slidein_0.4s_ease-out] px-3 py-2">
            <a href={p.url} target="_blank" rel="noreferrer noopener" className="line-clamp-2 text-sm hover:text-sky-600 dark:hover:text-sky-400">
              {p.title}
            </a>
            <div className="mt-1 flex items-center gap-2 text-[11px] text-zinc-500">
              <span
                className="inline-block h-2 w-2 rounded-full"
                style={{ backgroundColor: sentimentColor(p.sentiment) }}
                title={`sentiment ${p.sentiment.toFixed(2)}`}
              />
              <span>r/{p.subreddit}</span>
              <CategoryTag category={p.category} />
              <span className="ml-auto font-mono">{timeAgo(p.ingested_at, now)}</span>
            </div>
          </li>
        ))}
      </ul>
    </div>
  );
}
