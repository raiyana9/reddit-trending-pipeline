export interface TrendItem {
  topic: string;
  category: string;
  score: number;
  count_window: number;
  count_baseline: number;
  velocity: number;
  avg_sentiment: number;
  sample_titles: string[];
  sample_urls: string[];
  subreddit_count: number;
  first_seen: number;
}

export interface CategoryStat {
  category: string;
  count_window: number;
  avg_sentiment: number;
}

export interface TrendUpdate {
  generated_at: number | null;
  window_seconds: number;
  trends: TrendItem[];
  category_stats: CategoryStat[];
  max_source_ingested_at: number | null;
  total_window: number;
}

export interface LivePost {
  id: string;
  subreddit: string;
  title: string;
  url: string;
  category: string;
  sentiment: number;
  sentiment_label: "pos" | "neu" | "neg";
  topics: string[];
  ingested_at: number;
  processed_at: number;
}

export type FrameType = "snapshot" | "update" | "post" | "ping" | "poll";

export interface Frame<T = unknown> {
  type: FrameType;
  server_sent_at: number;
  data?: T;
}

export interface MetricsSummary {
  produced_per_s: number;
  produced_total: number;
  enriched_per_s: number;
  enriched_total: number;
  dlq_total: number;
  nlp_workers: number;
  consumer_lag: number;
  ingest_latency_ms: { p50: number; p95: number; p99: number };
  trend_updates_per_s: number;
  posts_in_window: number;
  ws_clients: number;
  e2e_p95_ms: number | null;
  freshness_s: number | null;
  projected_per_day: number;
}
