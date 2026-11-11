import type { TrendItem, TrendUpdate } from "../types";

export interface HistoryPoint {
  t: number; // generated_at (epoch s)
  score: number;
  count: number;
  sentiment: number;
}

export interface RankedTrend extends TrendItem {
  rank: number;
  prevRank: number | null;
  /** Positive = moved up. 0 = unchanged or new. */
  change: number;
  isNew: boolean;
}

export interface TrendState {
  update: TrendUpdate | null;
  rows: RankedTrend[];
  history: Record<string, HistoryPoint[]>;
  updatesReceived: number;
  /** Seconds between the newest contributing post's ingest and receipt in the browser. */
  freshnessS: number | null;
  receivedAt: number | null;
}

export const initialTrendState: TrendState = {
  update: null,
  rows: [],
  history: {},
  updatesReceived: 0,
  freshnessS: null,
  receivedAt: null,
};

export const MAX_HISTORY = 60;

/** Rank every trend and compare with the previous update's ranks. */
export function diffTrends(prev: RankedTrend[], next: TrendItem[], hadPrevious: boolean): RankedTrend[] {
  const prevRanks = new Map(prev.map((r) => [r.topic, r.rank]));
  return next.map((t, i) => {
    const rank = i + 1;
    const prevRank = prevRanks.get(t.topic) ?? null;
    return {
      ...t,
      rank,
      prevRank,
      change: prevRank === null ? 0 : prevRank - rank,
      isNew: hadPrevious && prevRank === null,
    };
  });
}

/**
 * Fold a TrendUpdate into state. Updates that are not newer than the current one
 * (e.g. a poll racing a socket push) are ignored. History is kept for topics still in the
 * update, capped at ``maxHistory`` points each, so memory stays bounded.
 */
export function applyUpdate(
  state: TrendState,
  update: TrendUpdate,
  receivedAtS: number,
  maxHistory = MAX_HISTORY,
): TrendState {
  if (update.generated_at === null) {
    return state.update ? state : { ...state, update, receivedAt: receivedAtS };
  }
  if (state.update?.generated_at != null && update.generated_at <= state.update.generated_at) {
    return state;
  }
  const rows = diffTrends(state.rows, update.trends, state.update?.generated_at != null);
  const history: Record<string, HistoryPoint[]> = {};
  for (const t of update.trends) {
    const prev = state.history[t.topic] ?? [];
    const point = { t: update.generated_at, score: t.score, count: t.count_window, sentiment: t.avg_sentiment };
    history[t.topic] = [...prev, point].slice(-maxHistory);
  }
  return {
    update,
    rows,
    history,
    updatesReceived: state.updatesReceived + 1,
    freshnessS: update.max_source_ingested_at != null ? receivedAtS - update.max_source_ingested_at : null,
    receivedAt: receivedAtS,
  };
}
