import { describe, expect, it } from "vitest";
import type { TrendItem, TrendUpdate } from "../types";
import { applyUpdate, diffTrends, initialTrendState } from "./trends";

const item = (topic: string, score = 1, extra: Partial<TrendItem> = {}): TrendItem => ({
  topic,
  category: "Gaming",
  score,
  count_window: 10,
  count_baseline: 1,
  velocity: 0,
  avg_sentiment: 0,
  sample_titles: [],
  sample_urls: [],
  subreddit_count: 1,
  first_seen: 0,
  ...extra,
});

const update = (t: number, topics: string[], ingested = t - 0.5): TrendUpdate => ({
  generated_at: t,
  window_seconds: 300,
  trends: topics.map((x, i) => item(x, 10 - i)),
  category_stats: [],
  max_source_ingested_at: ingested,
  total_window: 0,
});

describe("diffTrends", () => {
  it("computes rank changes and new entries", () => {
    const first = diffTrends([], [item("a"), item("b"), item("c")], false);
    expect(first.map((r) => [r.topic, r.rank, r.change, r.isNew])).toEqual([
      ["a", 1, 0, false],
      ["b", 2, 0, false],
      ["c", 3, 0, false],
    ]);
    const second = diffTrends(first, [item("c"), item("a"), item("d")], true);
    expect(second.map((r) => [r.topic, r.rank, r.prevRank, r.change, r.isNew])).toEqual([
      ["c", 1, 3, 2, false],
      ["a", 2, 1, -1, false],
      ["d", 3, null, 0, true],
    ]);
  });
});

describe("applyUpdate", () => {
  it("accumulates bounded history and computes freshness", () => {
    let s = initialTrendState;
    for (let t = 1; t <= 10; t++) s = applyUpdate(s, update(t, ["a", "b"]), t + 0.25, 5);
    expect(s.updatesReceived).toBe(10);
    expect(s.history.a).toHaveLength(5);
    expect(s.history.a.map((p) => p.t)).toEqual([6, 7, 8, 9, 10]);
    expect(s.freshnessS).toBeCloseTo(0.75);
  });

  it("ignores stale or duplicate updates (poll racing the socket)", () => {
    let s = applyUpdate(initialTrendState, update(10, ["a"]), 10);
    const same = applyUpdate(s, update(10, ["b"]), 11);
    expect(same).toBe(s);
    const older = applyUpdate(s, update(9, ["b"]), 11);
    expect(older).toBe(s);
    s = applyUpdate(s, update(11, ["b", "a"]), 11);
    expect(s.rows.map((r) => r.topic)).toEqual(["b", "a"]);
    expect(s.rows[0].isNew).toBe(true);
    expect(s.rows[1].change).toBe(-1);
  });

  it("drops history for topics that left the update", () => {
    let s = applyUpdate(initialTrendState, update(1, ["a", "b"]), 1);
    s = applyUpdate(s, update(2, ["b"]), 2);
    expect(Object.keys(s.history)).toEqual(["b"]);
    expect(s.history.b).toHaveLength(2);
  });

  it("accepts an empty initial snapshot without counting it", () => {
    const empty: TrendUpdate = { ...update(0, []), generated_at: null, max_source_ingested_at: null };
    const s = applyUpdate(initialTrendState, empty, 1);
    expect(s.updatesReceived).toBe(0);
    expect(s.update).toBe(empty);
    const next = applyUpdate(s, update(5, ["a"]), 5);
    expect(next.rows[0].isNew).toBe(false);
  });
});
