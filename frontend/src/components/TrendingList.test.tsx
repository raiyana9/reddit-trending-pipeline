import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { diffTrends } from "../lib/trends";
import type { TrendItem } from "../types";
import { CategoryGrid } from "./CategoryGrid";
import { TrendingList } from "./TrendingList";

const item = (topic: string, category = "Gaming"): TrendItem => ({
  topic,
  category,
  score: 5,
  count_window: 12,
  count_baseline: 1,
  velocity: 3,
  avg_sentiment: 0.3,
  sample_titles: [],
  sample_urls: [],
  subreddit_count: 2,
  first_seen: 0,
});

describe("TrendingList", () => {
  it("renders at most 20 rows positioned by rank, with change arrows", () => {
    const prev = diffTrends([], Array.from({ length: 25 }, (_, i) => item(`t${i}`)), false);
    const next = diffTrends(prev, [item("t3"), ...prev.filter((r) => r.topic !== "t3")], true);
    const onSelect = vi.fn();
    render(<TrendingList rows={next} history={{}} selected={null} onSelect={onSelect} />);
    const rows = screen.getAllByTestId("trend-row");
    expect(rows).toHaveLength(20);
    const t3 = rows.find((r) => r.textContent?.includes("t3"))!;
    expect(t3.style.top).toBe("0px");
    expect(t3.textContent).toContain("▲3");
    fireEvent.click(t3.querySelector("button")!);
    expect(onSelect).toHaveBeenCalledWith("t3");
  });

  it("shows an empty state", () => {
    render(<TrendingList rows={[]} history={{}} selected={null} onSelect={() => {}} />);
    expect(screen.getByText(/Waiting for trends/)).toBeInTheDocument();
  });
});

describe("CategoryGrid", () => {
  it("toggles a category filter", () => {
    const onSelect = vi.fn();
    const stats = [
      { category: "Gaming", count_window: 10, avg_sentiment: 0.2 },
      { category: "Sports", count_window: 5, avg_sentiment: -0.4 },
    ];
    const { rerender } = render(<CategoryGrid stats={stats} selected={null} onSelect={onSelect} />);
    fireEvent.click(screen.getByText("Gaming"));
    expect(onSelect).toHaveBeenLastCalledWith("Gaming");
    rerender(<CategoryGrid stats={stats} selected="Gaming" onSelect={onSelect} />);
    fireEvent.click(screen.getByText("Gaming"));
    expect(onSelect).toHaveBeenLastCalledWith(null);
  });
});
