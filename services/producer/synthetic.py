"""Synthetic Reddit-like post generator.

Produces plausible posts from per-category templates with Zipf-distributed topic popularity,
plus an ``inject`` mode that floods a chosen phrase to create a deliberate trend spike
(used by the freshness benchmark).
"""

from __future__ import annotations

import random
import string
import time
from dataclasses import dataclass, field
from functools import lru_cache

import yaml

from shared.config import get_settings
from shared.schemas import RawPost

# category -> (topics, events)
CATALOG: dict[str, tuple[list[str], list[str]]] = {
    "Technology": (
        ["Apple Vision Pro", "the Pixel 10", "Windows 12", "the Linux kernel", "USB-C chargers",
         "Starlink", "the Framework laptop", "Raspberry Pi 6"],
        ["just got a major software update", "has a serious security flaw",
         "battery life tested for a week", "price leaked ahead of launch", "announced at the keynote"],
    ),
    "AI/ML": (
        ["GPT-6", "Claude", "Gemini Ultra", "Llama 5", "Stable Diffusion", "AI agents",
         "open-weight models", "Mistral"],
        ["tops the new benchmark", "released with a huge context window",
         "fine-tuning is now open to everyone", "inference costs drop again", "sparks a new AGI debate"],
    ),
    "Gaming": (
        ["GTA 6", "the Switch 2", "Hollow Knight Silksong", "the Steam Deck 2", "Baldur's Gate 3",
         "Minecraft", "the PS6", "Elden Ring"],
        ["finally gets a release date", "patch notes are out", "multiplayer servers are down",
         "new DLC announced", "speedrun record broken"],
    ),
    "Sports": (
        ["LeBron James", "the Lakers", "Real Madrid", "the Kansas City Chiefs", "Max Verstappen",
         "Lionel Messi", "the Champions League", "the World Cup"],
        ["wins in overtime", "trade rumors heat up", "injury update after the match",
         "clinches the playoff spot", "coach fired after a bad season"],
    ),
    "Politics": (
        ["the Senate", "the Supreme Court", "the infrastructure bill", "the governor race",
         "the midterm elections", "the White House", "the filibuster", "the new executive order"],
        ["vote scheduled for Tuesday", "faces a new poll slump", "campaign ad triggers backlash",
         "passes along party lines", "blocked by lawmakers"],
    ),
    "World News": (
        ["Ukraine", "the United Nations", "NATO", "Gaza", "Taiwan", "the European Union", "Japan",
         "Brazil"],
        ["ceasefire talks resume", "new sanctions announced", "protests spread across the capital",
         "earthquake hits the coast", "prime minister resigns"],
    ),
    "Finance/Crypto": (
        ["Bitcoin", "Nvidia stock", "Ethereum", "the Fed", "Tesla shares", "the S&P 500",
         "GameStop", "index funds"],
        ["hits an all-time high", "crashes after earnings", "interest rate decision looms",
         "short squeeze incoming", "inflation report surprises the market"],
    ),
    "Science": (
        ["the James Webb telescope", "NASA", "SpaceX Starship", "CRISPR", "a new exoplanet",
         "black holes", "the Mars rover", "quantum computing"],
        ["discovery stuns scientists", "new study published in Nature", "rocket launch succeeds",
         "experiment confirms the theory", "fossil found in Antarctica"],
    ),
    "Health": (
        ["Ozempic", "the flu vaccine", "intermittent fasting", "creatine", "sleep apnea",
         "the measles outbreak", "the FDA", "mental health apps"],
        ["new study links it to longer life", "doctors issue a warning", "FDA approval granted",
         "hospital cases rise", "workout results after 90 days"],
    ),
    "Entertainment": (
        ["The Last of Us", "Dune Part Three", "the new Batman movie", "Stranger Things",
         "Marvel Studios", "Netflix", "Severance", "the Oscars"],
        ["trailer just dropped", "season finale breaks records", "box office opening weekend",
         "sequel confirmed", "cast announced"],
    ),
    "Music": (
        ["Taylor Swift", "Kendrick Lamar", "Radiohead", "the Grammys", "Billie Eilish",
         "Coachella", "Spotify Wrapped", "Daft Punk"],
        ["surprise album drops tonight", "tour dates announced", "new single leaks early",
         "wins album of the year", "live performance goes viral"],
    ),
    "Business/Startups": (
        ["Stripe", "OpenAI's funding round", "Y Combinator", "Shopify", "Amazon layoffs",
         "the Figma acquisition", "Rivian", "small business loans"],
        ["raises a Series B", "announces more layoffs", "IPO filing revealed", "revenue doubles",
         "CEO steps down"],
    ),
    "Education/Careers": (
        ["the tech job market", "FAANG interviews", "student loan forgiveness", "remote work",
         "grad school applications", "LinkedIn", "the PhD stipend", "entry level jobs"],
        ["hiring freeze continues", "salary data released", "finally got a job offer",
         "professor rejects my thesis", "internship season opens"],
    ),
    "Lifestyle": (
        ["sourdough bread", "Japan travel", "IKEA furniture", "cold brew coffee",
         "minimalist apartments", "budget weddings", "capsule wardrobes", "houseplants"],
        ["recipe that changed my life", "tips after six months", "renovation before and after",
         "cheapest way to do it", "mistakes to avoid"],
    ),
    "Memes/Humor": (
        ["Monday mornings", "cats", "the printer", "group projects", "my code", "dad jokes",
         "the WiFi", "adulting"],
        ["meme of the year", "me irl every single time", "nobody: absolutely nobody:",
         "starter pack", "so relatable lol"],
    ),
}

TITLE_TEMPLATES = [
    "{t} {e}",
    "Thoughts on {t}? {e}",
    "I can't believe {t} {e}",
    "Breaking: {t} {e}",
    "Why is nobody talking about {t}?",
    "{t} — {o}",
    "Just read that {t} {e}. {o}",
    "Unpopular opinion: {t} is {a}",
]

OPINIONS = {
    "pos": ["honestly amazing", "love it, great job", "best news this year", "so impressed",
            "this is fantastic"],
    "neg": ["absolutely terrible", "worst decision ever", "I hate this", "what a disaster",
            "this is awful"],
    "neu": ["not sure what to think", "here's the summary", "details below",
            "what do you all think", "curious how this plays out"],
}
ADJECTIVES = {"pos": ["underrated", "brilliant", "great"], "neg": ["overrated", "broken", "awful"],
              "neu": ["complicated", "different", "changing"]}

BODY_TEMPLATES = [
    "Saw the news about {t} this morning. {O}. There's a lot of {k1} and {k2} chatter in r/{s}.",
    "{O}. Long time lurker here — the {k1} angle matters more than people think, and the {k2} side too.",
    "Context: {t} {e}. {O}. Would love to hear from people who know about {k1}.",
    "",
]


@lru_cache
def load_taxonomy() -> dict[str, dict]:
    with open(get_settings().categories_path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return {c["name"]: c for c in data["categories"]}


def _zipf_weights(n: int, s: float = 1.1) -> list[float]:
    return [1 / (i + 1) ** s for i in range(n)]


def _post_id(rng: random.Random) -> str:
    return "".join(rng.choices(string.ascii_lowercase + string.digits, k=7))


@dataclass
class SyntheticGenerator:
    seed: int | None = None
    rng: random.Random = field(init=False)

    def __post_init__(self) -> None:
        self.rng = random.Random(self.seed)
        self.taxonomy = load_taxonomy()
        self.categories = list(CATALOG)
        self.cat_weights = _zipf_weights(len(self.categories), 0.6)
        self.rng.shuffle(self.cat_weights)

    def _compose(self, category: str, topic: str, event: str | None = None) -> tuple[str, str]:
        rng = self.rng
        tax = self.taxonomy[category]
        mood = rng.choices(["pos", "neg", "neu"], weights=[0.35, 0.3, 0.35])[0]
        event = event or rng.choice(CATALOG[category][1])
        fill = {
            "t": topic,
            "e": event,
            "o": rng.choice(OPINIONS[mood]),
            "a": rng.choice(ADJECTIVES[mood]),
        }
        title = rng.choice(TITLE_TEMPLATES).format(**fill)
        title = title[0].upper() + title[1:]
        kws = tax["keywords"]
        body = rng.choice(BODY_TEMPLATES).format(
            t=topic, e=event, O=fill["o"].capitalize(), k1=rng.choice(kws), k2=rng.choice(kws),
            s=rng.choice(tax["subreddits"]),
        )
        return title, body

    def _raw(self, category: str, title: str, body: str, now: float, source: str) -> RawPost:
        rng = self.rng
        sub = rng.choice(self.taxonomy[category]["subreddits"])
        pid = _post_id(rng)
        return RawPost(
            id=pid,
            subreddit=sub,
            title=title,
            selftext=body,
            author=f"user_{rng.randint(1, 50_000)}",
            score=int(rng.paretovariate(1.2)) - 1,
            num_comments=int(rng.paretovariate(1.5)) - 1,
            url=f"https://www.reddit.com/r/{sub}/comments/{pid}/",
            created_utc=now,
            ingested_at=now,
            source=source,  # type: ignore[arg-type]
        )

    def make_post(self, now: float | None = None, source: str = "synthetic") -> RawPost:
        now = time.time() if now is None else now
        category = self.rng.choices(self.categories, weights=self.cat_weights)[0]
        topics = CATALOG[category][0]
        topic = self.rng.choices(topics, weights=_zipf_weights(len(topics)))[0]
        title, body = self._compose(category, topic)
        return self._raw(category, title, body, now, source)

    def make_injected(
        self, phrase: str, category: str = "Technology", now: float | None = None,
        source: str = "synthetic",
    ) -> RawPost:
        """A post about ``phrase`` — used to create a deliberate trend spike."""
        now = time.time() if now is None else now
        title, body = self._compose(category, phrase)
        return self._raw(category, title, body, now, source)

    def timeline(
        self, n: int, span_s: float, start: float, stories: list[tuple[str, str, float, float, int]]
    ) -> list[RawPost]:
        """Generate ``n`` background posts across ``span_s`` seconds plus burst ``stories``.

        Each story is ``(phrase, category, start_offset_s, duration_s, count)``.
        Returned posts are sorted by ``created_utc``.
        """
        posts = [self.make_post(start + self.rng.uniform(0, span_s)) for _ in range(n)]
        for phrase, category, offset, duration, count in stories:
            for _ in range(count):
                t = start + offset + self.rng.uniform(0, duration)
                posts.append(self.make_injected(phrase, category, t))
        posts.sort(key=lambda p: p.created_utc)
        return posts
