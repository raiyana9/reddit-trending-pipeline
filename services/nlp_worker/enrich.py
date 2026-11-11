"""Pure enrichment: RawPost batch -> EnrichedPost batch. No Kafka I/O here."""

from __future__ import annotations

import time
from dataclasses import dataclass

from spacy.language import Language

from services.nlp_worker.category import CategoryClassifier, load_categories
from services.nlp_worker.pipeline import build_nlp
from services.nlp_worker.topics import extract_entities, extract_topics
from shared.config import Settings
from shared.schemas import EnrichedPost, RawPost, sentiment_label

BODY_CHARS = 1_000


def post_text(post: RawPost) -> tuple[str, int]:
    """Text fed to spaCy and the char offset where the title ends."""
    title = post.title.strip()
    body = post.selftext[:BODY_CHARS].strip()
    if not body:
        return title, len(title)
    sep = "" if title.endswith((".", "!", "?")) else "."
    text = f"{title}{sep}\n{body}"
    return text, len(title)


@dataclass
class Enricher:
    nlp: Language
    classifier: CategoryClassifier
    batch_size: int = 32

    @classmethod
    def from_settings(cls, settings: Settings) -> Enricher:
        nlp = build_nlp(settings.spacy_model)
        classifier = CategoryClassifier(nlp, load_categories(settings.categories_path))
        return cls(nlp=nlp, classifier=classifier, batch_size=settings.nlp_batch_size)

    def enrich(self, posts: list[RawPost]) -> list[EnrichedPost | Exception]:
        """Enrich a batch; per-post failures are returned as exceptions (for the DLQ)."""
        if not posts:
            return []
        t0 = time.perf_counter()
        texts = [post_text(p) for p in posts]
        docs = list(self.nlp.pipe((t for t, _ in texts), batch_size=self.batch_size))
        per_post_ms = (time.perf_counter() - t0) * 1000 / len(posts)
        out: list[EnrichedPost | Exception] = []
        for post, doc, (_, title_end) in zip(posts, docs, texts, strict=True):
            try:
                t1 = time.perf_counter()
                pred = self.classifier.predict(doc, post.subreddit, title_end)
                compound = max(-1.0, min(1.0, float(doc._.sentiment)))
                enriched = EnrichedPost.from_raw(
                    post,
                    topics=extract_topics(doc),
                    entities=extract_entities(doc),
                    category=pred.category,
                    category_confidence=pred.confidence,
                    sentiment=round(compound, 4),
                    sentiment_label=sentiment_label(compound),
                    processed_at=0.0,
                    nlp_ms=0.0,
                )
                enriched.nlp_ms = round(per_post_ms + (time.perf_counter() - t1) * 1000, 3)
                enriched.processed_at = time.time()
                out.append(enriched)
            except Exception as exc:  # noqa: BLE001 - surfaced to DLQ by caller
                out.append(exc)
        return out
