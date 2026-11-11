"""15-way category classifier.

score[c] = W_SUBREDDIT * prior(subreddit -> c)
         + W_KEYWORD   * sum(keyword matches for c; title matches count TITLE_BOOST x)
         + W_ENTITY    * sum(entities whose label is a hint for c)

Keyword matching uses two spaCy ``PhraseMatcher`` passes: one on the lowercased surface form and
one on a lemma-lowercased copy of the doc, so "Bitcoin", "bitcoins" and "BITCOIN" all match the
seed term "bitcoin". Confidence = softmax over category scores.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path

import yaml
from spacy.language import Language
from spacy.matcher import PhraseMatcher
from spacy.tokens import Doc

W_SUBREDDIT = 5.0
W_KEYWORD = 1.0
W_ENTITY = 0.3
TITLE_BOOST = 1.5
MAX_KEYWORD_SCORE = 6.0
FALLBACK_CATEGORY = "Lifestyle"


@dataclass(frozen=True)
class Prediction:
    category: str
    confidence: float
    scores: dict[str, float]


def load_categories(path: Path) -> list[dict]:
    with path.open(encoding="utf-8") as fh:
        cats = yaml.safe_load(fh)["categories"]
    if len(cats) != 15:
        raise ValueError(f"expected exactly 15 categories, found {len(cats)}")
    return cats


def softmax(scores: dict[str, float], temperature: float = 1.0) -> dict[str, float]:
    m = max(scores.values())
    exps = {k: math.exp((v - m) / temperature) for k, v in scores.items()}
    z = sum(exps.values())
    return {k: v / z for k, v in exps.items()}


def _lemma_doc(doc: Doc) -> Doc:
    words = [t.lemma_.lower() or t.lower_ for t in doc]
    spaces = [bool(t.whitespace_) for t in doc]
    return Doc(doc.vocab, words=words, spaces=spaces)


class CategoryClassifier:
    def __init__(self, nlp: Language, categories: list[dict]) -> None:
        self.names: list[str] = [c["name"] for c in categories]
        self.sub_prior: dict[str, str] = {
            s.lower(): c["name"] for c in categories for s in c.get("subreddits", [])
        }
        self.entity_hints: dict[str, list[str]] = {}
        for c in categories:
            for label in c.get("entity_hints", []) or []:
                self.entity_hints.setdefault(label, []).append(c["name"])

        self.surface = PhraseMatcher(nlp.vocab, attr="LOWER")
        self.lemma = PhraseMatcher(nlp.vocab, attr="ORTH")
        # Patterns are built with the full pipeline so lemmas match what we see at runtime.
        for c in categories:
            terms = [str(t) for t in c.get("keywords", [])]
            docs = list(nlp.pipe(terms, disable=["ner", "parser", "vader_sentiment"]))
            self.surface.add(c["name"], [nlp.make_doc(t.lower()) for t in terms])
            self.lemma.add(c["name"], [_lemma_doc(d) for d in docs])

    def _keyword_scores(self, doc: Doc, title_end_char: int) -> dict[str, float]:
        scores = dict.fromkeys(self.names, 0.0)
        hits: set[tuple[str, int, int]] = set()
        lemma_doc = _lemma_doc(doc)
        for matcher, target in ((self.surface, doc), (self.lemma, lemma_doc)):
            for match_id, start, end in matcher(target):
                name = doc.vocab.strings[match_id]
                if (name, start, end) in hits:
                    continue
                hits.add((name, start, end))
                in_title = doc[start].idx < title_end_char
                scores[name] += TITLE_BOOST if in_title else 1.0
        return {k: min(v, MAX_KEYWORD_SCORE) for k, v in scores.items()}

    def predict(self, doc: Doc, subreddit: str, title_end_char: int | None = None) -> Prediction:
        title_end = len(doc.text) if title_end_char is None else title_end_char
        kw = self._keyword_scores(doc, title_end)
        scores = {name: W_KEYWORD * kw[name] for name in self.names}

        prior = self.sub_prior.get(subreddit.lower())
        if prior:
            scores[prior] += W_SUBREDDIT

        for ent in doc.ents:
            for name in self.entity_hints.get(ent.label_, []):
                scores[name] += W_ENTITY

        if max(scores.values()) <= 0:
            probs = softmax(scores)
            return Prediction(FALLBACK_CATEGORY, probs[FALLBACK_CATEGORY], scores)
        probs = softmax(scores)
        best = max(probs, key=probs.__getitem__)
        return Prediction(best, round(probs[best], 4), scores)
