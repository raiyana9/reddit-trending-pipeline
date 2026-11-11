"""Keyphrase + entity extraction (pure functions over a spaCy ``Doc``)."""

from __future__ import annotations

import re

from spacy.tokens import Doc, Span, Token

from shared.schemas import ALLOWED_ENTITY_LABELS, MAX_TOPICS, Entity

# Reddit / conversational noise that should never be a "topic" on its own.
NOISE: frozenset[str] = frozenset({
    "edit", "edit:", "tl;dr", "tldr", "op", "anyone", "anybody", "everyone", "everybody",
    "someone", "somebody", "nobody", "thread", "post", "reddit", "subreddit", "sub", "question",
    "thought", "thoughts", "people", "person", "thing", "things", "lot", "lots", "way", "time",
    "day", "today", "tonight", "lol", "lmao", "guy", "guys", "summary", "news", "lurker",
    "context", "detail", "details", "chatter", "chat", "angle", "side", "week", "year",
    "morning", "all", "help", "advice", "update", "opinion", "unpopular opinion", "breaking",
    "eli5", "psa", "til", "imo", "imho", "fyi", "deleted", "removed", "bit", "kind", "sort",
    "long time lurker", "what", "who", "this", "that", "one", "the news",
})
# Noun-chunk heads that carry opinion/structure, not subject matter ("worst decision", "a mess").
GENERIC_HEADS: frozenset[str] = frozenset({
    "decision", "disaster", "idea", "thing", "stuff", "problem", "issue", "point", "reason", "part",
    "place", "deal", "mess", "fact", "case", "example", "story", "result", "look", "job", "work",
    "news", "opinion", "question", "answer", "summary", "detail", "minute", "hour", "life",
    "word", "start", "end", "kind", "type", "number", "amount", "level", "side", "rest", "share",
})
CONTENT_POS = {"NOUN", "PROPN"}
EDGE_POS = {"DET", "PRON", "PUNCT", "CCONJ", "ADP", "PART", "SCONJ", "AUX", "SYM", "INTJ"}
_NUMERIC = re.compile(r"^[\d\s.,:%$#+\-/]+$")
MAX_PHRASE_TOKENS = 5
# spaCy's lemmatizer produces some lemmas no Reddit user would write.
LEMMA_OVERRIDES = {"datum": "data", "medium": "media", "news": "news", "series": "series"}
_REDDIT_REF = re.compile(r"(^|\W)/?[ru]\s*/\s*\w+", re.IGNORECASE)


def _is_edge_junk(tok: Token) -> bool:
    return tok.is_stop or tok.is_punct or tok.is_space or tok.pos_ in EDGE_POS or \
        tok.lower_ in NOISE or tok.lemma_.lower() in NOISE


def _trim(span: Span) -> Span | None:
    start, end = span.start, span.end
    doc = span.doc
    while start < end and _is_edge_junk(doc[start]):
        start += 1
    while end > start and _is_edge_junk(doc[end - 1]):
        end -= 1
    return doc[start:end] if end > start else None


def _lemma(tok: Token) -> str:
    lemma = tok.lemma_.lower()
    return LEMMA_OVERRIDES.get(lemma, lemma)


def _render(span: Span, lemmatize: bool) -> str:
    if lemmatize:
        text = "".join(_lemma(t) + t.whitespace_ for t in span)
    else:
        text = span.text.lower()
    return re.sub(r"\s+", " ", text).strip(" -–—'\".,:;!?")


def normalize_phrase(span: Span, lemmatize: bool = True, is_entity: bool = False) -> str | None:
    """Trim junk at the edges, lemmatize + lowercase, and apply quality filters.

    Noun chunks must contain a real noun and must not be headed by a generic/opinion noun unless
    they include a proper noun ("worst decision" is dropped, "Fed decision" is kept).
    """
    trimmed = _trim(span)
    if trimmed is None or len(trimmed) > MAX_PHRASE_TOKENS:
        return None
    if not is_entity:
        if not any(t.pos_ in CONTENT_POS for t in trimmed):
            return None
        has_propn = any(t.pos_ == "PROPN" for t in trimmed)
        if trimmed.root.lemma_.lower() in GENERIC_HEADS and not has_propn:
            return None
    if _REDDIT_REF.search(span.text) or any(t.like_url or t.like_email for t in trimmed):
        return None
    text = _render(trimmed, lemmatize)
    if len(text) < 3 or _NUMERIC.match(text) or text in NOISE:
        return None
    if all(t.is_stop or t.lower_ in NOISE for t in trimmed):
        return None
    return text


def extract_entities(doc: Doc) -> list[Entity]:
    seen: set[tuple[str, str]] = set()
    out: list[Entity] = []
    for ent in doc.ents:
        if ent.label_ not in ALLOWED_ENTITY_LABELS:
            continue
        text = ent.text.strip()
        key = (text.lower(), ent.label_)
        if len(text) < 2 or key in seen:
            continue
        seen.add(key)
        out.append(Entity(text=text, label=ent.label_))  # type: ignore[arg-type]
    return out


def extract_topics(doc: Doc, max_topics: int = MAX_TOPICS) -> list[str]:
    """Candidates = filtered named entities + noun chunks, normalized and deduped.

    Ranking: entities before noun chunks, then longer phrases first, then first occurrence.
    Entities keep their surface form (lowercased) so names like "kansas city chiefs" aren't
    mangled by lemmatization; noun chunks are lemmatized.
    """
    candidates: list[tuple[int, int, int, str]] = []  # (kind_rank, -n_tokens, position, text)
    for ent in doc.ents:
        if ent.label_ in ALLOWED_ENTITY_LABELS:
            text = normalize_phrase(ent, lemmatize=False, is_entity=True)
            if text:
                candidates.append((0, -len(text.split()), ent.start, text))
    for chunk in doc.noun_chunks:
        text = normalize_phrase(chunk, lemmatize=True)
        if text:
            candidates.append((1, -len(text.split()), chunk.start, text))

    candidates.sort()
    topics: list[str] = []
    seen: set[str] = set()
    for _, _, _, text in candidates:
        if text in seen:
            continue
        seen.add(text)
        topics.append(text)
        if len(topics) >= max_topics:
            break
    return topics
