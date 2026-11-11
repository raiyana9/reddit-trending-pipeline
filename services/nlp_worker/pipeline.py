"""spaCy pipeline construction, including the custom VADER sentiment component."""

from __future__ import annotations

from functools import lru_cache

import spacy
from spacy.language import Language
from spacy.tokens import Doc
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

if not Doc.has_extension("sentiment"):
    Doc.set_extension("sentiment", default=0.0)


@lru_cache(maxsize=1)
def _analyzer() -> SentimentIntensityAnalyzer:
    return SentimentIntensityAnalyzer()


@Language.component("vader_sentiment")
def vader_sentiment(doc: Doc) -> Doc:
    """Set ``doc._.sentiment`` to VADER's compound score in [-1, 1]."""
    doc._.sentiment = float(_analyzer().polarity_scores(doc.text)["compound"])
    return doc


def build_nlp(model: str = "en_core_web_sm") -> Language:
    """Load the model once and append ``vader_sentiment``.

    Everything in en_core_web_sm is needed: tok2vec/tagger/attribute_ruler feed the
    lemmatizer (topic normalization), the parser provides noun chunks, and NER provides entities.
    Only the (already disabled) ``senter`` is excluded.
    """
    nlp = spacy.load(model, exclude=["senter"])
    if "vader_sentiment" not in nlp.pipe_names:
        nlp.add_pipe("vader_sentiment", last=True)
    return nlp
