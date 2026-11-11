import time

import pytest

from services.nlp_worker.category import CategoryClassifier, load_categories, softmax
from services.nlp_worker.enrich import Enricher, post_text
from services.nlp_worker.pipeline import build_nlp
from services.nlp_worker.topics import extract_entities, extract_topics
from services.nlp_worker.worker import BatchBuffer
from shared.config import get_settings
from shared.schemas import EnrichedPost, RawPost


@pytest.fixture(scope="module")
def nlp():
    return build_nlp(get_settings().spacy_model)


@pytest.fixture(scope="module")
def clf(nlp):
    return CategoryClassifier(nlp, load_categories(get_settings().categories_path))


@pytest.fixture(scope="module")
def enricher(nlp, clf):
    return Enricher(nlp=nlp, classifier=clf)


def raw(title, selftext="", subreddit="AskReddit"):
    return RawPost(id="t1", subreddit=subreddit, title=title, selftext=selftext, author="u",
                   created_utc=time.time(), ingested_at=time.time(), source="synthetic")


# ---------------------------------------------------------------- pipeline / sentiment
def test_vader_is_a_pipeline_component(nlp):
    assert nlp.pipe_names[-1] == "vader_sentiment"
    assert {"parser", "ner", "lemmatizer"} <= set(nlp.pipe_names)


@pytest.mark.parametrize("text,sign", [
    ("This is absolutely amazing, I love it!", 1),
    ("This is the worst, most awful update ever. I hate it.", -1),
    ("The meeting is scheduled for Tuesday.", 0),
])
def test_sentiment(nlp, text, sign):
    s = nlp(text)._.sentiment
    assert -1 <= s <= 1
    if sign > 0:
        assert s >= 0.5
    elif sign < 0:
        assert s <= -0.5
    else:
        assert abs(s) < 0.05


# ---------------------------------------------------------------- topics
def test_topics_entities_first_and_normalized(nlp):
    doc = nlp("Apple announced the new MacBook Pro chips at the keynote in Cupertino.")
    topics = extract_topics(doc)
    assert "apple" in topics
    assert topics.index("apple") < topics.index("keynote")
    assert all(t == t.lower() for t in topics)
    assert "the new macbook pro chip" not in topics  # determiner stripped
    assert any("macbook pro chip" in t for t in topics)  # lemmatized plural


def test_topics_drop_noise_numbers_and_short(nlp):
    doc = nlp("EDIT: anyone in this thread know? TL;DR 42 is the answer. OP said ok. r/python")
    topics = extract_topics(doc)
    for bad in ("edit", "anyone", "thread", "tl;dr", "42", "op", "ok", "r/python"):
        assert bad not in topics


def test_topics_drop_opinion_phrases(nlp):
    topics = extract_topics(nlp("Unpopular opinion: the new tax plan is the worst decision ever. Best thing ever lol"))
    for bad in ("bad decision", "worst decision", "good", "unpopular", "good thing", "best thing"):
        assert bad not in topics
    assert any("tax plan" in t for t in topics)


def test_topics_deduped_and_capped(nlp):
    text = ". ".join(f"Topic number {w} is about {w} cats" for w in
                     ["red", "blue", "green", "yellow", "purple", "orange", "pink", "brown", "black"])
    topics = extract_topics(nlp(text))
    assert len(topics) <= 8
    assert len(topics) == len(set(topics))


def test_entities_filtered_to_allowed_labels(nlp):
    ents = extract_entities(nlp("Elon Musk said Tesla will pay $5 billion in Texas on March 3rd."))
    labels = {e.label for e in ents}
    assert labels <= {"ORG", "PERSON", "GPE", "PRODUCT", "EVENT", "WORK_OF_ART", "NORP"}
    assert "MONEY" not in labels and "DATE" not in labels
    assert {"PERSON", "GPE"} & labels


# ---------------------------------------------------------------- category (3+ hand-labeled per class)
LABELED = {
    "Technology": [
        "My new laptop battery dies in two hours after the Windows update",
        "Best browser for privacy on Linux?",
        "Smartphone makers are finally switching to a better chip and processor",
    ],
    "AI/ML": [
        "OpenAI releases a new large language model with better inference",
        "Fine-tuning a transformer on my own dataset for deep learning class",
        "Is AGI closer than we think? The latest neural network benchmark results",
    ],
    "Gaming": [
        "The new PlayStation console exclusive has amazing open world gameplay",
        "Steam early access RPG just added multiplayer and new DLC",
        "Finally beat the boss fight after 40 hours, best video game this year",
    ],
    "Sports": [
        "The coach was fired after the team lost the playoff match",
        "Quarterback injury could change the whole season for the league",
        "Striker scores in overtime to win the championship tournament",
    ],
    "Politics": [
        "Senate vote on the new bill expected after the campaign",
        "Supreme Court ruling on the election law angers lawmakers",
        "Governor announces run for president in the next primary",
    ],
    "World News": [
        "Ceasefire talks collapse as troops gather at the border",
        "United Nations announces sanctions after the invasion",
        "Earthquake and flood leave thousands of refugees in need of humanitarian aid",
    ],
    "Finance/Crypto": [
        "Bitcoin and Ethereum crash as the Fed raises the interest rate",
        "Should I move my 401k into an index fund before the recession?",
        "My portfolio of dividend stocks after earnings season",
    ],
    "Science": [
        "NASA telescope spots a galaxy near a black hole",
        "New study shows how species evolution responds to climate change",
        "Physics experiment confirms a quantum particle discovery",
    ],
    "Health": [
        "Doctors warn about the new virus outbreak at the hospital",
        "My workout and diet for weight loss: calorie tracking and cardio",
        "Therapy helped my anxiety and depression more than medication",
    ],
    "Entertainment": [
        "The new movie trailer for the sequel looks incredible",
        "Season finale of the tv show was the best episode of the series",
        "Box office numbers for the film premiere were huge, director thanks the cast",
    ],
    "Music": [
        "Her new album has the best song of the year, can't wait for the tour",
        "The band played an amazing setlist at the concert and festival",
        "This rapper just dropped a mixtape and the music video is wild",
    ],
    "Business/Startups": [
        "Our startup just closed a seed round led by venture capital",
        "Founder and CEO announces layoffs after revenue drop",
        "The acquisition and merger will boost the company valuation before the IPO",
    ],
    "Education/Careers": [
        "Got a job offer after my internship, how do I negotiate salary?",
        "Should I do a PhD or take the job at the university?",
        "My resume keeps getting rejected by every recruiter on LinkedIn",
    ],
    "Lifestyle": [
        "My favorite recipe for baking sourdough bread and cooking at home",
        "Travel tips for a budget trip: cheap flight and hotel hacks",
        "Apartment renovation before and after with new furniture and decor",
    ],
    "Memes/Humor": [
        "me irl when the printer stops working lol",
        "Starter pack meme for every group project, so relatable",
        "Nobody: absolutely nobody: my cat at 3am lmao",
    ],
}


def test_labeled_set_covers_all_15(clf):
    assert set(LABELED) == set(clf.names) and len(clf.names) == 15


@pytest.mark.parametrize("category,title", [(c, t) for c, ts in LABELED.items() for t in ts])
def test_category_from_text_only(nlp, clf, category, title):
    pred = clf.predict(nlp(title), subreddit="AskReddit")
    assert pred.category == category, (title, sorted(pred.scores.items(), key=lambda kv: -kv[1])[:3])
    assert 0 < pred.confidence <= 1


def test_subreddit_prior_dominates_weak_text(nlp, clf):
    pred = clf.predict(nlp("Look at this"), subreddit="nba")
    assert pred.category == "Sports" and pred.confidence > 0.9


def test_no_evidence_falls_back(nlp, clf):
    pred = clf.predict(nlp("hmm"), subreddit="unknownsub123")
    assert pred.category in clf.names


def test_softmax_sums_to_one():
    p = softmax({"a": 1.0, "b": 2.0, "c": 0.0})
    assert abs(sum(p.values()) - 1) < 1e-9 and max(p, key=p.get) == "b"


# ---------------------------------------------------------------- enrichment end to end (no Kafka)
def test_post_text_uses_first_1000_chars():
    text, title_end = post_text(raw("Hello", "x" * 3000))
    assert text.startswith("Hello") and title_end == 5 and len(text) <= 5 + 2 + 1000


def test_enrich_batch(enricher):
    posts = [raw("Bitcoin hits an all-time high, amazing!", subreddit="CryptoCurrency"),
             raw("The Lakers lost again, terrible defense", subreddit="nba")]
    out = enricher.enrich(posts)
    assert all(isinstance(o, EnrichedPost) for o in out)
    assert out[0].category == "Finance/Crypto" and out[0].sentiment_label == "pos"
    assert out[1].category == "Sports" and out[1].sentiment_label == "neg"
    assert all(o.processed_at >= o.ingested_at and o.nlp_ms > 0 for o in out)
    assert "bitcoin" in out[0].topics


# ---------------------------------------------------------------- batching policy
def test_batch_buffer_flushes_on_size():
    b = BatchBuffer(max_size=3, max_age_s=10)
    b.add([1, 2], now=0.0)  # type: ignore[list-item]
    assert not b.ready(0.0) and b.room() == 1
    b.add([3], now=0.01)  # type: ignore[list-item]
    assert b.ready(0.01) and len(b.drain()) == 3 and b.first_at is None


def test_batch_buffer_flushes_on_age():
    b = BatchBuffer(max_size=32, max_age_s=0.1)
    assert b.wait_ms(0.0) == 100
    b.add([1], now=5.0)  # type: ignore[list-item]
    assert not b.ready(5.05) and 40 <= b.wait_ms(5.05) <= 60
    assert b.ready(5.1)
