"""Request/response schemas for the corpus API."""

from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator
from typing_extensions import Annotated

from .db import MAX_ARTICLES, MAX_TEXT_LEN, MIN_TEXT_LEN

ArticleId = Annotated[str, Field(min_length=1, max_length=128)]
ArticleText = Annotated[str, Field(min_length=MIN_TEXT_LEN, max_length=MAX_TEXT_LEN)]


class ArticleUpsert(BaseModel):
    id: ArticleId
    side: Literal["train", "test"]
    text: ArticleText


class BatchRequest(BaseModel):
    """One atomic corpus modification: additions/corrections plus deletions.

    The whole batch is validated first and applied in a single transaction;
    if anything fails, nothing takes effect.
    """

    upserts: list[ArticleUpsert] = Field(default_factory=list, max_length=MAX_ARTICLES)
    deletes: list[ArticleId] = Field(default_factory=list, max_length=MAX_ARTICLES)


class BatchResponse(BaseModel):
    changed: bool
    generation: int
    published_generation: int
    upserted: int
    deleted: int
    articles: int


# ---------------------------------------------------------------------------
# encoding repair (mojibake) preview / confirm
# ---------------------------------------------------------------------------

# One fix request selects at most 5 articles, each with at most 6
# non-overlapping half-open character intervals.
MAX_FIX_ARTICLES = 5
MAX_FIX_INTERVALS = 6

# Inverse mis-decoding that produced the mojibake: the segment is encoded
# back with this charset and the resulting bytes decoded as UTF-8.
Conversion = Literal["latin-1", "windows-1252"]


def _ensure_non_overlapping(intervals):
    ordered = sorted((iv.start, iv.end) for iv in intervals)
    for (s1, e1), (s2, e2) in zip(ordered, ordered[1:]):
        if s2 < e1:
            raise ValueError(f"intervals [{s1}, {e1}) and [{s2}, {e2}) overlap")
    return intervals


class FixInterval(BaseModel):
    """Half-open character interval ``[start, end)`` into the article text."""

    start: int = Field(ge=0)
    end: int = Field(ge=1)

    @model_validator(mode="after")
    def _half_open(self):
        if self.end <= self.start:
            raise ValueError(
                "interval must satisfy start < end (half-open [start, end))"
            )
        return self


class PreviewArticle(BaseModel):
    """One article selected for a repair preview.

    ``updated_generation`` is the per-article version reported by
    ``GET /articles``; the preview recomputes candidates from the current
    text and reports whether this version still matches.
    """

    id: ArticleId
    updated_generation: int = Field(ge=0)
    intervals: list[FixInterval] = Field(
        min_length=1, max_length=MAX_FIX_INTERVALS
    )

    @field_validator("intervals")
    @classmethod
    def _non_overlapping(cls, v):
        return _ensure_non_overlapping(v)


class PreviewRequest(BaseModel):
    articles: list[PreviewArticle] = Field(
        min_length=1, max_length=MAX_FIX_ARTICLES
    )


class ConfirmInterval(FixInterval):
    """One chosen repair candidate for a half-open interval."""

    conversion: Conversion
    replacement: str = Field(min_length=1)


class ConfirmArticle(BaseModel):
    """One article to repair, pinned to its previewed version."""

    id: ArticleId
    updated_generation: int = Field(ge=0)
    fixes: list[ConfirmInterval] = Field(min_length=1, max_length=MAX_FIX_INTERVALS)

    @field_validator("fixes")
    @classmethod
    def _non_overlapping(cls, v):
        return _ensure_non_overlapping(v)


class ConfirmRequest(BaseModel):
    articles: list[ConfirmArticle] = Field(min_length=1, max_length=MAX_FIX_ARTICLES)


class CandidatePreview(BaseModel):
    conversion: Conversion
    valid: bool
    replacement: str | None = None
    reason: str | None = None


class SegmentPreview(BaseModel):
    start: int
    end: int
    original: str | None = None
    error: str | None = None
    candidates: list[CandidatePreview] = Field(default_factory=list)


class ArticlePreview(BaseModel):
    id: str
    exists: bool
    submitted_updated_generation: int
    current_updated_generation: int | None = None
    version_match: bool
    segments: list[SegmentPreview] = Field(default_factory=list)


class PreviewResponse(BaseModel):
    articles: list[ArticlePreview]


class FixedArticle(BaseModel):
    id: str
    text_length: int
    updated_generation: int


class ConfirmResponse(BaseModel):
    changed: bool
    generation: int
    published_generation: int
    fixed_articles: int
    fixed_segments: int
    articles: list[FixedArticle]
