"""Corpus contamination checker.

Finds near-duplicate articles across a train/test corpus before model
evaluation. Two (or more) API processes share one SQLite database; every
committed batch modification bumps the corpus generation and enqueues a
background verification job. Verification results (direct similarity edges
and connected groups) are persisted per generation and published
atomically; queries never compare texts on the fly.

Freshness rule: /contamination answers 200 only when the published results
cover the whole currently committed corpus (published_generation ==
current_generation); otherwise it answers 202 with the pending state, so
stale groups are never paired with new texts and a "clean" verdict is
never reported early.

Encoding repair: crawled articles sometimes contain UTF-8 spans misdecoded
as Latin-1 / Windows-1252. /encoding-fix/preview lists the repair
candidates for selected spans; /encoding-fix/confirm applies a chosen set
atomically (per-article optimistic versioning, one generation bump for the
whole batch, re-verification before any verdict is served again).
"""

import logging
import os
import socket
import sqlite3
import threading
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse

from .db import MAX_ARTICLES, MAX_TEXT_LEN, MIN_TEXT_LEN, connect, get_meta, init_db
from .mojibake import CONVERSIONS, inverse_convert
from .schemas import (
    ArticlePreview,
    BatchRequest,
    BatchResponse,
    CandidatePreview,
    ConfirmRequest,
    ConfirmResponse,
    FixedArticle,
    PreviewRequest,
    PreviewResponse,
    SegmentPreview,
)
from .worker import CompareWorker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("corpus.api")

DB_PATH = os.environ.get("DB_PATH", "/data/corpus.db")
INSTANCE_ID = os.environ.get("INSTANCE_ID") or socket.gethostname()
WORKER_INTERVAL_MS = int(os.environ.get("WORKER_INTERVAL_MS", "1000"))
COMPARE_DELAY_MS = int(os.environ.get("COMPARE_DELAY_MS", "0"))
# Chaos/restart-demo knob: when set, the process hard-exits (os._exit, like
# a crash) as soon as this file appears. Inert when unset.
EXIT_WHEN_FILE_EXISTS = os.environ.get("EXIT_WHEN_FILE_EXISTS", "")


def _watch_exit_file(path: str) -> None:
    while True:
        if os.path.exists(path):
            os._exit(1)
        time.sleep(0.2)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db(DB_PATH)
    worker = CompareWorker(
        DB_PATH,
        INSTANCE_ID,
        interval_ms=WORKER_INTERVAL_MS,
        compare_delay_ms=COMPARE_DELAY_MS,
    )
    # Restart recovery: if the corpus has unpublished changes (e.g. the
    # previous process died mid-verification), re-enqueue the job now.
    worker.ensure_pending_job()
    worker.start()
    app.state.worker = worker
    if EXIT_WHEN_FILE_EXISTS:
        log.warning("exit-file hook armed: %s", EXIT_WHEN_FILE_EXISTS)
        threading.Thread(
            target=_watch_exit_file, args=(EXIT_WHEN_FILE_EXISTS,), daemon=True
        ).start()
    log.info("instance %s ready, db=%s", INSTANCE_ID, DB_PATH)
    yield
    worker.stop()
    worker.join(timeout=5)


app = FastAPI(
    title="Corpus Contamination Checker",
    version="1.1.0",
    description=(
        "Detects near-duplicate articles (5-char shingle Jaccard >= 0.8) "
        "across train/test corpora. Results are computed in the background, "
        "persisted per corpus generation and served only when they cover "
        "the whole committed corpus. Also offers a two-step encoding repair "
        "API (preview/confirm) for UTF-8 spans misdecoded as Latin-1 or "
        "Windows-1252."
    ),
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.post("/articles/batch", response_model=BatchResponse)
def apply_batch(req: BatchRequest):
    """Apply one atomic batch of additions/corrections/deletions.

    Every article is validated first; the batch is applied in a single
    transaction together with the generation bump and the verification job.
    Any failure (invalid article, 300-article limit, id conflicts) rolls
    the whole batch back — nothing takes effect partially.
    """
    upsert_ids = [u.id for u in req.upserts]
    if len(upsert_ids) != len(set(upsert_ids)):
        raise HTTPException(422, "duplicate article ids in upserts")
    if len(req.deletes) != len(set(req.deletes)):
        raise HTTPException(422, "duplicate article ids in deletes")
    conflict = set(upsert_ids) & set(req.deletes)
    if conflict:
        raise HTTPException(
            422, f"article ids both upserted and deleted: {sorted(conflict)}"
        )

    conn = connect(DB_PATH)
    try:
        if not req.upserts and not req.deletes:
            current, published = get_meta(conn)
            count = conn.execute("SELECT COUNT(*) AS c FROM articles").fetchone()["c"]
            return BatchResponse(
                changed=False,
                generation=current,
                published_generation=published,
                upserted=0,
                deleted=0,
                articles=count,
            )

        conn.execute("BEGIN IMMEDIATE")
        try:
            current, _ = get_meta(conn)
            new_generation = current + 1

            deleted = 0
            for article_id in req.deletes:
                cur = conn.execute("DELETE FROM articles WHERE id = ?", (article_id,))
                deleted += cur.rowcount
            for u in req.upserts:
                conn.execute(
                    """
                    INSERT INTO articles (id, side, text, updated_generation)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        side = excluded.side,
                        text = excluded.text,
                        updated_generation = excluded.updated_generation
                    """,
                    (u.id, u.side, u.text, new_generation),
                )

            count = conn.execute("SELECT COUNT(*) AS c FROM articles").fetchone()["c"]
            if count > MAX_ARTICLES:
                raise HTTPException(
                    409,
                    f"corpus limit exceeded: batch would leave {count} articles "
                    f"(max {MAX_ARTICLES}); whole batch rolled back",
                )

            conn.execute(
                "UPDATE meta SET current_generation = ? WHERE id = 1", (new_generation,)
            )
            conn.execute(
                "INSERT OR IGNORE INTO compute_jobs (generation, status)"
                " VALUES (?, 'pending')",
                (new_generation,),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        _, published = get_meta(conn)
        log.info(
            "batch committed: generation=%d upserts=%d deleted=%d articles=%d",
            new_generation,
            len(req.upserts),
            deleted,
            count,
        )
        return BatchResponse(
            changed=True,
            generation=new_generation,
            published_generation=published,
            upserted=len(req.upserts),
            deleted=deleted,
            articles=count,
        )
    except HTTPException:
        raise
    except sqlite3.IntegrityError as exc:
        raise HTTPException(422, f"batch rejected, nothing applied: {exc}")
    finally:
        conn.close()


@app.get("/articles")
def list_articles():
    """List the currently committed corpus."""
    conn = connect(DB_PATH)
    try:
        rows = conn.execute(
            "SELECT id, side, text, updated_generation FROM articles ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    return {
        "count": len(rows),
        "articles": [
            {
                "id": r["id"],
                "side": r["side"],
                "text": r["text"],
                "text_length": len(r["text"]),
                "updated_generation": r["updated_generation"],
            }
            for r in rows
        ],
    }


# ---------------------------------------------------------------------------
# encoding repair (UTF-8 mojibake) — preview then confirm
# ---------------------------------------------------------------------------


def _preview_segment(text: str, start: int, end: int) -> SegmentPreview:
    if end > len(text):
        return SegmentPreview(
            start=start,
            end=end,
            error=(
                f"interval [{start}, {end}) is out of bounds for the current "
                f"text of length {len(text)}"
            ),
        )
    segment = text[start:end]
    candidates = []
    for conversion in CONVERSIONS:
        replacement, reason = inverse_convert(segment, conversion)
        candidates.append(
            CandidatePreview(
                conversion=conversion,
                valid=replacement is not None,
                replacement=replacement,
                reason=reason,
            )
        )
    return SegmentPreview(start=start, end=end, original=segment, candidates=candidates)


@app.post("/encoding-fix/preview", response_model=PreviewResponse)
def preview_encoding_fix(req: PreviewRequest):
    """Preview repairs for UTF-8 spans misdecoded as Latin-1 / Windows-1252.

    Selects up to 5 articles, each pinned to the ``updated_generation``
    reported by ``GET /articles`` and carrying up to 6 non-overlapping
    half-open character intervals. For every interval both inverse
    conversions are attempted against the *current* text: valid candidates
    are listed, non-convertible spans come with the reason. Read-only —
    use ``/encoding-fix/confirm`` to apply a selection.
    """
    ids = [a.id for a in req.articles]
    if len(ids) != len(set(ids)):
        raise HTTPException(422, "duplicate article ids in request")

    conn = connect(DB_PATH)
    try:
        conn.execute("BEGIN")
        previews = []
        for sel in req.articles:
            row = conn.execute(
                "SELECT text, updated_generation FROM articles WHERE id = ?",
                (sel.id,),
            ).fetchone()
            if row is None:
                previews.append(
                    ArticlePreview(
                        id=sel.id,
                        exists=False,
                        submitted_updated_generation=sel.updated_generation,
                        version_match=False,
                    )
                )
                continue
            text = row["text"]
            segments = [
                _preview_segment(text, iv.start, iv.end)
                for iv in sorted(sel.intervals, key=lambda i: (i.start, i.end))
            ]
            previews.append(
                ArticlePreview(
                    id=sel.id,
                    exists=True,
                    submitted_updated_generation=sel.updated_generation,
                    current_updated_generation=row["updated_generation"],
                    version_match=row["updated_generation"] == sel.updated_generation,
                    segments=segments,
                )
            )
        conn.commit()
        return PreviewResponse(articles=previews)
    finally:
        conn.close()


def _splice(text: str, splices) -> str:
    """Replace the sorted, non-overlapping ``(start, end, replacement)``
    spans of *text*, leaving everything else byte-identical."""
    parts, pos = [], 0
    for start, end, replacement in splices:
        parts.append(text[pos:start])
        parts.append(replacement)
        pos = end
    parts.append(text[pos:])
    return "".join(parts)


@app.post("/encoding-fix/confirm", response_model=ConfirmResponse)
def confirm_encoding_fix(req: ConfirmRequest):
    """Apply one atomic batch of encoding repairs.

    Every selected article is re-read inside a single transaction: its
    ``updated_generation`` must still match (the article was not edited
    since the preview — edits of *unrelated* articles do not matter), each
    interval must be in bounds, and each chosen candidate is recomputed
    from the current text and must match the submitted replacement. The
    repaired text must keep satisfying the 5–2000 character limit. If any
    check fails, the whole batch is rolled back (409) and nothing changes.
    On success all repairs commit together, the corpus generation advances
    exactly once and a fresh verification is enqueued — ``/contamination``
    answers 202 until it covers the repaired corpus.
    """
    ids = [a.id for a in req.articles]
    if len(ids) != len(set(ids)):
        raise HTTPException(422, "duplicate article ids in request")

    conn = connect(DB_PATH)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            current, _ = get_meta(conn)
            new_generation = current + 1

            errors = []
            planned = []  # (id, new_text, segment_count)
            for sel in req.articles:
                row = conn.execute(
                    "SELECT text, updated_generation FROM articles WHERE id = ?",
                    (sel.id,),
                ).fetchone()
                if row is None:
                    errors.append(
                        {"id": sel.id, "reason": "article does not exist (anymore)"}
                    )
                    continue
                text = row["text"]
                if row["updated_generation"] != sel.updated_generation:
                    errors.append(
                        {
                            "id": sel.id,
                            "reason": (
                                "article was modified since the preview: current "
                                f"updated_generation is {row['updated_generation']}, "
                                f"submitted {sel.updated_generation}"
                            ),
                        }
                    )
                    continue

                splices = []
                article_error = None
                for f in sorted(sel.fixes, key=lambda i: (i.start, i.end)):
                    if f.end > len(text):
                        article_error = {
                            "id": sel.id,
                            "start": f.start,
                            "end": f.end,
                            "reason": (
                                f"interval [{f.start}, {f.end}) is out of bounds "
                                f"for the current text of length {len(text)}"
                            ),
                        }
                        break
                    segment = text[f.start : f.end]
                    replacement, reason = inverse_convert(segment, f.conversion)
                    if replacement is None:
                        article_error = {
                            "id": sel.id,
                            "start": f.start,
                            "end": f.end,
                            "reason": (
                                f"chosen {f.conversion} candidate is not valid "
                                f"for the current text: {reason}"
                            ),
                        }
                        break
                    if replacement != f.replacement:
                        article_error = {
                            "id": sel.id,
                            "start": f.start,
                            "end": f.end,
                            "reason": (
                                "candidate mismatch: submitted replacement does "
                                "not match the candidate recomputed from the "
                                f"current text ({replacement!r})"
                            ),
                        }
                        break
                    splices.append((f.start, f.end, replacement))
                if article_error is not None:
                    errors.append(article_error)
                    continue

                new_text = _splice(text, splices)
                if not MIN_TEXT_LEN <= len(new_text) <= MAX_TEXT_LEN:
                    errors.append(
                        {
                            "id": sel.id,
                            "reason": (
                                f"repaired text would be {len(new_text)} "
                                f"characters, outside the allowed "
                                f"{MIN_TEXT_LEN}..{MAX_TEXT_LEN}"
                            ),
                        }
                    )
                    continue
                planned.append((sel.id, new_text, len(splices)))

            if errors:
                raise HTTPException(
                    409,
                    detail={
                        "message": "encoding fix rejected, nothing applied",
                        "errors": errors,
                    },
                )

            for article_id, new_text, _ in planned:
                conn.execute(
                    "UPDATE articles SET text = ?, updated_generation = ?"
                    " WHERE id = ?",
                    (new_text, new_generation, article_id),
                )
            conn.execute(
                "UPDATE meta SET current_generation = ? WHERE id = 1",
                (new_generation,),
            )
            conn.execute(
                "INSERT OR IGNORE INTO compute_jobs (generation, status)"
                " VALUES (?, 'pending')",
                (new_generation,),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        _, published = get_meta(conn)
        log.info(
            "encoding fix committed: generation=%d articles=%d segments=%d",
            new_generation,
            len(planned),
            sum(n for _, _, n in planned),
        )
        return ConfirmResponse(
            changed=True,
            generation=new_generation,
            published_generation=published,
            fixed_articles=len(planned),
            fixed_segments=sum(n for _, _, n in planned),
            articles=[
                FixedArticle(
                    id=article_id,
                    text_length=len(new_text),
                    updated_generation=new_generation,
                )
                for article_id, new_text, _ in planned
            ],
        )
    except HTTPException:
        raise
    except sqlite3.IntegrityError as exc:
        raise HTTPException(409, f"encoding fix rejected, nothing applied: {exc}")
    finally:
        conn.close()


@app.get("/contamination")
def contamination():
    """Cross-set contamination report for the current corpus.

    200: results cover every committed article — returns contamination
    groups (connected components containing both train and test articles),
    their members and the direct similarity edges inside each group.
    202: verification is still pending for the current corpus state.
    """
    conn = connect(DB_PATH)
    try:
        # One read transaction => consistent view of pointer + results.
        conn.execute("BEGIN")
        current, published = get_meta(conn)
        if published != current:
            conn.rollback()
            return JSONResponse(
                status_code=202,
                content={
                    "status": "pending",
                    "detail": "verification does not cover the current corpus yet",
                    "current_generation": current,
                    "published_generation": published,
                },
                headers={"Retry-After": "1"},
            )
        group_rows = conn.execute(
            """
            SELECT g.group_id, g.article_id, g.side, a.text
            FROM article_groups g JOIN articles a ON a.id = g.article_id
            WHERE g.generation = ?
            ORDER BY g.group_id, g.article_id
            """,
            (published,),
        ).fetchall()
        edge_rows = conn.execute(
            "SELECT a, b, similarity FROM edges WHERE generation = ? ORDER BY a, b",
            (published,),
        ).fetchall()
        conn.commit()
    finally:
        conn.close()

    edges = [
        {"a": e["a"], "b": e["b"], "similarity": e["similarity"]} for e in edge_rows
    ]
    by_group = {}
    for r in group_rows:
        by_group.setdefault(r["group_id"], []).append(r)

    contamination_groups = []
    same_set_groups = 0
    for group_id, members in sorted(by_group.items()):
        sides = {m["side"] for m in members}
        if sides != {"train", "test"}:
            same_set_groups += 1
            continue
        member_ids = {m["article_id"] for m in members}
        contamination_groups.append(
            {
                "group_id": group_id,
                "size": len(members),
                "train_members": sorted(
                    m["article_id"] for m in members if m["side"] == "train"
                ),
                "test_members": sorted(
                    m["article_id"] for m in members if m["side"] == "test"
                ),
                "members": [
                    {"id": m["article_id"], "side": m["side"], "text": m["text"]}
                    for m in members
                ],
                "edges": [
                    e
                    for e in edges
                    if e["a"] in member_ids and e["b"] in member_ids
                ],
            }
        )

    return {
        "status": "ok",
        "generation": published,
        "contaminated": bool(contamination_groups),
        "contamination_group_count": len(contamination_groups),
        "same_set_group_count": same_set_groups,
        "groups": contamination_groups,
    }


@app.get("/status")
def status():
    """Instance, generation and background-job state."""
    conn = connect(DB_PATH)
    try:
        current, published = get_meta(conn)
        row = conn.execute(
            """
            SELECT COUNT(*) AS total,
                   COALESCE(SUM(side = 'train'), 0) AS train,
                   COALESCE(SUM(side = 'test'), 0) AS test
            FROM articles
            """
        ).fetchone()
        jobs = conn.execute(
            "SELECT generation, status, claimed_by, claimed_at FROM compute_jobs"
            " ORDER BY generation"
        ).fetchall()
    finally:
        conn.close()
    return {
        "instance": INSTANCE_ID,
        "current_generation": current,
        "published_generation": published,
        "up_to_date": current == published,
        "articles": {"total": row["total"], "train": row["train"], "test": row["test"]},
        "jobs": [
            {
                "generation": j["generation"],
                "status": j["status"],
                "claimed_by": j["claimed_by"],
                "claimed_at": j["claimed_at"],
            }
            for j in jobs
        ],
        "config": {
            "max_articles": MAX_ARTICLES,
            "compare_delay_ms": COMPARE_DELAY_MS,
            "worker_interval_ms": WORKER_INTERVAL_MS,
        },
    }
