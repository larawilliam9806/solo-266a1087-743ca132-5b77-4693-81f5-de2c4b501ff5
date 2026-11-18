# Observation Archive Collector

A small FastAPI service that collects per-station/per-day observation archives
from a directory-driven source, with pause/resume, restart-safe progress,
verify-before-download semantics and a revision-impact API that tells
downstream consumers exactly which moments changed between two published
versions of the same station-day.

## Components

| Service | Port | Purpose |
| --- | --- | --- |
| `collector` | 8000 | Collection API + OpenAPI docs at `/docs` |
| `source` | 9000 | Local mock observation source with strong ETags, Range, revisions, latency and fault injection |

## Run with Docker Compose

```bash
docker compose up --build
```

- API docs / demo entry point: <http://localhost:8000/docs>
- Root descriptor: <http://localhost:8000/> lists all endpoints
- Source admin state: <http://localhost:9000/admin/state>

Docker is not required for the tests; they start both ASGI apps on loopback
ports in-process.

A scripted end-to-end demonstration (collect → pause → resume → revision switch
→ verified download):

```bash
python scripts/demo.py
```

Local (non-Docker) run:

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn mock_source.app:app --port 9000 &
SOURCE_BASE_URL=http://127.0.0.1:9000 COLLECTOR_DATA_DIR=./data \
  uvicorn app.main:app --port 8000
python scripts/demo.py
```

`POST /tasks` returns HTTP 201 for new tasks; an existing station+date returns
the current task.

## API

| Method & path | Meaning |
| --- | --- |
| `POST /tasks` `{station,date}` | Create (or return the existing) task; one task per station+date |
| `POST /tasks/{id}/start` | Start a queued/paused/failed task |
| `POST /tasks/{id}/pause` | Pause; returns only after in-flight writes can no longer land |
| `POST /tasks/{id}/resume` | Resume from durable progress |
| `POST /tasks/{id}/refresh` | Re-read the directory for a completed task and re-collect if it moved |
| `GET /tasks`, `GET /tasks/{id}` | List / inspect tasks (state, bytes, percent, expected vs verified size & SHA-256, error, `latest_revision_seq`) |
| `GET /tasks/{id}/events` | Append-only event log for the task |
| `GET /tasks/{id}/package` | Download the currently published archive; `409` until a first verified package exists |
| `GET /tasks/{id}/revisions` | Revision-impact manifests, one per publication that changed the package |
| `GET /tasks/{id}/revisions/latest` | The most recent manifest (`404` if none) |
| `GET /tasks/{id}/revisions/{seq}` | One manifest by sequence number |

`GET /tasks` accepts `?state=running|paused|completed|failed|queued`.

## Revision impact

When the source corrects a station-day and `refresh` publishes the new
package, the service parses `observations.csv` from BOTH versions (the
previous validated package is pinned before the refresh starts), aligns rows
by `time` — duplicate times resolve to the last row — and records every
moment that was **added**, **withdrawn** or had **temperature_c /
pressure_hpa / wind_mps** changed, with the before/after values and the
SHA-256 of both versions:

```json
{
  "seq": 1,
  "previous_sha256": "b753…", "current_sha256": "3057…",
  "previous_revision": "v1", "current_revision": "v2",
  "added": 3, "withdrawn": 3, "changed": 6,
  "changes": [
    {"time": "00:45", "kind": "changed",
     "before": {"temperature_c": -11.7, "pressure_hpa": 965.69, "wind_mps": 16.36},
     "after":  {"temperature_c": -9.94, "pressure_hpa": 965.69, "wind_mps": 16.36},
     "fields": ["temperature_c"]},
    {"time": "02:34", "kind": "added", "after": {"temperature_c": 1.2, "...": "..."}},
    {"time": "06:00", "kind": "withdrawn", "before": {"...": "..."}}
  ]
}
```

Semantics:

* A manifest is published only when the new package passes size + SHA-256
  verification AND its `observations.csv` validates (station, date and every
  numeric value) AND the complete diff against the pinned previous package
  could be built.  The manifest row and the task's completion commit in ONE
  SQLite transaction, so the package and its list are always seen as one
  publication — never a new package beside a stale list, and never half a
  list after a restart.
* A wrong station, wrong date or bad value in the new CSV fails the WHOLE
  refresh: the task becomes `failed` with the reason, and the previous
  validated package keeps being served with its previous manifest list.
* Source outages, parse failures and interruptions behave the same way: the
  old publication (package + manifests) survives; the package endpoint keeps
  serving the old bytes during and after the failed refresh.
* During a refresh the old package remains downloadable until the exact
  moment its replacement is committed.
* If the source package did not change, no new manifest is added.  A first
  collection has no previous version, so it publishes no manifest either.

## Correctness guarantees

1. **Directory is authoritative.** Each attempt reads size and SHA-256 from the
   source directory; nothing is trusted from an earlier attempt unless the whole
   identity (URL + strong ETag + size + digest) still matches.
2. **Resume only within one version.** The part file name embeds a hash of the
   source identity. If the directory reports a changed revision, partial bytes
   and metadata are discarded before a fresh transfer starts, so bytes from two
   versions are never concatenated.
3. **Range + If-Range.** Resumes send `Range: bytes=N-` and `If-Range: <strong
   ETag>`. A mismatched ETag, a wrong `Content-Range`, or a 412/416 causes a
   re-read instead of an append. A server that ignores Range but returns the
   same ETag is handled by truncating and restarting that version from zero
   (never appending a full body to partial data).
4. **Pause is a generation fence.** Pause bumps `generation` transactionally
   before responding; every worker write is a compare-and-set on
   `(state='running', generation)`. Late chunks/responses therefore cannot
   advance progress, mark completion, or publish a package after the pause
   response.
5. **Restart-safe.** Running tasks are flipped to `paused` at startup; the part
   file and byte count survive. With `AUTO_RESUME=true` only tasks interrupted by
   the restart (and queued tasks) resume automatically — explicit user pauses are
   left alone.
6. **Verify before exposure.** Completion requires the downloaded file's length
   and SHA-256 to equal the directory entry's. The file is written to a temp
   name and atomically renamed; the task becomes `completed` only after the
   rename. Until then — and for tasks that never published — `GET .../package`
   returns 409. The directory is re-read once more just before publishing so a
   mid-transfer revision swap is discarded, not published.
7. **Failure keeps the reason — and the publication.** Failed tasks retain
   `error`; transient failures (timeouts, 5xx, network) keep their partial
   prefix and can be retried via `start`; terminal failures (bad metadata,
   repeated verification/source mismatch, invalid observations) delete partial
   data.  A failure NEVER deletes or hides the currently published package:
   after a failed refresh the previous validated package and its manifest list
   remain available.
8. **Package and manifest are one publication.** The revision manifest is
   inserted in the same SQLite transaction that marks the task completed and
   points `latest_revision_seq` at it.  Manifest rows are immutable and
   append-only, so a list query interleaved with a refresh always sees a
   prefix of the final history, and a crash/restart cannot leave half a
   manifest behind.  The previous package file is deleted only after the
   replacement's transaction commits.

## Source mock controls

```bash
# switch the revision served for a station/date (changes body, size, digest;
# non-baseline revisions apply deterministic corrections to a few moments)
curl -X POST localhost:9000/admin/revision \
  -H 'content-type: application/json' \
  -d '{"station":"54511","date":"2026-11-17","revision":"v2"}'

# serve an explicit observations.csv as a revision (precise revision-impact demos)
curl -X POST localhost:9000/admin/package \
  -H 'content-type: application/json' \
  -d '{"station":"54511","date":"2026-11-17","revision":"v3",
       "csv_text":"station,date,time,temperature_c,pressure_hpa,wind_mps\n54511,2026-11-17,00:00,1.0,1000.0,2.0\n"}'

# add 300 ms header delay and 10 ms per-chunk delay
curl -X POST localhost:9000/admin/delay \
  -H 'content-type: application/json' -d '{"seconds":0.3,"chunk_seconds":0.01}'

# simulate outage (503) or a corrupted body (honest digest, tampered bytes)
curl -X POST localhost:9000/admin/fault \
  -H 'content-type: application/json' -d '{"station":"54511","date":"2026-11-17","mode":"fail"}'
```

`mode` accepts `fail`, `corrupt` or `clear`. `GET /admin/state` also shows the
recent package requests including their `Range`/`If-Range` headers.

## Tests

```bash
pytest -q
```

The suite runs the real API and the real mock source on loopback ports and
covers: full verified flow, duplicate task creation, pause with a late response
(progress/completion must not move), restart recovery with a real `Range`
resume, revision change mid-pause (old bytes discarded), corruption → failure
with no downloadable package, outage → failure with reason → successful retry,
refresh replacing a package on revision change, a source that ignores Range
(clean restart, no append), a pause issued before a delayed response arrives,
input validation — and the revision-impact API: manifest content for explicit
CSV edits (added/withdrawn/changed with before/after values, duplicate times
won by the last row), no manifest for an unchanged source, empty-diff
manifests, invalid station/date/value failing the whole refresh while the old
package and manifest list survive, source outage mid-refresh keeping the old
publication, old-package downloads during a refresh, interleaved list queries
that only ever see committed publications, and restart mid-refresh leaving a
consistent state.
