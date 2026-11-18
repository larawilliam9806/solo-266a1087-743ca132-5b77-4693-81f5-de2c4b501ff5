"""Download worker: resumable, ETag-guarded, verified by size and SHA-256.

Invariants enforced here
------------------------
* Every worker-side mutation is a compare-and-set from ``state='running'`` with
  the generation the worker started with.  Pausing (or a newer start) bumps the
  generation *before* the HTTP call returns, so a late chunk or late response
  can never advance, complete or overwrite a paused task.
* Resumption is only allowed against the exact source identity recorded in the
  database (URL + strong ETag + size + digest + revision).  If the directory
  reports a different identity, the old part file is discarded and a fresh
  transfer begins from the new directory entry.
* Before publishing, the directory is re-read: a package assembled from a
  revision that was superseded mid-transfer is discarded, never stitched.
* A package becomes downloadable only after size and SHA-256 both match the
  directory entry *and* the file is atomically renamed into place; only then is
  the task marked completed.
* The current publication (``package_path`` + ``verified_*``) is never cleared
  by a refresh, a failure or an interruption: the previous validated package
  stays downloadable until its replacement is fully verified and committed.
* Revision impact: when a refresh replaces a published package, both versions'
  ``observations.csv`` are parsed and validated (station/date/values) BEFORE
  anything is published, and the diff manifest is committed in the SAME
  transaction as the task's completion — a new package is never visible next
  to a stale manifest, and a crash can never leave half a manifest behind.
  An unchanged source package produces no new manifest.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

from .config import Settings
from .models import CatalogEntry
from .observations import PackageError, diff_observations, read_observations
from .store import CasConflict, Store, utcnow

log = logging.getLogger("collector.worker")


class Resync(Exception):
    """The source changed underneath us; re-read the directory and restart."""


class FatalSourceError(Exception):
    """The directory cannot be trusted (bad metadata, missing entry, ...)."""


class Interrupted(Exception):
    """The task was paused, superseded or shut down mid-transfer."""


class TransferError(Exception):
    """A transient or permanent HTTP failure."""


@dataclass
class RunningTask:
    task_id: str
    generation: int
    cancel: asyncio.Event = field(default_factory=asyncio.Event)
    task: asyncio.Task | None = None


class Collector:
    def __init__(
        self,
        settings: Settings,
        http_client: httpx.AsyncClient | None = None,
    ):
        self.settings = settings
        self.store = Store(settings.database_path)
        settings.parts_dir.mkdir(parents=True, exist_ok=True)
        settings.packages_dir.mkdir(parents=True, exist_ok=True)
        self._runs: dict[str, RunningTask] = {}
        self._tasks: set[asyncio.Task] = set()
        self._client: httpx.AsyncClient | None = http_client
        self._owns_client = http_client is None

    # -- lifecycle -------------------------------------------------------
    async def startup(self) -> None:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.settings.http_timeout, connect=5.0),
                follow_redirects=True,
                headers={"User-Agent": "observation-collector/1.0"},
            )
        interrupted = [row["id"] for row in self.store.list("running")]
        self._recover_interrupted()
        if self.settings.auto_resume:
            # Only tasks that were mid-transfer when the process died (plus queued
            # work) resume automatically; an operator's explicit pause is respected.
            for task_id in interrupted + [row["id"] for row in self.store.list("queued")]:
                row = self.store.get(task_id)
                if row and row["state"] in {"paused", "queued"}:
                    await self.start(task_id)

    async def shutdown(self) -> None:
        for run in list(self._runs.values()):
            run.cancel.set()
        if self._tasks:
            _, pending = await asyncio.wait(self._tasks, timeout=10)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        self._recover_interrupted()
        try:
            self.store.close()
        except Exception:  # noqa: BLE001 - shutdown must not mask the real error
            log.warning("could not close the SQLite connection", exc_info=True)
        if self._owns_client and self._client:
            await self._client.aclose()
            self._client = None

    def _recover_interrupted(self) -> None:
        """A process restart leaves running tasks resumable rather than lost."""
        for row in self.store.list("running"):
            self.store.cas_state(
                row["id"],
                "running",
                "paused",
                expected_generation=row["generation"],
                generation=row["generation"] + 1,
                error="interrupted by process restart; resumable",
            )
            self.store.event(row["id"], "interrupted", "resumable after restart")

    # -- public operations ----------------------------------------------
    def create_task(self, task_id: str, station: str, date: str) -> None:
        self.store.insert_task(task_id, station, date)

    async def start(self, task_id: str) -> dict[str, str]:
        row = self.get_row(task_id)
        state = row["state"]
        if state == "completed":
            return {"state": "completed", "detail": "already completed"}
        if state == "running":
            return {"state": "running", "detail": "already running"}

        generation = row["generation"] + 1
        fields: dict[str, Any] = {
            "generation": generation,
            "error": None,
            "attempts": row["attempts"] + 1,
        }
        if state == "failed":
            # Resume only a failure that kept a valid partial prefix; terminal
            # failures (verification/source errors) start from zero.
            row_part = Path(row["part_path"]) if row["part_path"] else None
            if not row_part or not row_part.exists() or row["downloaded_bytes"] <= 0:
                self._discard_parts(task_id)
                fields.update(downloaded_bytes=0, part_path=None)
        # Starting a failed task is an explicit retry; starting a paused task is a
        # resume.  Both are atomic and generation-bumped.
        if not self.store.cas_state(task_id, state, "running", **fields):
            current = self.store.get(task_id)
            return {
                "state": current["state"] if current else "unknown",
                "detail": "state changed concurrently",
            }
        self.store.event(
            task_id,
            "resume" if state == "paused" else "started",
            f"attempt {row['attempts'] + 1}",
        )
        self._spawn(task_id, generation)
        return {"state": "running", "detail": "started"}

    async def refresh(self, task_id: str) -> dict[str, str]:
        """Re-read the directory for a completed task; collect again if it moved.

        The current publication stays pinned: the previous validated package
        keeps its ``verified_*`` fields and remains downloadable for the whole
        refresh, and it is only replaced together with its revision manifest.
        """
        row = self.get_row(task_id)
        if row["state"] != "completed":
            return {"state": row["state"], "detail": "refresh requires a completed task"}
        generation = row["generation"] + 1
        if not self.store.cas_state(
            task_id,
            "completed",
            "running",
            generation=generation,
            error=None,
            attempts=row["attempts"] + 1,
        ):
            return {"state": "unknown", "detail": "state changed concurrently"}
        self.store.event(task_id, "refresh", "re-reading source directory")
        self._spawn(task_id, generation)
        return {"state": "running", "detail": "refreshing"}

    async def pause(self, task_id: str) -> dict[str, str]:
        row = self.get_row(task_id)
        state = row["state"]
        if state == "paused":
            return {"state": "paused", "detail": "already paused"}
        if state == "completed":
            return {"state": "completed", "detail": "already completed"}
        if state == "failed":
            return {"state": "failed", "detail": "task failed; reason retained"}

        # Bump the generation *first*.  Even if a response is in flight right now,
        # its generation guard will reject every subsequent write.  A queued task
        # that never started is paused with no part file at all.
        moved = self.store.cas_state(
            task_id,
            state,
            "paused",
            expected_generation=row["generation"],
            generation=row["generation"] + 1,
            error=None if state == "running" else row["error"],
        )
        run = self._runs.get(task_id)
        if not moved:
            current = self.store.get(task_id)
            return {
                "state": current["state"] if current else "unknown",
                "detail": "state changed concurrently",
            }
        if run:
            run.cancel.set()
        if state == "running":
            # Wait (bounded) until the worker has stopped writing.  The DB is
            # already authoritative, so even a stuck socket cannot advance it.
            run = self._runs.get(task_id)
            if run and run.task:
                await asyncio.wait({run.task}, timeout=5)
            row = self.store.get(task_id)
            # A transfer that finished between the CAS and the cancel has
            # completed legitimately; report it as such rather than lying.
            if row and row["state"] == "completed":
                return {"state": "completed", "detail": "completed before pause took effect"}
            if row and row["state"] == "paused" and row["part_path"]:
                part = Path(row["part_path"])
                if part.exists():
                    # Now that no writer remains, sync the reported progress with
                    # the durable bytes on disk.
                    self.store.update(
                        task_id,
                        downloaded_bytes=min(part.stat().st_size, row["expected_size"] or 0),
                    )
        self.store.event(task_id, "pause_requested", "late responses rejected")
        return {"state": "paused", "detail": "paused; progress retained"}

    def get_row(self, task_id: str):
        row = self.store.get(task_id)
        if row is None:
            raise KeyError(task_id)
        return row

    def _spawn(self, task_id: str, generation: int) -> None:
        run = RunningTask(task_id=task_id, generation=generation)
        self._runs[task_id] = run
        task = asyncio.create_task(self._run(run), name=f"collect:{task_id}")
        run.task = task
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # -- worker ----------------------------------------------------------
    async def _run(self, run: RunningTask) -> None:
        try:
            await self._collect(run)
        except Interrupted:
            log.info("task %s interrupted; progress kept", run.task_id)
        except FatalSourceError as exc:
            self._fail(run, str(exc), retryable=False)
        except PackageError as exc:
            # The fetched package fails validation (bad station/date/values,
            # unreadable CSV, ...): fail the refresh.  The previous validated
            # package and its manifest stay published.
            self._fail(run, f"invalid package: {exc}", retryable=False)
        except asyncio.CancelledError:
            raise
        except TransferError as exc:
            # Timeout / network / 5xx: the part file is a valid prefix, so keep it
            # and report the failure reason.  A later resume continues with Range.
            self._fail(run, str(exc), retryable=True)
        except (httpx.HTTPError, OSError, ValueError) as exc:
            self._fail(run, f"{type(exc).__name__}: {exc}", retryable=False)
        except Exception as exc:  # noqa: BLE001 - the worker must never vanish
            log.exception("unexpected failure for %s", run.task_id)
            self._fail(run, f"unexpected error: {type(exc).__name__}: {exc}", retryable=False)
        finally:
            if self._runs.get(run.task_id) is run:
                self._runs.pop(run.task_id, None)

    def _fail(self, run: RunningTask, message: str, retryable: bool) -> None:
        # The current publication (package_path + verified_*) is deliberately
        # never touched here: a failed refresh must keep the previous validated
        # package downloadable together with its last manifest.  A first
        # download that never published simply has nothing to keep.
        row = self.store.get(run.task_id)
        if retryable:
            part = Path(row["part_path"]) if row and row["part_path"] else None
            keep = part.stat().st_size if part and part.exists() else 0
            moved = self.store.cas_state(
                run.task_id,
                "running",
                "failed",
                expected_generation=run.generation,
                error=message,
                downloaded_bytes=keep,
            )
        else:
            moved = self.store.cas_state(
                run.task_id,
                "running",
                "failed",
                expected_generation=run.generation,
                error=message,
                part_path=None,
                downloaded_bytes=0,
            )
        if moved:
            if not retryable:
                # Terminal failure: no partial data that could be mistaken for
                # resumable bytes.  The published package, if any, stays.
                self._discard_parts(run.task_id)
            self.store.event(
                run.task_id,
                "failed",
                message if not retryable else f"{message} (progress retained, resumable)",
            )

    def _alive(self, run: RunningTask) -> bool:
        if run.cancel.is_set():
            return False
        row = self.store.get(run.task_id)
        return bool(
            row and row["state"] == "running" and row["generation"] == run.generation
        )

    def _update(self, run: RunningTask, **fields: Any) -> None:
        if not self.store.update_guarded(run.task_id, run.generation, **fields):
            raise Interrupted()

    def _note(self, run: RunningTask, event: str, detail: str | None = None) -> None:
        if self._alive(run):
            self.store.event(run.task_id, event, detail)

    async def _collect(self, run: RunningTask) -> None:
        limit = max(1, self.settings.max_source_resyncs)
        for _ in range(limit):
            try:
                await self._attempt(run)
                return
            except Resync as exc:
                if not self._alive(run):
                    raise Interrupted() from exc
                log.warning("source changed for %s: %s", run.task_id, exc)
                row = self.store.get(run.task_id)
                if not self.store.update_guarded(
                    run.task_id,
                    run.generation,
                    resyncs=(row["resyncs"] if row else 0) + 1,
                    error=None,
                ):
                    raise Interrupted() from exc
                self.store.event(run.task_id, "source_changed", str(exc))
        raise FatalSourceError(
            f"source kept changing or failed verification after {limit} re-reads"
        )

    # -- directory -------------------------------------------------------
    async def _read_directory(self, run: RunningTask, station: str, date: str) -> CatalogEntry:
        assert self._client is not None
        url = f"{self.settings.source_base_url}/directory/{station}/{date}"
        try:
            response = await self._client.get(url)
        except httpx.HTTPError as exc:
            raise TransferError(f"directory request failed: {type(exc).__name__}: {exc}") from exc
        if response.status_code == 404:
            raise FatalSourceError("directory entry not found (404)")
        if response.status_code != 200:
            raise TransferError(f"directory returned HTTP {response.status_code}")
        try:
            payload = response.json()
            entries = payload["entries"]
            if not isinstance(entries, list) or not entries:
                raise ValueError("empty entries")
            raw = next(
                (e for e in entries if str(e.get("name", "")).endswith(".tar.gz")),
                entries[0],
            )
            entry = CatalogEntry(
                station=station,
                date=date,
                url=raw.get("url") or f"/packages/{station}/{date}.tar.gz",
                size=int(raw["size"]),
                sha256=raw["sha256"],
                etag=raw["etag"],
                revision=payload.get("revision") or raw.get("revision"),
                updated_at=payload.get("updated_at"),
            )
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise FatalSourceError(f"malformed directory entry: {exc}") from exc
        if entry.size < 0:
            raise FatalSourceError("directory reports negative size")
        return entry

    def _absolute(self, url: str) -> str:
        if url.startswith(("http://", "https://")):
            return url
        return f"{self.settings.source_base_url}/{url.lstrip('/')}"

    def _identity_matches(self, row, entry: CatalogEntry) -> bool:
        return bool(
            row["expected_sha256"] == entry.sha256
            and row["expected_size"] == entry.size
            and row["source_etag"] == entry.etag
            and row["source_url"] == self._absolute(entry.url)
        )

    def _part_path(self, run: RunningTask, entry: CatalogEntry) -> Path:
        # The identity is embedded in the filename, so bytes written for one
        # version can never be mistaken for another version's partial data.
        tag = hashlib.sha256(
            f"{entry.etag}|{entry.size}|{entry.sha256}".encode()
        ).hexdigest()[:16]
        return self.settings.parts_dir / f"{run.task_id}-{tag}.part"

    # -- one attempt -----------------------------------------------------
    async def _attempt(self, run: RunningTask) -> None:
        row = self.store.get(run.task_id)
        if row is None or not self._alive(run):
            raise Interrupted()
        station, date = row["station"], row["date"]

        entry = await self._read_directory(run, station, date)
        if not self._alive(run):
            raise Interrupted()

        part = self._part_path(run, entry)
        same_identity = self._identity_matches(row, entry)
        if not same_identity:
            # Never stitch two revisions: drop partial data and stale metadata.
            # The previous publication (verified_* + package_path) is NOT
            # cleared: it stays downloadable until the replacement commits.
            self._discard_parts(run.task_id)
            self._update(
                run,
                source_url=self._absolute(entry.url),
                source_revision=entry.revision,
                source_etag=entry.etag,
                expected_size=entry.size,
                expected_sha256=entry.sha256,
                downloaded_bytes=0,
                part_path=str(part),
                error=None,
            )
            log.info("dropped stale partial for %s (revision %s)", run.task_id, entry.revision)
        else:
            self._update(
                run,
                source_revision=entry.revision,
                expected_size=entry.size,
                expected_sha256=entry.sha256,
                part_path=str(part),
            )
            self._discard_parts(run.task_id, keep=part)

        # Re-read the database for the authoritative published path.
        row = self.store.get(run.task_id)
        package = Path(row["package_path"]) if row["package_path"] else None
        if same_identity and package and package.exists():
            if self._verify(package, entry):
                # Refresh found the same revision already on disk: no transfer.
                self._publish_from_existing(run, entry, package)
                return

        offset = part.stat().st_size if part.exists() else 0
        if offset > entry.size:
            part.unlink(missing_ok=True)
            offset = 0
        if offset == entry.size and offset > 0:
            if self._verify(part, entry):
                self._publish(run, entry, part)
                return
            part.unlink(missing_ok=True)
            offset = 0

        self._update(run, downloaded_bytes=offset)
        if offset:
            self._note(run, "resume", f"from byte {offset} of {entry.size}")
        else:
            self._note(run, "download", f"0 of {entry.size}")

        await self._download(run, entry, part, offset)

    async def _download(self, run: RunningTask, entry: CatalogEntry, part: Path, offset: int) -> None:
        assert self._client is not None
        url = self._absolute(entry.url)
        headers = {"Range": f"bytes={offset}-"} if offset else {}
        if offset:
            headers["If-Range"] = entry.etag

        try:
            async with self._client.stream("GET", url, headers=headers) as response:
                if response.status_code in (412, 416):
                    raise Resync(f"source precondition rejected (HTTP {response.status_code})")
                if response.status_code not in (200, 206):
                    raise TransferError(f"package returned HTTP {response.status_code}")

                response_etag = response.headers.get("etag")
                if not response_etag:
                    raise Resync("source omitted ETag on package response")
                if response_etag != entry.etag:
                    raise Resync(f"ETag changed during request ({response_etag} != {entry.etag})")
                if offset and response.status_code == 200:
                    # The server ignored Range but confirmed the same ETag.  It is
                    # safe (and simpler) to restart this version from byte zero.
                    log.info("source ignored Range for %s; restarting from 0", run.task_id)
                    offset = 0
                    self._update(run, downloaded_bytes=0)
                    self._note(run, "range_ignored", "restarted from byte 0")

                if response.status_code == 206:
                    content_range = response.headers.get("content-range", "")
                    if not content_range.startswith(f"bytes {offset}-"):
                        raise Resync(f"unexpected Content-Range: {content_range!r}")
                    total = content_range.rsplit("/", 1)[-1]
                    if total.isdigit() and int(total) != entry.size:
                        raise Resync(f"Content-Range total {total} != directory size {entry.size}")

                # A 200 for a ranged request means the server ignored Range; the
                # ETag already matched, so truncate and write this version anew.
                mode = "wb" if offset == 0 else "ab"
                written = offset
                with open(part, mode) as handle:
                    async for chunk in response.aiter_bytes(self.settings.chunk_size):
                        if not self._alive(run):
                            self._flush(handle)
                            # Record the durable byte count only for the live
                            # generation; a paused task's stored progress is never
                            # advanced by a response that arrived late.
                            self.store.update_guarded(
                                run.task_id, run.generation,
                                downloaded_bytes=part.stat().st_size,
                            )
                            raise Interrupted()
                        handle.write(chunk)
                        written += len(chunk)
                        if written > entry.size:
                            self._flush(handle)
                            raise Resync("downloaded more than directory size")
                        self._update(run, downloaded_bytes=written)
                    self._flush(handle)
        except httpx.HTTPError as exc:
            raise TransferError(f"package request failed: {type(exc).__name__}: {exc}") from exc

        if not self._alive(run):
            raise Interrupted()
        actual = part.stat().st_size
        if actual != entry.size:
            raise Resync(f"body size {actual} != directory size {entry.size}")

        # Confirm the directory still advertises the identity we just fetched,
        # so a revision swapped in mid-transfer is never published.
        if not self._alive(run):
            raise Interrupted()
        latest = await self._read_directory(run, entry.station, entry.date)
        if (latest.sha256, latest.size, latest.etag) != (entry.sha256, entry.size, entry.etag):
            raise Resync("directory changed while downloading")
        self._publish(run, entry, part)

    @staticmethod
    def _flush(handle) -> None:
        handle.flush()
        os.fsync(handle.fileno())

    def _verify(self, path: Path, entry: CatalogEntry) -> bool:
        try:
            if path.stat().st_size != entry.size:
                return False
            digest = hashlib.sha256()
            with open(path, "rb") as handle:
                while chunk := handle.read(1024 * 1024):
                    digest.update(chunk)
            return digest.hexdigest() == entry.sha256
        except OSError:
            return False

    def _publish(self, run: RunningTask, entry: CatalogEntry, part: Path) -> None:
        if not self._verify(part, entry):
            part.unlink(missing_ok=True)
            raise Resync("size/SHA-256 verification failed against directory entry")
        if not self._alive(run):
            raise Interrupted()
        self._publish_from_existing(run, entry, part)

    def _publish_from_existing(self, run: RunningTask, entry: CatalogEntry, part: Path) -> None:
        row = self.store.get(run.task_id)
        station, date = row["station"], row["date"]
        final = self.settings.packages_dir / f"{station}-{date}-{entry.sha256[:12]}.tar.gz"

        # Revision impact, step 1: the new package's observations must parse
        # and validate (station/date/values) BEFORE anything is published.
        # A bad package fails the whole refresh and the old publication stays.
        new_observations = read_observations(part, station, date)

        # Step 2: when a previous validated package is being replaced, pin it
        # and diff both versions aligned by time.  The manifest must be
        # complete before the new package may be published at all.
        manifest: dict[str, Any] | None = None
        previous_sha = row["verified_sha256"]
        if previous_sha and previous_sha != entry.sha256:
            previous_path = row["package_path"]
            if not previous_path or not Path(previous_path).is_file():
                raise PackageError(
                    "previous validated package is missing; "
                    "cannot build a complete revision manifest"
                )
            previous_observations = read_observations(previous_path, station, date)
            manifest = {
                "station": station,
                "date": date,
                "previous_sha256": previous_sha,
                "current_sha256": entry.sha256,
                "previous_revision": row["published_revision"],
                "current_revision": entry.revision,
                "changes": diff_observations(previous_observations, new_observations),
            }

        fd, tmp_name = tempfile.mkstemp(prefix=".publish-", dir=self.settings.packages_dir)
        tmp = Path(tmp_name)
        try:
            with open(part, "rb") as src, os.fdopen(fd, "wb") as dst:
                while chunk := src.read(1024 * 1024):
                    dst.write(chunk)
                dst.flush()
                os.fsync(dst.fileno())
            os.replace(tmp, final)
        except BaseException:
            tmp.unlink(missing_ok=True)
            raise

        # Step 3: mark the task completed AND append the manifest in a single
        # transaction, so the package and its revision list are always seen as
        # one publication — before, during and after any restart.
        try:
            self.store.complete_publication(
                run.task_id,
                run.generation,
                manifest=manifest,
                part_path=None,
                package_path=str(final),
                downloaded_bytes=entry.size,
                verified_size=entry.size,
                verified_sha256=entry.sha256,
                published_revision=entry.revision,
                error=None,
                completed_at=utcnow(),
            )
        except CasConflict:
            # A pause (or a newer generation) won the race: remove the artifact
            # and keep the part file so the task is never falsely completed and
            # never exposes an unverified/half package.  When the target path IS
            # the currently published package (unchanged-source republish), the
            # file must survive — the publication is still in force.
            if str(final) != row["package_path"]:
                final.unlink(missing_ok=True)
            raise Interrupted()

        if part != final:
            part.unlink(missing_ok=True)
        for old in self.settings.packages_dir.glob(f"{station}-{date}-*.tar.gz"):
            if old != final:
                try:
                    old.unlink(missing_ok=True)
                except OSError:
                    # A reader may hold the file open; a leftover superseded
                    # package is harmless and cleaned up by the next publish.
                    log.warning("could not remove superseded package %s", old, exc_info=True)
        self.store.event(run.task_id, "verified", f"sha256={entry.sha256} size={entry.size}")

    def _discard_parts(self, task_id: str, keep: Path | None = None) -> None:
        for stale in self.settings.parts_dir.glob(f"{task_id}-*.part"):
            if keep is None or stale != keep:
                stale.unlink(missing_ok=True)
