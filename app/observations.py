"""Parse, validate and diff ``observations.csv`` inside station/date packages.

A package is a ``.tar.gz`` carrying one ``observations.csv`` member.  Rows are
aligned by their ``time`` column; when several rows share a time the LAST row
wins.  Any malformed content — wrong station, wrong date, bad time or a
non-numeric/non-finite value — raises :class:`PackageError`, which fails the
whole refresh: a partially trusted package is never published.
"""

from __future__ import annotations

import csv
import io
import math
import re
import tarfile
from pathlib import Path
from typing import Any

VALUE_FIELDS = ("temperature_c", "pressure_hpa", "wind_mps")
REQUIRED_COLUMNS = frozenset({"station", "date", "time", *VALUE_FIELDS})
TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
MAX_ROWS = 200_000
MEMBER_NAMES = ("observations.csv", "./observations.csv")

# A single observation row, keyed by field name.
Observation = dict[str, float]
# Aligned observations: "HH:MM" -> observation.
ObservationTable = dict[str, Observation]


class PackageError(Exception):
    """The package cannot be parsed or its observations fail validation."""


def _csv_member(tar: tarfile.TarFile) -> tarfile.TarInfo:
    for name in MEMBER_NAMES:
        try:
            return tar.getmember(name)
        except KeyError:
            continue
    raise PackageError("observations.csv not found in package")


def read_observations(package_path: str | Path, station: str, date: str) -> ObservationTable:
    """Return ``{time: {field: value}}`` for a validated package.

    Duplicate times resolve to the last row in file order.  Raises
    :class:`PackageError` on any structural or content problem.
    """
    path = Path(package_path)
    try:
        with tarfile.open(path, "r:gz") as tar:
            member = _csv_member(tar)
            extracted = tar.extractfile(member)
            if extracted is None:
                raise PackageError("observations.csv is not a regular file")
            with extracted:
                text = io.TextIOWrapper(extracted, encoding="utf-8", newline="")
                reader = csv.DictReader(text)
                missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
                if missing:
                    raise PackageError(
                        f"observations.csv missing columns: {', '.join(sorted(missing))}"
                    )
                observations: ObservationTable = {}
                for lineno, row in enumerate(reader, start=2):
                    if lineno - 1 > MAX_ROWS:
                        raise PackageError(f"observations.csv exceeds {MAX_ROWS} rows")
                    row_station = (row.get("station") or "").strip()
                    if row_station != station:
                        raise PackageError(
                            f"row {lineno}: station {row_station!r} does not match {station!r}"
                        )
                    row_date = (row.get("date") or "").strip()
                    if row_date != date:
                        raise PackageError(
                            f"row {lineno}: date {row_date!r} does not match {date!r}"
                        )
                    moment = (row.get("time") or "").strip()
                    if not TIME_RE.fullmatch(moment):
                        raise PackageError(f"row {lineno}: invalid time {moment!r}")
                    values: Observation = {}
                    for field in VALUE_FIELDS:
                        raw = (row.get(field) or "").strip()
                        try:
                            value = float(raw)
                        except ValueError:
                            raise PackageError(
                                f"row {lineno}: {field} {raw!r} is not a number"
                            ) from None
                        if not math.isfinite(value):
                            raise PackageError(
                                f"row {lineno}: {field} {raw!r} is not finite"
                            )
                        values[field] = value
                    # Same-time rows: the last one read wins.
                    observations[moment] = values
                return observations
    except (tarfile.TarError, OSError, EOFError, UnicodeDecodeError, csv.Error) as exc:
        raise PackageError(f"cannot parse {path.name}: {type(exc).__name__}: {exc}") from exc


def diff_observations(
    previous: ObservationTable, current: ObservationTable
) -> list[dict[str, Any]]:
    """Align two tables by time and list added/withdrawn/changed moments.

    Each change carries the moment, its kind, and the before/after values so
    downstream consumers know exactly which readings must be recomputed.
    """
    changes: list[dict[str, Any]] = []
    for moment in sorted(set(previous) | set(current)):
        before = previous.get(moment)
        after = current.get(moment)
        if before is None:
            changes.append({"time": moment, "kind": "added", "after": after})
        elif after is None:
            changes.append({"time": moment, "kind": "withdrawn", "before": before})
        else:
            fields = [f for f in VALUE_FIELDS if before[f] != after[f]]
            if fields:
                changes.append(
                    {
                        "time": moment,
                        "kind": "changed",
                        "before": before,
                        "after": after,
                        "fields": fields,
                    }
                )
    return changes
