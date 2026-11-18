"""End-to-end tests for the revision-impact API.

A refresh that replaces a published package must publish the new package and
its revision manifest atomically; failures, interruptions and restarts must
keep the previous package downloadable together with the previous manifest.
"""

from __future__ import annotations

import asyncio
import hashlib
import time

import httpx
import pytest

from app.collector import Collector
from app.config import Settings
from app.main import create_app
from mock_source.app import app as source_app, state as source_state
from tests.conftest import free_port, run_server

HEADER = "station,date,time,temperature_c,pressure_hpa,wind_mps"


class Stack:
    def __init__(self, source_url: str, api_url: str):
        self.source_url = source_url
        self.api_url = api_url


async def _wait(client: httpx.AsyncClient, api: str, task_id: str, states: set[str], timeout: float = 60):
    deadline = time.monotonic() + timeout
    view = {}
    while time.monotonic() < deadline:
        view = (await client.get(f"{api}/tasks/{task_id}")).json()
        if view["state"] in states:
            return view
        await asyncio.sleep(0.05)
    raise AssertionError(f"timeout; last={view}")


@pytest.fixture
async def stack(tmp_path):
    source_state.revisions.clear()
    source_state.overrides.clear()
    source_state.delay_seconds = 0.0
    source_state.chunk_delay_seconds = 0.0
    source_state.failing.clear()
    source_state.corrupting.clear()
    source_state.no_range.clear()
    source_state.requests.clear()

    source_port = free_port()
    api_port = free_port()
    settings = Settings(
        source_base_url=f"http://127.0.0.1:{source_port}",
        data_dir=tmp_path / "data",
        http_timeout=15,
        chunk_size=8 * 1024,
        max_source_resyncs=3,
    )
    collector = Collector(settings)
    api_app = create_app(settings, collector)
    async with run_server(source_app, source_port):
        async with run_server(api_app, api_port):
            async with httpx.AsyncClient(timeout=30) as client:
                yield Stack(f"http://127.0.0.1:{source_port}", f"http://127.0.0.1:{api_port}"), client, settings, collector


async def _completed_task(client: httpx.AsyncClient, api: str, station: str, date: str) -> dict:
    task_id = (await client.post(f"{api}/tasks", json={"station": station, "date": date})).json()["id"]
    await client.post(f"{api}/tasks/{task_id}/start")
    view = await _wait(client, api, task_id, {"completed", "failed"})
    assert view["state"] == "completed", view
    return view


async def _set_csv(client: httpx.AsyncClient, source: str, station: str, date: str, revision: str, csv_text: str):
    resp = await client.post(
        f"{source}/admin/package",
        json={"station": station, "date": date, "revision": revision, "csv_text": csv_text},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()["entry"]


async def _revisions(client: httpx.AsyncClient, api: str, task_id: str) -> list[dict]:
    resp = await client.get(f"{api}/tasks/{task_id}/revisions")
    assert resp.status_code == 200, resp.text
    return resp.json()["revisions"]


async def _package_sha(client: httpx.AsyncClient, api: str, task_id: str) -> str:
    resp = await client.get(f"{api}/tasks/{task_id}/package")
    assert resp.status_code == 200, resp.text
    return hashlib.sha256(resp.content).hexdigest()


async def test_refresh_publishes_revision_manifest(stack):
    st, client, _settings, _collector = stack
    source, api = st.source_url, st.api_url
    station, date = "54511", "2026-11-17"
    first = await _completed_task(client, api, station, date)
    task_id = first["id"]
    assert first["latest_revision_seq"] is None
    assert first["revisions_url"].endswith(f"/tasks/{task_id}/revisions")
    assert await _revisions(client, api, task_id) == []

    await client.post(f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v2"})
    await client.post(f"{api}/tasks/{task_id}/refresh")
    second = await _wait(client, api, task_id, {"completed", "failed"})
    assert second["state"] == "completed", second
    assert second["verified_sha256"] != first["verified_sha256"]

    manifests = await _revisions(client, api, task_id)
    assert len(manifests) == 1
    manifest = manifests[0]
    assert manifest["seq"] == 1
    assert manifest["station"] == station and manifest["date"] == date
    assert manifest["previous_sha256"] == first["verified_sha256"]
    assert manifest["current_sha256"] == second["verified_sha256"]
    assert manifest["previous_revision"] == "v1"
    assert manifest["current_revision"] == "v2"
    # The mock applies deterministic corrections: 3 added + 3 withdrawn moments.
    assert manifest["added"] == 3
    assert manifest["withdrawn"] == 3
    assert manifest["added"] + manifest["withdrawn"] + manifest["changed"] == len(manifest["changes"])
    assert manifest["changes"], manifest
    times = [c["time"] for c in manifest["changes"]]
    assert times == sorted(times)
    assert {c["kind"] for c in manifest["changes"]} <= {"added", "withdrawn", "changed"}
    for change in manifest["changes"]:
        if change["kind"] == "added":
            assert change["after"] and change["before"] is None
        elif change["kind"] == "withdrawn":
            assert change["before"] and change["after"] is None
        else:
            assert change["before"] and change["after"] and change["fields"]

    view = (await client.get(f"{api}/tasks/{task_id}")).json()
    assert view["latest_revision_seq"] == 1
    events = [e["event"] for e in (await client.get(f"{api}/tasks/{task_id}/events")).json()]
    assert "revision_manifest" in events


async def test_manifest_matches_explicit_csv_changes(stack):
    st, client, _settings, _collector = stack
    source, api = st.source_url, st.api_url
    station, date = "S1", "2026-11-10"
    v1_csv = (
        f"{HEADER}\n"
        f"{station},{date},00:00,10.0,1000.0,5.0\n"
        f"{station},{date},00:15,11.0,1001.0,6.0\n"
        f"{station},{date},00:30,12.0,1002.0,7.0\n"
        f"{station},{date},00:30,12.5,1002.5,7.5\n"  # duplicate time: last row wins
        f"{station},{date},00:45,13.0,1003.0,8.0\n"
    )
    v2_csv = (
        f"{HEADER}\n"
        f"{station},{date},00:00,10.0,1000.0,5.0\n"      # unchanged
        f"{station},{date},00:15,11.9,1001.6,6.0\n"      # temperature + pressure changed
        f"{station},{date},00:30,12.5,1002.5,7.5\n"      # unchanged (v1 last row)
        f"{station},{date},01:00,14.0,1004.0,9.0\n"      # added; 00:45 withdrawn
    )
    entry_v1 = await _set_csv(client, source, station, date, "v1", v1_csv)
    first = await _completed_task(client, api, station, date)
    task_id = first["id"]
    assert first["verified_sha256"] == entry_v1["sha256"]

    entry_v2 = await _set_csv(client, source, station, date, "v2", v2_csv)
    await client.post(f"{api}/tasks/{task_id}/refresh")
    second = await _wait(client, api, task_id, {"completed", "failed"})
    assert second["state"] == "completed", second
    assert second["verified_sha256"] == entry_v2["sha256"]

    manifests = await _revisions(client, api, task_id)
    assert len(manifests) == 1
    manifest = manifests[0]
    assert manifest["previous_sha256"] == entry_v1["sha256"]
    assert manifest["current_sha256"] == entry_v2["sha256"]
    assert (manifest["added"], manifest["withdrawn"], manifest["changed"]) == (1, 1, 1)
    assert manifest["changes"] == [
        {
            "time": "00:15",
            "kind": "changed",
            "before": {"temperature_c": 11.0, "pressure_hpa": 1001.0, "wind_mps": 6.0},
            "after": {"temperature_c": 11.9, "pressure_hpa": 1001.6, "wind_mps": 6.0},
            "fields": ["temperature_c", "pressure_hpa"],
        },
        {
            "time": "00:45",
            "kind": "withdrawn",
            "before": {"temperature_c": 13.0, "pressure_hpa": 1003.0, "wind_mps": 8.0},
            "after": None,
            "fields": [],
        },
        {
            "time": "01:00",
            "kind": "added",
            "before": None,
            "after": {"temperature_c": 14.0, "pressure_hpa": 1004.0, "wind_mps": 9.0},
            "fields": [],
        },
    ]

    # Individual manifests are addressable; latest points at the same one.
    by_seq = await client.get(f"{api}/tasks/{task_id}/revisions/1")
    assert by_seq.status_code == 200
    assert by_seq.json()["current_sha256"] == entry_v2["sha256"]
    latest = await client.get(f"{api}/tasks/{task_id}/revisions/latest")
    assert latest.status_code == 200
    assert latest.json()["seq"] == 1
    missing = await client.get(f"{api}/tasks/{task_id}/revisions/99")
    assert missing.status_code == 404


async def test_unchanged_source_adds_no_manifest(stack):
    st, client, _settings, _collector = stack
    api = st.api_url
    view = await _completed_task(client, api, "SAME", "2026-11-12")
    task_id = view["id"]
    assert await _revisions(client, api, task_id) == []
    latest = await client.get(f"{api}/tasks/{task_id}/revisions/latest")
    assert latest.status_code == 404

    for _ in range(2):
        await client.post(f"{api}/tasks/{task_id}/refresh")
        done = await _wait(client, api, task_id, {"completed", "failed"})
        assert done["state"] == "completed", done
        assert done["verified_sha256"] == view["verified_sha256"]
        assert await _revisions(client, api, task_id) == []
        assert done["latest_revision_seq"] is None


async def test_empty_diff_still_publishes_manifest(stack):
    st, client, _settings, _collector = stack
    source, api = st.source_url, st.api_url
    station, date = "ED", "2026-11-13"
    csv_text = f"{HEADER}\n{station},{date},08:00,4.5,1001.0,2.5\n"
    await _set_csv(client, source, station, date, "v1", csv_text)
    first = await _completed_task(client, api, station, date)
    task_id = first["id"]

    # Same observations, different revision label: the package bytes change
    # but no reading does — the manifest must say exactly that.
    await _set_csv(client, source, station, date, "v2", csv_text)
    await client.post(f"{api}/tasks/{task_id}/refresh")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    assert done["verified_sha256"] != first["verified_sha256"]
    manifests = await _revisions(client, api, task_id)
    assert len(manifests) == 1
    assert manifests[0]["changes"] == []
    assert (manifests[0]["added"], manifests[0]["withdrawn"], manifests[0]["changed"]) == (0, 0, 0)


@pytest.mark.parametrize(
    "bad_row,fragment",
    [
        ("WRONG,{date},00:15,1.0,1000.0,1.0", "station"),
        ("{station},2020-01-01,00:15,1.0,1000.0,1.0", "date"),
        ("{station},{date},00:15,hot,1000.0,1.0", "not a number"),
    ],
)
async def test_invalid_csv_fails_refresh_and_keeps_old_publication(stack, bad_row, fragment):
    st, client, settings, _collector = stack
    source, api = st.source_url, st.api_url
    station, date = "VAL", "2026-11-14"
    v1_csv = f"{HEADER}\n{station},{date},00:00,10.0,1000.0,5.0\n"
    await _set_csv(client, source, station, date, "v1", v1_csv)
    first = await _completed_task(client, api, station, date)
    task_id = first["id"]

    bad_csv = (
        f"{HEADER}\n"
        f"{station},{date},00:00,10.0,1000.0,5.0\n"
        + bad_row.format(station=station, date=date)
        + "\n"
    )
    await _set_csv(client, source, station, date, "v2", bad_csv)
    await client.post(f"{api}/tasks/{task_id}/refresh")
    failed = await _wait(client, api, task_id, {"completed", "failed"})
    assert failed["state"] == "failed", failed
    assert fragment in (failed["error"] or "")

    # The whole refresh failed: the previous validated package is still served
    # and no manifest appeared.  Nothing half-published on disk either.
    assert await _package_sha(client, api, task_id) == first["verified_sha256"]
    assert await _revisions(client, api, task_id) == []
    view = (await client.get(f"{api}/tasks/{task_id}")).json()
    assert view["verified_sha256"] == first["verified_sha256"]
    assert view["download_url"]

    # A corrected package can be picked up by a plain retry.
    v3_csv = f"{HEADER}\n{station},{date},00:00,10.5,1000.0,5.0\n"
    await _set_csv(client, source, station, date, "v3", v3_csv)
    await client.post(f"{api}/tasks/{task_id}/start")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    manifests = await _revisions(client, api, task_id)
    assert len(manifests) == 1
    assert manifests[0]["previous_sha256"] == first["verified_sha256"]
    assert manifests[0]["current_sha256"] == done["verified_sha256"]
    assert manifests[0]["changes"][0]["kind"] == "changed"


async def test_source_outage_during_refresh_keeps_old_package_and_manifest(stack):
    st, client, _settings, _collector = stack
    source, api = st.source_url, st.api_url
    station, date = "OUT2", "2026-11-15"
    first = await _completed_task(client, api, station, date)
    task_id = first["id"]
    await client.post(f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v2"})
    await client.post(f"{api}/tasks/{task_id}/refresh")
    second = await _wait(client, api, task_id, {"completed", "failed"})
    assert second["state"] == "completed"
    manifests = await _revisions(client, api, task_id)
    assert len(manifests) == 1

    # Source dies during the NEXT refresh: old package + old manifest stay.
    await client.post(f"{source}/admin/fault", json={"station": station, "date": date, "mode": "fail"})
    await client.post(f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v3"})
    await client.post(f"{api}/tasks/{task_id}/refresh")
    failed = await _wait(client, api, task_id, {"completed", "failed"})
    assert failed["state"] == "failed", failed
    assert "503" in (failed["error"] or "")
    assert await _package_sha(client, api, task_id) == second["verified_sha256"]
    assert await _revisions(client, api, task_id) == manifests

    # Recovery republishes the pending revision together with its manifest.
    await client.post(f"{source}/admin/fault", json={"station": station, "date": date, "mode": "clear"})
    await client.post(f"{api}/tasks/{task_id}/start")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    assert done["verified_sha256"] != second["verified_sha256"]
    after = await _revisions(client, api, task_id)
    assert len(after) == 2
    assert after[0] == manifests[0]  # previous manifest untouched
    assert after[1]["previous_sha256"] == second["verified_sha256"]
    assert after[1]["current_sha256"] == done["verified_sha256"]
    assert await _package_sha(client, api, task_id) == done["verified_sha256"]


async def test_old_package_downloadable_during_refresh(stack):
    st, client, _settings, _collector = stack
    source, api = st.source_url, st.api_url
    station, date = "DURING", "2026-11-16"
    first = await _completed_task(client, api, station, date)
    task_id = first["id"]

    await client.post(f"{source}/admin/delay", json={"seconds": 0, "chunk_seconds": 0.05})
    await client.post(f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v2"})
    started = await client.post(f"{api}/tasks/{task_id}/refresh")
    assert started.json()["state"] == "running"

    # While the new version is still being transferred the OLD package is served.
    deadline = time.monotonic() + 10
    saw_running_download = False
    while time.monotonic() < deadline:
        view = (await client.get(f"{api}/tasks/{task_id}")).json()
        if view["state"] != "running":
            break
        package = await client.get(f"{api}/tasks/{task_id}/package")
        assert package.status_code == 200
        assert hashlib.sha256(package.content).hexdigest() == first["verified_sha256"]
        assert view["download_url"]
        saw_running_download = True
        await asyncio.sleep(0.05)
    assert saw_running_download

    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    assert await _package_sha(client, api, task_id) == done["verified_sha256"]
    assert len(await _revisions(client, api, task_id)) == 1


async def test_interleaved_queries_see_consistent_publication(stack):
    st, client, _settings, _collector = stack
    source, api = st.source_url, st.api_url
    station, date = "RACE", "2026-11-18"
    await _completed_task(client, api, station, date)
    task_id = (await client.get(f"{api}/tasks")).json()[0]["id"]
    await client.post(f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v2"})
    await client.post(f"{api}/tasks/{task_id}/refresh")
    second = await _wait(client, api, task_id, {"completed", "failed"})
    assert second["state"] == "completed"
    manifests_v2 = await _revisions(client, api, task_id)
    assert len(manifests_v2) == 1

    # Refresh to v3 slowly; poll the list and the package while it runs.
    await client.post(f"{source}/admin/delay", json={"seconds": 0, "chunk_seconds": 0.05})
    await client.post(f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v3"})
    await client.post(f"{api}/tasks/{task_id}/refresh")

    known_shas = {second["verified_sha256"]}
    polls = 0
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        view = (await client.get(f"{api}/tasks/{task_id}")).json()
        if view["state"] != "running":
            break
        seen = await _revisions(client, api, task_id)
        # Every list seen is a prefix of the final history: manifests are never
        # mutated or reordered, only appended by a committed publication.
        assert [m["seq"] for m in seen] == list(range(1, len(seen) + 1))
        assert seen[: len(manifests_v2)] == manifests_v2
        # The package served at any moment belongs to one committed publication.
        package = await client.get(f"{api}/tasks/{task_id}/package")
        assert package.status_code == 200
        known_shas.add(hashlib.sha256(package.content).hexdigest())
        polls += 1
        await asyncio.sleep(0.03)
    assert polls > 0

    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    final = await _revisions(client, api, task_id)
    assert len(final) == 2
    assert final[0] == manifests_v2[0]
    assert final[1]["current_sha256"] == done["verified_sha256"]
    assert done["latest_revision_seq"] == 2
    assert known_shas <= {m["current_sha256"] for m in final} | {second["verified_sha256"]}
    assert await _package_sha(client, api, task_id) == done["verified_sha256"]


async def test_pause_during_refresh_keeps_old_publication(stack):
    st, client, _settings, _collector = stack
    source, api = st.source_url, st.api_url
    station, date = "PAUSE", "2026-11-20"
    first = await _completed_task(client, api, station, date)
    task_id = first["id"]

    await client.post(f"{source}/admin/delay", json={"seconds": 0, "chunk_seconds": 0.05})
    await client.post(f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v2"})
    await client.post(f"{api}/tasks/{task_id}/refresh")
    view = (await client.get(f"{api}/tasks/{task_id}")).json()
    assert view["state"] == "running"

    paused = (await client.post(f"{api}/tasks/{task_id}/pause")).json()
    assert paused["state"] == "paused"
    # Interrupted refresh: old package and (empty) manifest list are intact.
    assert await _package_sha(client, api, task_id) == first["verified_sha256"]
    assert await _revisions(client, api, task_id) == []

    await client.post(f"{source}/admin/delay", json={"seconds": 0, "chunk_seconds": 0})
    await client.post(f"{api}/tasks/{task_id}/resume")
    done = await _wait(client, api, task_id, {"completed", "failed"})
    assert done["state"] == "completed", done
    manifests = await _revisions(client, api, task_id)
    assert len(manifests) == 1
    assert manifests[0]["previous_sha256"] == first["verified_sha256"]
    assert manifests[0]["current_sha256"] == done["verified_sha256"]


async def test_restart_during_refresh_keeps_consistent_state(stack):
    st, client, settings, collector = stack
    source, api = st.source_url, st.api_url
    station, date = "RST", "2026-11-19"
    first = await _completed_task(client, api, station, date)
    task_id = first["id"]

    await client.post(f"{source}/admin/delay", json={"seconds": 0, "chunk_seconds": 0.05})
    await client.post(f"{source}/admin/revision", json={"station": station, "date": date, "revision": "v2"})
    await client.post(f"{api}/tasks/{task_id}/refresh")
    view = (await client.get(f"{api}/tasks/{task_id}")).json()
    assert view["state"] == "running"

    # Simulate a process restart mid-refresh.
    await collector.shutdown()
    fresh = Collector(settings)
    await fresh.startup()
    try:
        paused = (await client.get(f"{api}/tasks/{task_id}")).json()
        assert paused["state"] == "paused"
        # No half publication: old package + old (empty) manifest list survive.
        assert paused["verified_sha256"] == first["verified_sha256"]
        assert await _package_sha(client, api, task_id) == first["verified_sha256"]
        assert await _revisions(client, api, task_id) == []

        await client.post(f"{source}/admin/delay", json={"seconds": 0, "chunk_seconds": 0})
        await fresh.start(task_id)
        done = await _wait(client, api, task_id, {"completed", "failed"})
        assert done["state"] == "completed", done
        manifests = await _revisions(client, api, task_id)
        assert len(manifests) == 1
        assert manifests[0]["previous_sha256"] == first["verified_sha256"]
        assert manifests[0]["current_sha256"] == done["verified_sha256"]
        assert await _package_sha(client, api, task_id) == done["verified_sha256"]
    finally:
        await fresh.shutdown()
