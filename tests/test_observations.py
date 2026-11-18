"""Unit tests for observations.csv parsing, validation and diffing."""

from __future__ import annotations

import io
import tarfile

import pytest

from app.observations import PackageError, diff_observations, read_observations

HEADER = "station,date,time,temperature_c,pressure_hpa,wind_mps"


def make_package(tmp_path, csv_text: str, name: str = "observations.csv"):
    path = tmp_path / "pkg.tar.gz"
    data = csv_text.encode()
    with tarfile.open(path, "w:gz") as tar:
        info = tarfile.TarInfo(name)
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    return path


def test_parse_valid_rows(tmp_path):
    pkg = make_package(
        tmp_path,
        f"{HEADER}\n"
        "54511,2026-11-17,00:00,10.5,1000.2,3.4\n"
        "54511,2026-11-17,00:15,-2.25,999.0,0.0\n",
    )
    table = read_observations(pkg, "54511", "2026-11-17")
    assert table == {
        "00:00": {"temperature_c": 10.5, "pressure_hpa": 1000.2, "wind_mps": 3.4},
        "00:15": {"temperature_c": -2.25, "pressure_hpa": 999.0, "wind_mps": 0.0},
    }


def test_same_time_last_row_wins(tmp_path):
    pkg = make_package(
        tmp_path,
        f"{HEADER}\n"
        "54511,2026-11-17,06:00,1.0,1000.0,1.0\n"
        "54511,2026-11-17,06:00,2.0,1001.0,2.0\n"
        "54511,2026-11-17,06:00,3.0,1002.0,3.0\n",
    )
    table = read_observations(pkg, "54511", "2026-11-17")
    assert table == {"06:00": {"temperature_c": 3.0, "pressure_hpa": 1002.0, "wind_mps": 3.0}}


def test_dot_slash_member_name_accepted(tmp_path):
    pkg = make_package(tmp_path, f"{HEADER}\n54511,2026-11-17,01:00,1,2,3\n", name="./observations.csv")
    assert read_observations(pkg, "54511", "2026-11-17")["01:00"]["temperature_c"] == 1.0


@pytest.mark.parametrize(
    "csv_text,fragment",
    [
        # wrong station in a data row
        (f"{HEADER}\n99999,2026-11-17,00:00,1,2,3\n", "station"),
        # wrong date in a data row
        (f"{HEADER}\n54511,2020-01-01,00:00,1,2,3\n", "date"),
        # non-numeric temperature
        (f"{HEADER}\n54511,2026-11-17,00:00,abc,2,3\n", "not a number"),
        # non-finite value
        (f"{HEADER}\n54511,2026-11-17,00:00,1,inf,3\n", "not finite"),
        # empty value
        (f"{HEADER}\n54511,2026-11-17,00:00,1,,3\n", "not a number"),
        # bad time
        (f"{HEADER}\n54511,2026-11-17,25:00,1,2,3\n", "invalid time"),
        (f"{HEADER}\n54511,2026-11-17,6:0,1,2,3\n", "invalid time"),
        # missing column
        ("station,date,time,temperature_c,wind_mps\n54511,2026-11-17,00:00,1,3\n", "missing columns"),
    ],
)
def test_invalid_content_rejected(tmp_path, csv_text, fragment):
    pkg = make_package(tmp_path, csv_text)
    with pytest.raises(PackageError, match=fragment):
        read_observations(pkg, "54511", "2026-11-17")


def test_missing_csv_member_rejected(tmp_path):
    pkg = make_package(tmp_path, "hello\n", name="README.txt")
    with pytest.raises(PackageError, match="observations.csv not found"):
        read_observations(pkg, "54511", "2026-11-17")


def test_not_a_gzip_rejected(tmp_path):
    path = tmp_path / "broken.tar.gz"
    path.write_bytes(b"this is not a gzip stream")
    with pytest.raises(PackageError):
        read_observations(path, "54511", "2026-11-17")


def test_missing_file_rejected(tmp_path):
    with pytest.raises(PackageError):
        read_observations(tmp_path / "nope.tar.gz", "54511", "2026-11-17")


def _obs(temp: float, pres: float = 1000.0, wind: float = 1.0):
    return {"temperature_c": temp, "pressure_hpa": pres, "wind_mps": wind}


def test_diff_added_withdrawn_changed():
    previous = {
        "00:00": _obs(10.0),
        "00:15": _obs(11.0),
        "00:30": _obs(12.0, pres=1001.0, wind=7.0),
    }
    current = {
        "00:00": _obs(10.0),                      # unchanged
        "00:30": _obs(12.0, pres=1002.5, wind=7.0),  # pressure changed
        "01:00": _obs(13.0),                      # added
    }
    changes = diff_observations(previous, current)
    assert changes == [
        {"time": "00:15", "kind": "withdrawn", "before": _obs(11.0)},
        {
            "time": "00:30",
            "kind": "changed",
            "before": _obs(12.0, pres=1001.0, wind=7.0),
            "after": _obs(12.0, pres=1002.5, wind=7.0),
            "fields": ["pressure_hpa"],
        },
        {"time": "01:00", "kind": "added", "after": _obs(13.0)},
    ]


def test_diff_multiple_fields_changed():
    changes = diff_observations(
        {"08:00": _obs(1.0, pres=1000.0, wind=2.0)},
        {"08:00": _obs(2.0, pres=1000.0, wind=5.0)},
    )
    assert changes[0]["fields"] == ["temperature_c", "wind_mps"]


def test_diff_empty_when_identical():
    table = {"00:00": _obs(10.0), "23:45": _obs(-1.0)}
    assert diff_observations(table, dict(table)) == []


def test_diff_sorted_by_time():
    changes = diff_observations(
        {},
        {"23:00": _obs(1.0), "00:30": _obs(2.0), "06:15": _obs(3.0)},
    )
    assert [c["time"] for c in changes] == ["00:30", "06:15", "23:00"]
