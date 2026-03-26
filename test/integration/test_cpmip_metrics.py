# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit. If not, see <http://www.gnu.org/licenses/>.

from types import SimpleNamespace

from autosubmit.metrics.cpmip_metrics import CPMIPMetrics


def test_cpmip_evaluate_computes_bounds_and_violations_end_to_end():
    job = SimpleNamespace(
        runtime=12.0,
        chunk_size=1,
        chunk_size_unit="year",
        total_processors=240,
    )
    thresholds = {
        "SYPD": {
            "THRESHOLD": 5.0,
            "COMPARISON": "greater_than",
            "%_ACCEPTED_ERROR": 10,
        }
    }

    violations = CPMIPMetrics.evaluate(job, thresholds)

    assert "SYPD" in violations
    assert violations["SYPD"]["comparison"] == "greater_than"
    assert violations["SYPD"]["threshold"] == 5.0
    assert violations["SYPD"]["accepted_error"] == 10.0
    assert violations["SYPD"]["bound"] == 4.5
    # SYPD = 1 * 24 / 12 = 2.0
    assert violations["SYPD"]["real_value"] == 2.0


def test_cpmip_evaluate_returns_empty_when_metric_within_tolerance():
    job = SimpleNamespace(
        runtime=6.0,
        chunk_size=1,
        chunk_size_unit="year",
        total_processors=240,
    )
    thresholds = {
        "SYPD": {
            "THRESHOLD": 3.0,
            "COMPARISON": "greater_than",
            "%_ACCEPTED_ERROR": 0,
        }
    }

    violations = CPMIPMetrics.evaluate(job, thresholds)

    # SYPD = 4.0, no violation for greater_than threshold=3.0
    assert violations == {}


def test_cpmip_fetch_metrics_requires_canonical_chunk_fields():
    job = SimpleNamespace(
        runtime=24.0,
        chunksize=365,
        chunksizeunit="day",
        total_processors=120,
    )

    metrics = CPMIPMetrics._fetch_metrics(job)

    assert metrics == {}


def test_cpmip_fetch_metrics_returns_empty_for_missing_metadata():
    job = SimpleNamespace(
        runtime=24.0,
        chunk_size=1,
        total_processors=120,
    )

    metrics = CPMIPMetrics._fetch_metrics(job)

    assert metrics == {}


def test_cpmip_evaluate_computes_chsy_violation_end_to_end():
    job = SimpleNamespace(
        runtime=12.0,
        chunk_size=1,
        chunk_size_unit="year",
        total_processors=240,
    )
    thresholds = {
        "CHSY": {
            "THRESHOLD": 2000.0,
            "COMPARISON": "less_than",
            "%_ACCEPTED_ERROR": 5,
        }
    }

    violations = CPMIPMetrics.evaluate(job, thresholds)

    assert "CHSY" in violations
    assert violations["CHSY"]["comparison"] == "less_than"
    assert violations["CHSY"]["threshold"] == 2000.0
    assert violations["CHSY"]["accepted_error"] == 5.0
    assert violations["CHSY"]["bound"] == 2100.0
    # CHSY = (240 * 12) / 1 = 2880
    assert violations["CHSY"]["real_value"] == 2880.0


def test_cpmip_evaluate_chsy_boundary_is_not_violation_for_less_than():
    job = SimpleNamespace(
        runtime=12.0,
        chunk_size=1,
        chunk_size_unit="year",
        total_processors=240,
    )
    thresholds = {
        "CHSY": {
            "THRESHOLD": 2880.0,
            "COMPARISON": "less_than",
            "%_ACCEPTED_ERROR": 0,
        }
    }

    violations = CPMIPMetrics.evaluate(job, thresholds)

    assert violations == {}
