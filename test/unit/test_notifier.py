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

from autosubmit.notifications.notifier import Notifier


class DummyImplementation:
    def __init__(self):
        self.calls = []

    def notify_cpmip_threshold_violations(self, exp_id, job_name, violations, notify_to):
        self.calls.append((exp_id, job_name, violations, notify_to))


def test_notifier_skips_cpmip_dispatch_when_violations_empty():
    implementation = DummyImplementation()

    Notifier.notify_cpmip_threshold_violations(
        implementation,
        "a000",
        "a000_SIM",
        {},
        ["user@example.com"],
    )

    assert implementation.calls == []


def test_notifier_dispatches_cpmip_when_violations_present():
    implementation = DummyImplementation()
    violations = {
        "SYPD": {
            "threshold": 5.0,
            "accepted_error": 10,
            "comparison": "greater_than",
            "bound": 4.5,
            "real_value": 3.9,
        }
    }

    Notifier.notify_cpmip_threshold_violations(
        implementation,
        "a000",
        "a000_SIM",
        violations,
        ["user@example.com"],
    )

    assert len(implementation.calls) == 1
    assert implementation.calls[0][2] == violations
