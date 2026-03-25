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

from autosubmit.autosubmit import Autosubmit
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.job.job_common import Status
from autosubmit.notifications.mail_notifier import MailNotifier


def test_notify_cpmip_threshold_violations_sends_email_with_computed_metrics(monkeypatch):
    sent_messages = []

    def fake_send_mail(self, mail_from, mail_to, message):
        sent_messages.append((mail_from, mail_to, message))

    monkeypatch.setattr(BasicConfig, "MAIL_FROM", "notifier@localhost")
    monkeypatch.setattr(BasicConfig, "SMTP_SERVER", "smtp.local")
    monkeypatch.setattr(MailNotifier, "_send_mail", fake_send_mail)

    as_conf = SimpleNamespace(
        experiment_data={
            "MAIL": {
                "TO": ["user@example.com"],
            }
        }
    )

    # SYPD = 1 year * 24 / 12h = 2.0, below lower bound 4.5 -> violation.
    job = SimpleNamespace(
        name="a000_SIM",
        status=Status.COMPLETED,
        runtime=12.0,
        chunk_size=1,
        chunk_size_unit="year",
        cpmip_thresholds={
            "SYPD": {
                "THRESHOLD": 5.0,
                "COMPARISON": "greater_than",
                "%_ACCEPTED_ERROR": 10,
            }
        },
    )

    Autosubmit._notify_cpmip_threshold_violations(as_conf, "a000", job)

    assert len(sent_messages) == 1
    _, _, message = sent_messages[0]
    body = message.get_payload()
    assert "CPMIP Threshold Violation detected for a000_SIM" in message["Subject"]
    assert "Metric: SYPD" in body
    assert "Comparison: must be >= effective bound (greater_than)" in body
    assert "Configured threshold: 5.0" in body
    assert "Effective bound: 4.5" in body
    assert "Observed value: 2.0" in body


def test_notify_cpmip_threshold_violations_does_not_send_when_no_violations(monkeypatch):
    sent_messages = []

    def fake_send_mail(self, mail_from, mail_to, message):
        sent_messages.append((mail_from, mail_to, message))

    monkeypatch.setattr(BasicConfig, "MAIL_FROM", "notifier@localhost")
    monkeypatch.setattr(BasicConfig, "SMTP_SERVER", "smtp.local")
    monkeypatch.setattr(MailNotifier, "_send_mail", fake_send_mail)

    as_conf = SimpleNamespace(
        experiment_data={
            "MAIL": {
                "TO": ["user@example.com"],
            }
        }
    )

    # SYPD = 4.0, threshold=3.0 greater_than => not a violation.
    job = SimpleNamespace(
        name="a000_SIM",
        status=Status.COMPLETED,
        runtime=6.0,
        chunk_size=1,
        chunk_size_unit="year",
        cpmip_thresholds={
            "SYPD": {
                "THRESHOLD": 3.0,
                "COMPARISON": "greater_than",
                "%_ACCEPTED_ERROR": 0,
            }
        },
    )

    Autosubmit._notify_cpmip_threshold_violations(as_conf, "a000", job)

    assert sent_messages == []
