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
from unittest.mock import Mock

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.job.job_common import Status
from autosubmit.notifications.cpmip_threshold_notification_manager import (
    CPMIPThresholdNotificationManager,
)
from autosubmit.notifications.mail_notifier import MailNotifier


def _as_conf(notifications="true", mail_to=None):
    return SimpleNamespace(
        get_notifications=lambda: notifications,
        experiment_data={"MAIL": {"TO": mail_to or ["user@example.com"]}},
    )


def _job(**kwargs):
    defaults = {
        "status": Status.COMPLETED,
        "cpmip_thresholds": {"SYPD": {"THRESHOLD": 5.0}},
        "chunk_size": 2,
        "chunk_size_unit": "month",
        "script_name": "a000_SIM.cmd",
        "stat_file": "/tmp/a000_SIM.stat",
        "processors": 16,
        "nodes": 2,
        "fail_count": 0,
        "expid": "a000",
        "name": "a000_SIM",
        "check_start_time": Mock(return_value=100),
        "check_end_time": Mock(return_value=200),
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def test_build_context_before_log_update_copies_expected_fields():
    thresholds = {"SYPD": {"THRESHOLD": 5.0}}
    job = _job(cpmip_thresholds=thresholds)

    context = CPMIPThresholdNotificationManager.build_context_before_log_update(job)

    assert context["cpmip_thresholds"] == thresholds
    assert context["cpmip_thresholds"] is not thresholds
    assert context["chunk_size"] == 2
    assert context["chunk_size_unit"] == "month"
    assert context["script_name"] == "a000_SIM.cmd"
    assert context["stat_file"] == "/tmp/a000_SIM.stat"
    assert context["processors"] == 16
    assert context["nodes"] == 2


def test_build_context_before_log_update_handles_missing_thresholds():
    job = _job(cpmip_thresholds=None)

    context = CPMIPThresholdNotificationManager.build_context_before_log_update(job)

    assert context["cpmip_thresholds"] == {}


def test_notify_after_log_recovery_returns_when_notifications_disabled(mocker):
    as_conf = _as_conf(notifications="false")
    job = _job()
    notify_mock = mocker.patch(
        "autosubmit.notifications.cpmip_threshold_notification_manager.CPMIPThresholdNotificationManager.notify_threshold_violations",
    )

    CPMIPThresholdNotificationManager.notify_after_log_recovery(
        as_conf,
        job,
        {"cpmip_thresholds": {"SYPD": {"THRESHOLD": 5.0}}},
    )

    job.check_start_time.assert_not_called()
    job.check_end_time.assert_not_called()
    notify_mock.assert_not_called()


def test_notify_after_log_recovery_returns_when_context_thresholds_empty(mocker):
    as_conf = _as_conf(notifications="true")
    job = _job()
    notify_mock = mocker.patch(
        "autosubmit.notifications.cpmip_threshold_notification_manager.CPMIPThresholdNotificationManager.notify_threshold_violations",
    )

    CPMIPThresholdNotificationManager.notify_after_log_recovery(as_conf, job, {"cpmip_thresholds": {}})

    job.check_start_time.assert_not_called()
    job.check_end_time.assert_not_called()
    notify_mock.assert_not_called()


def test_notify_after_log_recovery_returns_when_timestamps_invalid(mocker):
    as_conf = _as_conf(notifications="true")
    job = _job(check_start_time=Mock(return_value=0), check_end_time=Mock(return_value=200))
    notify_mock = mocker.patch(
        "autosubmit.notifications.cpmip_threshold_notification_manager.CPMIPThresholdNotificationManager.notify_threshold_violations",
    )

    CPMIPThresholdNotificationManager.notify_after_log_recovery(
        as_conf,
        job,
        {"cpmip_thresholds": {"SYPD": {"THRESHOLD": 5.0}}},
    )

    job.check_start_time.assert_called_once_with(job.fail_count)
    job.check_end_time.assert_called_once_with(job.fail_count)
    notify_mock.assert_not_called()


def test_notify_after_log_recovery_restores_fields_and_delegates(mocker):
    as_conf = _as_conf(notifications="true")
    job = _job(
        script_name=None,
        stat_file=None,
        check_start_time=Mock(return_value=111),
        check_end_time=Mock(return_value=222),
    )
    notify_mock = mocker.patch(
        "autosubmit.notifications.cpmip_threshold_notification_manager.CPMIPThresholdNotificationManager.notify_threshold_violations",
    )
    context = {
        "cpmip_thresholds": {"SYPD": {"THRESHOLD": 5.0}},
        "chunk_size": 6,
        "chunk_size_unit": "month",
        "script_name": "a000_SIM_restored.cmd",
        "stat_file": "/tmp/a000_SIM_restored.stat",
        "processors": 24,
        "nodes": 3,
    }

    CPMIPThresholdNotificationManager.notify_after_log_recovery(as_conf, job, context)

    assert job.stat_file == "/tmp/a000_SIM_restored.stat"
    assert job.script_name == "a000_SIM_restored.cmd"
    assert job.start_time_timestamp == 111
    assert job.finish_time_timestamp == 222
    assert job.cpmip_thresholds == context["cpmip_thresholds"]
    assert job.chunk_size == 6
    assert job.chunk_size_unit == "month"
    assert job.processors == 24
    assert job.nodes == 3
    notify_mock.assert_called_once_with(as_conf, job.expid, job)


def test_notify_threshold_violations_returns_when_job_not_completed(mocker):
    as_conf = _as_conf()
    job = _job(status=Status.RUNNING)
    evaluate_mock = mocker.patch(
        "autosubmit.notifications.cpmip_threshold_notification_manager.CPMIPMetrics.evaluate"
    )
    notifier_mock = mocker.patch(
        "autosubmit.notifications.cpmip_threshold_notification_manager.Notifier.notify_cpmip_threshold_violations"
    )

    CPMIPThresholdNotificationManager.notify_threshold_violations(as_conf, "a000", job)

    evaluate_mock.assert_not_called()
    notifier_mock.assert_not_called()


def test_notify_threshold_violations_returns_when_thresholds_missing(mocker):
    as_conf = _as_conf()
    job = _job(cpmip_thresholds={})
    evaluate_mock = mocker.patch(
        "autosubmit.notifications.cpmip_threshold_notification_manager.CPMIPMetrics.evaluate"
    )
    notifier_mock = mocker.patch(
        "autosubmit.notifications.cpmip_threshold_notification_manager.Notifier.notify_cpmip_threshold_violations"
    )

    CPMIPThresholdNotificationManager.notify_threshold_violations(as_conf, "a000", job)

    evaluate_mock.assert_not_called()
    notifier_mock.assert_not_called()


def test_notify_threshold_violations_skips_dispatch_when_no_violations(mocker):
    as_conf = _as_conf()
    job = _job()
    mocker.patch(
        "autosubmit.notifications.cpmip_threshold_notification_manager.CPMIPMetrics.evaluate",
        return_value={},
    )
    notifier_mock = mocker.patch(
        "autosubmit.notifications.cpmip_threshold_notification_manager.Notifier.notify_cpmip_threshold_violations"
    )

    CPMIPThresholdNotificationManager.notify_threshold_violations(as_conf, "a000", job)

    notifier_mock.assert_not_called()


def test_notify_threshold_violations_dispatches_when_violations_found(mocker):
    as_conf = _as_conf(mail_to=["alerts@example.com"])
    job = _job()
    violations = {
        "SYPD": {
            "threshold": 5.0,
            "accepted_error": 10,
            "comparison": "greater_than",
            "bound": 4.5,
            "real_value": 3.9,
        }
    }
    mocker.patch(
        "autosubmit.notifications.cpmip_threshold_notification_manager.CPMIPMetrics.evaluate",
        return_value=violations,
    )
    notifier_mock = mocker.patch(
        "autosubmit.notifications.cpmip_threshold_notification_manager.Notifier.notify_cpmip_threshold_violations"
    )

    CPMIPThresholdNotificationManager.notify_threshold_violations(as_conf, "a000", job)

    notifier_mock.assert_called_once()
    implementation, expid, job_name, sent_violations, notify_to = notifier_mock.call_args.args
    assert isinstance(implementation, MailNotifier)
    assert implementation.config is BasicConfig
    assert expid == "a000"
    assert job_name == job.name
    assert sent_violations == violations
    assert notify_to == ["alerts@example.com"]
