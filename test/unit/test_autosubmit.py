# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

"""Tests for ``AutosubmitGit``."""

from pathlib import Path
from textwrap import dedent

import pytest
from types import SimpleNamespace

from autosubmit.autosubmit import Autosubmit
from autosubmit.job.job_common import Status
from autosubmit.config.basicconfig import BasicConfig
from autosubmit.log.log import AutosubmitCritical
from test.unit.conftest import AutosubmitConfigFactory


def test_copy_as_config(autosubmit_config: AutosubmitConfigFactory):
    """function to test copy_as_config from autosubmit.py

    :param autosubmit_config:
    :type autosubmit_config: AutosubmitConfigFactory
    """
    autosubmit_config('a000', {})
    BasicConfig.LOCAL_ROOT_DIR = f"{BasicConfig.LOCAL_ROOT_DIR}"

    ini_file = Path(f'{BasicConfig.LOCAL_ROOT_DIR}/a000/conf')
    new_file = Path(f'{BasicConfig.LOCAL_ROOT_DIR}/a001/conf')
    ini_file.mkdir(parents=True, exist_ok=True)
    new_file.mkdir(parents=True, exist_ok=True)
    ini_file = ini_file / 'jobs_a000.conf'
    new_file = new_file / 'jobs_a001.yml'

    with open(ini_file, 'w+', encoding="utf-8") as file:
        file.write(dedent('''\
                [LOCAL_SETUP]
                FILE = LOCAL_SETUP.sh
                PLATFORM = LOCAL
                '''))
        file.flush()

    Autosubmit.copy_as_config('a001', 'a000')

    new_yaml_file = Path(new_file.parent, new_file.stem).with_suffix('.yml')

    assert new_yaml_file.exists()
    assert new_yaml_file.stat().st_size > 0

    new_yaml_file = Path(new_file.parent, new_file.stem).with_suffix('.conf_AS_v3_backup')

    assert new_yaml_file.exists()
    assert new_yaml_file.stat().st_size > 0


def test_pkl_fix_postgres(monkeypatch, autosubmit):
    """Test that trying to fix the pkl when using Postgres results in an error."""
    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'postgres')

    with pytest.raises(AutosubmitCritical):
        autosubmit.pkl_fix('a000')


def test_database_backup_postgres(monkeypatch, autosubmit, mocker):
    """Test that trying to back up a Postgres DB results in just a log message of WIP."""
    monkeypatch.setattr(BasicConfig, 'DATABASE_BACKEND', 'postgres')
    mocked_log = mocker.patch('autosubmit.autosubmit.Log')
    autosubmit.database_backup('a000')
    assert mocked_log.debug.called

def _build_as_conf(autosubmit_config, notifications="true"):
    as_conf = autosubmit_config(
        "a000",
        {
            "MAIL": {
                "NOTIFICATIONS": notifications,
                "TO": ["user@example.com"]
            },
        },
    )
    return as_conf


def _build_job(
    name="a000_SIM",
    status=Status.COMPLETED,
    notify_on=None,
    cpmip_thresholds=None,
):
    return SimpleNamespace(
        name=name,
        status=status,
        notify_on=[] if notify_on is None else notify_on,
        cpmip_thresholds={} if cpmip_thresholds is None else cpmip_thresholds,
    )


def test_job_notify_calls_private_cpmip_helper_after_status_checks(autosubmit_config, mocker):
    as_conf = _build_as_conf(autosubmit_config, notifications="true")

    job = _build_job(
        status=Status.COMPLETED,
        notify_on=[],
        cpmip_thresholds={"SYPD": {"THRESHOLD": 5.0, "COMPARISON": "greater_than", "%_ACCEPTED_ERROR": 10}},
    )
    tracker = {}
    prev_status = Status.RUNNING

    status_mail_mock = mocker.patch("autosubmit.autosubmit.Notifier.notify_status_change")
    cpmip_helper_mock = mocker.patch("autosubmit.autosubmit.Autosubmit._notify_cpmip_threshold_violations")

    out = Autosubmit.job_notify(as_conf, "a000", job, prev_status, tracker)

    assert out[job.name] == (prev_status, job.status)
    status_mail_mock.assert_not_called()
    cpmip_helper_mock.assert_called_once_with(as_conf, "a000", job)


def test_job_notify_does_not_call_private_helper_when_notifications_disabled(autosubmit_config, mocker):
    as_conf = _build_as_conf(autosubmit_config, notifications="false")
    job = _build_job(
        status=Status.COMPLETED,
        cpmip_thresholds={"SYPD": {"THRESHOLD": 5.0, "COMPARISON": "greater_than", "%_ACCEPTED_ERROR": 10}},
    )

    cpmip_helper_mock = mocker.patch("autosubmit.autosubmit.Autosubmit._notify_cpmip_threshold_violations")

    Autosubmit.job_notify(as_conf, "a000", job, Status.RUNNING, {})

    cpmip_helper_mock.assert_not_called()


def test_private_helper_skips_when_job_not_completed(autosubmit_config, mocker, monkeypatch):
    as_conf = _build_as_conf(autosubmit_config, notifications="true")
    job = _build_job(
        status=Status.RUNNING,
        cpmip_thresholds={"SYPD": {"THRESHOLD": 5.0, "COMPARISON": "greater_than", "%_ACCEPTED_ERROR": 10}},
    )

    class DummyCPMIPMetrics:
        @staticmethod
        def evaluate(job_obj, thresholds):
            return {"SYPD": {"real_value": 4.0}}

    import autosubmit.autosubmit as autosubmit_module
    monkeypatch.setattr(autosubmit_module, "CPMIPMetrics", DummyCPMIPMetrics, raising=False)

    eval_mock = mocker.patch("autosubmit.autosubmit.CPMIPMetrics.evaluate", create=True)
    notify_mock = mocker.patch("autosubmit.autosubmit.Notifier.notify_cpmip_threshold_violations", create=True)

    Autosubmit._notify_cpmip_threshold_violations(as_conf, "a000", job)

    eval_mock.assert_not_called()
    notify_mock.assert_not_called()


def test_private_helper_skips_when_thresholds_empty(autosubmit_config, mocker, monkeypatch):
    as_conf = _build_as_conf(autosubmit_config, notifications="true")
    job = _build_job(status=Status.COMPLETED, cpmip_thresholds={})

    class DummyCPMIPMetrics:
        @staticmethod
        def evaluate(job_obj, thresholds):
            return {"SYPD": {"real_value": 4.0}}

    import autosubmit.autosubmit as autosubmit_module
    monkeypatch.setattr(autosubmit_module, "CPMIPMetrics", DummyCPMIPMetrics, raising=False)

    eval_mock = mocker.patch("autosubmit.autosubmit.CPMIPMetrics.evaluate", create=True)
    notify_mock = mocker.patch("autosubmit.autosubmit.Notifier.notify_cpmip_threshold_violations", create=True)

    Autosubmit._notify_cpmip_threshold_violations(as_conf, "a000", job)

    eval_mock.assert_not_called()
    notify_mock.assert_not_called()


def test_private_helper_calls_metrics_and_notifier_when_violations_found(autosubmit_config, mocker, monkeypatch):
    as_conf = _build_as_conf(autosubmit_config, notifications="true")
    job = _build_job(
        status=Status.COMPLETED,
        cpmip_thresholds={"SYPD": {"THRESHOLD": 5.0, "COMPARISON": "greater_than", "%_ACCEPTED_ERROR": 10}},
    )

    violations = {
        "SYPD": {
            "threshold": 5.0,
            "accepted_error": 10,
            "real_value": 3.9,
        }
    }

    class DummyCPMIPMetrics:
        @staticmethod
        def evaluate(job_obj, thresholds):
            return violations

    import autosubmit.autosubmit as autosubmit_module
    monkeypatch.setattr(autosubmit_module, "CPMIPMetrics", DummyCPMIPMetrics, raising=False)

    eval_mock = mocker.patch("autosubmit.autosubmit.CPMIPMetrics.evaluate", return_value=violations, create=True)
    notify_mock = mocker.patch("autosubmit.autosubmit.Notifier.notify_cpmip_threshold_violations", create=True)

    Autosubmit._notify_cpmip_threshold_violations(as_conf, "a000", job)

    eval_mock.assert_called_once_with(job, job.cpmip_thresholds)
    notify_mock.assert_called_once_with(
        mocker.ANY,          
        "a000",              
        job.name,            
        violations,          
        as_conf.experiment_data["MAIL"]["TO"],
    )


def test_private_helper_skips_notifier_when_no_violations(autosubmit_config, mocker, monkeypatch):
    as_conf = _build_as_conf(autosubmit_config, notifications="true")
    job = _build_job(
        status=Status.COMPLETED,
        cpmip_thresholds={"SYPD": {"THRESHOLD": 5.0, "COMPARISON": "greater_than", "%_ACCEPTED_ERROR": 10}},
    )

    class DummyCPMIPMetrics:
        @staticmethod
        def evaluate(job_obj, thresholds):
            return {}

    import autosubmit.autosubmit as autosubmit_module
    monkeypatch.setattr(autosubmit_module, "CPMIPMetrics", DummyCPMIPMetrics, raising=False)

    eval_mock = mocker.patch("autosubmit.autosubmit.CPMIPMetrics.evaluate", return_value={}, create=True)
    notify_mock = mocker.patch("autosubmit.autosubmit.Notifier.notify_cpmip_threshold_violations", create=True)

    Autosubmit._notify_cpmip_threshold_violations(as_conf, "a000", job)

    eval_mock.assert_called_once_with(job, job.cpmip_thresholds)
    notify_mock.assert_not_called()