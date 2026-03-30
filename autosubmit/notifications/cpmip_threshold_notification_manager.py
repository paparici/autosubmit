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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.job.job_common import Status
from autosubmit.metrics.cpmip_metrics import CPMIPMetrics
from autosubmit.notifications.mail_notifier import MailNotifier
from autosubmit.notifications.notifier import Notifier


class CPMIPThresholdNotificationManager:
    """Handle CPMIP notification data persistence and post-recovery evaluation."""

    @staticmethod
    def build_context_before_log_update(job) -> dict:
        """Collect CPMIP-related fields before runtime attributes are cleaned.

        :param job: Job-like object containing CPMIP configuration and runtime attributes.
        :return: Dictionary with fields needed after ``update_log_status`` cleanup.
        """
        thresholds = getattr(job, "cpmip_thresholds", None)

        return {
            "cpmip_thresholds": dict(thresholds) if thresholds else {},
            "chunk_size": getattr(job, "chunk_size", None),
            "chunk_size_unit": getattr(job, "chunk_size_unit", None),
            "script_name": getattr(job, "script_name", None),
            "stat_file": getattr(job, "stat_file", None),
            "processors": getattr(job, "processors", None),
            "nodes": getattr(job, "nodes", None),
        }

    @staticmethod
    def notify_after_log_recovery(as_conf, job, context: dict) -> None:
        """Restore needed fields and notify on CPMIP threshold violations after log recovery.

        This method is designed to be called after ``update_log_status`` has run and
        potentially cleaned runtime attributes from the job.

        :param as_conf: Autosubmit configuration object.
        :param job: Job-like object with stat-file helper methods and runtime metadata.
        :param context: Snapshot produced by ``build_context_before_log_update``.
        """
        if as_conf.get_notifications() != "true":
            return

        cpmip_thresholds = (context or {}).get("cpmip_thresholds") or {}
        if not cpmip_thresholds:
            return

        stat_file = (context or {}).get("stat_file")
        script_name = (context or {}).get("script_name")

        if stat_file:
            job.stat_file = stat_file
        if script_name:
            job.script_name = script_name

        start_time = job.check_start_time(job.fail_count)
        end_time = job.check_end_time(job.fail_count)
        if start_time <= 0 or end_time <= 0:
            return

        job.start_time_timestamp = start_time
        job.finish_time_timestamp = end_time
        job.cpmip_thresholds = cpmip_thresholds
        job.chunk_size = (context or {}).get("chunk_size")
        job.chunk_size_unit = (context or {}).get("chunk_size_unit")
        processors = (context or {}).get("processors")
        nodes = (context or {}).get("nodes")
        if processors is not None:
            job.processors = processors
        if nodes is not None:
            job.nodes = nodes

        CPMIPThresholdNotificationManager.notify_threshold_violations(
            as_conf,
            job.expid,
            job,
        )

    @staticmethod
    def notify_threshold_violations(as_conf, expid, job) -> None:
        """Evaluate CPMIP metrics and send notification when thresholds are violated.

        :param as_conf: Autosubmit configuration object.
        :param expid: Experiment identifier.
        :param job: Job-like object containing status, thresholds, and computed metadata.
        """
        if job.status != Status.COMPLETED:
            return

        if not job.cpmip_thresholds:
            return

        violations = CPMIPMetrics.evaluate(job, job.cpmip_thresholds)
        if not violations:
            return

        Notifier.notify_cpmip_threshold_violations(
            MailNotifier(BasicConfig),
            expid,
            job.name,
            violations,
            as_conf.experiment_data["MAIL"]["TO"],
        )
